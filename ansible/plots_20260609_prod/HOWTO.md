# How to run the reshard experiment and generate the plots

Step-by-step for the AqRaft RDMA reshard migration-window experiment (async-apply,
2 rounds, 3 donors → sg4). Run everything as **root** (`sudo`); the controller is
**redis0**, and all paths are under `/users/entall/rd`.

---

## 0. Prerequisites

- You are on **redis0** (the controller), in `/users/entall/rd`.
- Git branch is `aqueduct_broken` (this controls which `inventory.ini` is active).
  ```bash
  cd /users/entall/rd && git branch --show-current   # → aqueduct_broken
  ```
- Python has matplotlib + PIL (already installed on redis0) for the plot script.

---

## 1. Build & deploy — ONLY if you changed `redis/src`

`build_redis_custom` compiles **each node's own local copy** of the source, so you
must push the source to every peer first, then build everywhere.

```bash
cd /users/entall/rd

# 1a. push changed source to every peer (redis0 is the controller, already has it)
for h in redis1 redis2 redis3 redis4 ycsb1; do
  sudo rsync -e ssh -a redis/src/ "$h":/users/entall/rd/redis/src/
done

# 1b. build on all 6 nodes
cd ansible
sudo ansible-playbook -i inventory.ini tasks/build/build_redis_custom.yml

# 1c. (optional) verify a flag string is in every binary
cd /users/entall/rd
for h in redis0 redis1 redis2 redis3 redis4 ycsb1; do
  echo -n "$h: "; sudo ssh "$h" "strings /users/entall/rd/redis/src/redis-server | grep -c rdma-async-apply"
done   # expect 1 on every node
```

If you did **not** touch `redis/src`, skip this whole step — the deployed binaries
are reused.

---

## 2. Run the experiment

The playbook starts the cluster, loads YCSB, kicks off the resharding under live
traffic, collects logs, and computes the window. One command per workload:

```bash
cd /users/entall/rd/ansible

# workloada (50/50 read/write)
sudo ansible-playbook -i inventory.ini \
  experiments/custom_reshard_v2_orch_raft_chunked/workload_nround.yml \
  -e redis_variant=custom -e pre_reshard_pause=20 -e n_rounds=2 \
  -e rdma_backpatch_pool_size=4 -e rdma_migration_peer_stagger_ms=0 \
  -e ycsb_slotpoll_ms=100 -e rdma_chain_pipeline=yes -e rdma_chain_xsession=yes \
  -e rdma_async_apply=yes \
  -e redis_workload=workloada_prod \
  -e experiment_name=custom_reshard_prod_drip_workloada

# workloadb (95/5 read/write): swap the last two -e lines
sudo ansible-playbook -i inventory.ini \
  experiments/custom_reshard_v2_orch_raft_chunked/workload_nround.yml \
  -e redis_variant=custom -e pre_reshard_pause=20 -e n_rounds=2 \
  -e rdma_backpatch_pool_size=4 -e rdma_migration_peer_stagger_ms=0 \
  -e ycsb_slotpoll_ms=100 -e rdma_chain_pipeline=yes -e rdma_chain_xsession=yes \
  -e rdma_async_apply=yes \
  -e redis_workload=workloadb_prod \
  -e experiment_name=custom_reshard_prod_drip_workloadb
```

Each run takes ~5–6 min. A clean run ends with `failed=0` on every host.

### Parameter reference

| `-e` flag | Meaning |
|---|---|
| `redis_variant=custom` | use the AqRaft fork (not vanilla) |
| `n_rounds=2` | reshard in 2 rounds (3 donors/round = 6 sessions) |
| `pre_reshard_pause=20` | seconds of steady-state load before resharding |
| `rdma_backpatch_pool_size=4` | recipient backpatch worker pool size |
| `rdma_chain_pipeline=yes` | pipeline (drip) the chain forward with the merge |
| `rdma_chain_xsession=yes` | cross-session dispatch + 3-pool landing ring |
| `rdma_async_apply=yes` | commit the migration before the merge runs (apply in bg) |
| `rdma_migration_peer_stagger_ms=0` | completion-driven donor dispatch |
| `ycsb_slotpoll_ms=100` | YCSB client slot-map poll interval |
| `redis_workload=<name>` | which YCSB workload (`workloada_prod` / `workloadb_prod`) |
| `experiment_name=<name>` | output dir name (see step 3) |

---

## 3. Where the output lands

Results are collected to `/tmp/experiments/<experiment_name>/`:

```
/tmp/experiments/<experiment_name>/
├── migration_window.txt                          # the headline window numbers
├── logs/redis3/tmp/redis_logs/redis3_sg4.log     # recipient (sg4 leader) log
├── logs/redis0/tmp/redis_logs/redis0_sg1.log     # donor logs (redis0/1/2)
└── ycsb/ycsb0/tmp/ycsb_output_ycsb0              # YCSB run output (latencies, errors)
```

---

## 4. Generate the Gantt plot

```bash
cd /users/entall/rd
python3 plot_phase_gantt.py \
  /tmp/experiments/custom_reshard_prod_drip_workloada \
  ansible/plots_20260609_prod/phase_gantt_prod_drip_workloada.png
# → wrote ...png  (1960x462, ratio=4.242)
```

`plot_phase_gantt.py <experiment_dir> <output.png>`:
- reads the donor logs (PREP/REGISTER/FLIPPING/TRANSFER phases) and the recipient
  log (MERGE/CHAIN/COMMIT sub-phases),
- auto-detects pipelined runs and draws the CHAIN bar across its overlapping span,
- labels each bar `sg<donor>.<round>` with its duration; navy = round 1, light blue
  = round 2.

The output dir must exist and be writable by you (it's `chown`ed to `entall`); to
make a new one:
```bash
sudo mkdir -p ansible/plots_20260609_prod && sudo chown entall:streamstore-PG0 ansible/plots_20260609_prod
```

---

## 5. Validate a run (sanity checks)

```bash
D=/tmp/experiments/custom_reshard_prod_drip_workloada
L=$D/logs/redis3/tmp/redis_logs/redis3_sg4.log

# headline window
cat $D/migration_window.txt

# all 4095 slots migrated (682 + 683 per donor × 3)
grep -hoE "DONE n_slots=[0-9]+" $D/logs/redis{0,1,2}/tmp/redis_logs/redis*_sg*.log | sort | uniq -c

# 6 commits (3 donors × 2 rounds), no crash
grep -c "RECP_TXN_DONE logged" $L
grep -iE "signal|sigsegv|REDIS BUG" $L     # must be empty

# YCSB reads all OK (data correctness under load)
grep -E "Return=" $D/ycsb/ycsb0/tmp/ycsb_output_ycsb0 | grep -vE "Return=OK"   # must be empty
```

A good run: window ~4.5 s, `6 DONE n_slots` lines summing to 4095 slots,
`RECP_TXN_DONE = 6`, no crash lines, no non-OK reads.
