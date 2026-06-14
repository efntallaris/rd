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

# 1c. (optional) verify the binary carries the expected markers on every node
cd /users/entall/rd
for h in redis0 redis1 redis2 redis3 redis4 ycsb1; do
  echo -n "$h: "; sudo ssh "$h" "strings /users/entall/rd/redis/src/redis-server | grep -cE 'rdma-async-apply|PERCHUNK CHAIN'"
done   # expect 2 on every node (async-apply flag + per-chunk instrumentation)
```

If you did **not** touch `redis/src`, skip this whole step — the deployed binaries
are reused.

> **Note:** the per-chunk markers (`PERCHUNK BACKPATCH` / `PERCHUNK CHAIN`, used by
> §4.1) live in `cluster_rdma.c` / `cluster_rdma_chain.c`. If they may be uncommitted
> in your working tree, `git status` before a node restart — the source on disk
> survives a reboot, but commit it so a fresh checkout/rebuild keeps the markers.

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
  -e rdma_async_apply=yes -e rdma_transfer_chunk_slots=171 \
  -e redis_workload=workloada_prod \
  -e experiment_name=perchunk_4chunk_workloada

# workloadb (95/5 read/write): swap the last two -e lines
sudo ansible-playbook -i inventory.ini \
  experiments/custom_reshard_v2_orch_raft_chunked/workload_nround.yml \
  -e redis_variant=custom -e pre_reshard_pause=20 -e n_rounds=2 \
  -e rdma_backpatch_pool_size=4 -e rdma_migration_peer_stagger_ms=0 \
  -e ycsb_slotpoll_ms=100 -e rdma_chain_pipeline=yes -e rdma_chain_xsession=yes \
  -e rdma_async_apply=yes -e rdma_transfer_chunk_slots=171 \
  -e redis_workload=workloadb_prod \
  -e experiment_name=perchunk_4chunk_workloadb
```

Each run takes ~5–6 min. A clean run ends with `failed=0` on every host.

> **`rdma_transfer_chunk_slots=171`** splits each donor session (~683 slots) into
> **exactly 4 chunks**, so the donor fires a `DONE-SLOTS-CHUNK` RPC every 171 slots
> and the recipient backpatches + chain-forwards each chunk the instant it lands.
> This is what produces the per-chunk start markers in the Gantt (§4.1). Omit the
> flag (or set it back to the `32` default) for the standard production run — the
> migration window is the same either way; 171 just gives 4 clean chunk boundaries
> to visualize instead of ~21.

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
| `rdma_transfer_chunk_slots=171` | slots per `DONE-SLOTS-CHUNK` (171 → 4 chunks/session; default 32) |
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
mkdir -p ansible/plots_$(date +%Y%m%d)
python3 plot_phase_gantt.py \
  /tmp/experiments/perchunk_4chunk_workloada \
  ansible/plots_$(date +%Y%m%d)/phase_gantt_workloada.png
# → wrote ...png  (1960x462, ratio=4.242)
```

`plot_phase_gantt.py <experiment_dir> <output.png>`:
- reads the donor logs (PREP/REGISTER/FLIPPING/TRANSFER phases) and the recipient
  log (MERGE/CHAIN/COMMIT sub-phases),
- auto-detects pipelined runs and draws the CHAIN bar across its overlapping span,
- labels each bar `sg<donor>.<round>` with its duration; navy = round 1, light blue
  = round 2.

The output dir is plain (written by you, not via sudo) so no chown is needed; if you
reuse a root-owned `plots_*` dir, `sudo chown -R entall:streamstore-PG0 <dir>` first.

### 4.1 Per-chunk start markers (requires the 4-chunk run)

When the run used `-e rdma_transfer_chunk_slots=171` (§2), the recipient log carries
per-chunk timing markers and the Gantt draws them automatically on the
TRANSFER / BACKPATCH / CHAIN-REPLICATION lanes:

- **▼ green solid (triangle-capped) = transfer START** — when the donor begins
  streaming that chunk (chunk 0 = session transfer-begin / TRANSFER bar's left edge;
  chunk N = the instant chunk N−1 landed, since the donor streams continuously, so
  each later green ▼ sits on the previous chunk's grey landing tick).
- **grey dotted = chunk landed** — transfer of that chunk complete; this is *also*
  the instant backpatch + chain-forward start (the grey ticks line up vertically
  across all three lanes). The green▼→grey gap on the TRANSFER lane is the chunk's
  ~127 ms wire time (171 × 2 MiB ≈ 342 MiB at ~2.7 GB/s).

These come from three `serverLog` markers in the recipient (`redis3_sg4.log`):
`RDMA DONE-SLOTS-CHUNK ... seq=N` (landing), `PERCHUNK BACKPATCH ... seq=N` (backpatch
start), `PERCHUNK CHAIN ... slot0=S seq=N` (chain-forward start). The donor of a
`PERCHUNK CHAIN` line is read from `slot0` (`<5461`→sg1, `<10922`→sg2, else sg3);
`PERCHUNK BACKPATCH` lacks a donor and is matched to the nearest transfer landing.

To dump the raw per-chunk timestamps as a table:
```bash
grep -hE "DONE-SLOTS-CHUNK|PERCHUNK" \
  /tmp/experiments/perchunk_4chunk_workloada/logs/redis3/tmp/redis_logs/redis3_sg4.log
```

---

## 4.5 Generate the YCSB throughput + latency timeline

Shows client-observed throughput (ops/s) and latency over the run, with the
migration window(s) shaded — the figure that proves traffic stays up during reshard.

```bash
cd /users/entall/rd
python3 plot_ycsb_timeseries.py \
  /tmp/experiments/perchunk_4chunk_workloada \
  -o ansible/plots_$(date +%Y%m%d)/ycsb_timeline_workloada.png
```

`plot_ycsb_timeseries.py <expdir> [-o out.png]`:
- reads `<expdir>/ycsb/ycsb0/tmp/ycsb_output_ycsb0` (per-second YCSB status lines)
  and the recipient log for the migration span,
- top panel = throughput (near-black line), bottom panel = latency; the migration
  rounds are shaded as bands.
- default output is `<expdir>/ycsb_timeseries.png` if `-o` is omitted.

Useful flags:
| flag | effect |
|---|---|
| `--span-only` | shade ONE window (first round start → last round end), drop M1/M2/M3 labels |
| `--smooth 5` | rolling-median (5 samples) on the latency panel to tame the start spike |
| `--with-resources --hosts redis0 redis3` | also emit per-host CPU/network/disk panels from systat logs |
| `--xmin / --xmax` | clip the time axis (seconds) |

---

## 5. Validate a run (sanity checks)

```bash
D=/tmp/experiments/perchunk_4chunk_workloada
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

# follower data integrity — DBSIZE must match across the chain (baseline 125173)
for h in redis3 redis4 ycsb1; do
  echo -n "$h: "; sudo ssh "$h" "/users/entall/rd/redis/src/redis-cli -p 8000 DBSIZE"
done   # all three equal, == the pre-reshard baseline

# per-chunk markers present (4 chunks/session when chunk_slots=171)
grep -c "PERCHUNK CHAIN" $L     # expect 24 (4 chunks × 3 donors × 2 rounds)
```

A good run: window **~3.6–3.8 s** (pipelined + chunked async-apply), `6 DONE n_slots`
lines summing to 4095 slots, `RECP_TXN_DONE = 6`, matching follower DBSIZE, no crash
lines, no non-OK reads.
