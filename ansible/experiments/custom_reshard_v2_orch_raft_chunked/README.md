# aqraft reshard — 2-round chunked orchestrated migration

Aqueduct fork + RedisRaft + RDMA `MIGRATE-ALL` driven concurrently with a YCSB
workload, in **two chunked rounds** so the post-migration throughput curve shows
two distinct migration events (the "scaling" curve). This is the canonical
"aqraft reshard" experiment.

- Variant dir: `experiments/custom_reshard_v2_orch_raft_chunked/`
- Single-round sibling: `experiments/custom_reshard_v2_orch_raft/` (one MIGRATE-ALL)
- Reshard playbook: `tasks/cluster/reshard_cluster_rdma_v2_orchestrated_chunked.yml`

## Topology (set by the git branch's `inventory.ini` + `group_vars/all.yml`)

7 nodes. Three **donor** shardgroups, each a 3-node Raft group with its leader on
a distinct host/port; one **recipient** shardgroup (sg4):

| Shardgroup | Leader   | Slots         |
|------------|----------|---------------|
| sg1        | redis0:8000 | 0–5460     |
| sg2        | redis1:8001 | 5461–10921 |
| sg3        | redis2:8002 | 10922–16383 |
| sg4 (recipient) | redis3:8000 (followers redis4, ycsb1) | owns 0–16383 |

YCSB runs from `ycsb0`. Each round migrates the next ~682-slot chunk of **each**
donor's range to sg4 (`reshard_slots=4095` → 1365/donor → 682 + 683 over 2 rounds).

## Prerequisites (one-time per branch / node-set)

1. **Same git branch on ALL 7 nodes.** The branch controls `inventory.ini`.
   ```bash
   for h in redis0 redis1 redis2 redis3 redis4 ycsb0 ycsb1; do
     sudo ssh $h "git -C /users/entall/rd fetch origin <branch> -q && git -C /users/entall/rd checkout <branch>"
   done
   ```
   Re-read `inventory.ini` after the first checkout — the node list can change per branch.

2. **Outbound SSH/ansible only works as root.** Use `sudo ssh <host>` and
   `sudo ansible[-playbook] ...`. Plain `ssh`/`ansible` as the login user fails
   (`Permission denied (publickey)`). The repo `.git` and `ansible/` dirs are
   root-owned, so editing/plotting there also needs sudo.

3. **Stage the prod workloads.** YCSB reads `/users/entall/rd/workloads/<wl>` on
   ycsb0, deployed by `build_ycsb` from the controller's `/rd/workloads/*`.
   `workloada_prod` (500K records, 30M ops, `maxexecutiontime=300`, 50/50) ships
   in the repo; create b/c the same way:
   ```bash
   for w in workloadb workloadc; do
     cp ycsb_client/workloads/$w /tmp/${w}_prod
     sed -i -e 's/^operationcount=.*/operationcount=30000000/' /tmp/${w}_prod
     grep -q '^maxexecutiontime=' /tmp/${w}_prod && sed -i 's/^maxexecutiontime=.*/maxexecutiontime=300/' /tmp/${w}_prod || echo 'maxexecutiontime=300' >> /tmp/${w}_prod
     sudo cp /tmp/${w}_prod /rd/workloads/${w}_prod
   done
   # deploy to ycsb0 (skips a full build_ycsb)
   sudo rsync -a /rd/workloads/workload{a,b,c}_prod root@ycsb0:/users/entall/rd/workloads/
   ```

4. **Build the binaries** (redis_custom + redisraft + ycsb). The orch_raft
   `setup.yml` does this and a clean teardown:
   ```bash
   sudo ansible-playbook -i inventory.ini experiments/custom_reshard_v2_orch_raft/setup.yml -e redis_variant=custom
   ```

5. **matplotlib** for plotting: `sudo python3 -m pip install matplotlib`.

## Run

The chunked variant has no `setup.yml`/`teardown.yml` — it assumes binaries are
built and drives `workload.yml` directly. Run one workload:

```bash
cd /users/entall/rd/ansible
sudo ansible-playbook -i inventory.ini \
  experiments/custom_reshard_v2_orch_raft_chunked/workload.yml \
  -e redis_variant=custom \
  -e pre_reshard_pause=10 \
  -e rdma_migration_peer_stagger_ms=0 \
  -e experiment_name=custom_reshard_v2_orch_raft_chunked_workloada_prod \
  -e redis_workload=workloada_prod
```

`run_prod.sh` is a thin wrapper that runs exactly this for `workloada_prod`.

### Key `-e` parameters

| Var | Meaning | Notes |
|-----|---------|-------|
| `redis_workload` | which `/users/entall/rd/workloads/<file>` YCSB drives | `workloada_prod` (50/50), `workloadb_prod` (95/5), `workloadc_prod` (100% read) |
| `experiment_name` | results land in `/tmp/experiments/<name>/` | use a distinct name per run to keep comparisons |
| `pre_reshard_pause` | seconds of steady-state YCSB before the migration starts | default 10 |
| `rdma_migration_peer_stagger_ms` | per-peer donor start delay = `(idx+1)*this`; `0` = completion-driven (each donor starts when the prior hits terminal — tight, no idle gap). Default **30000** (huge idle gaps). | use `0` for tight rounds |

### Sweep across workloads

```bash
for w in workloada_prod workloadb_prod workloadc_prod; do
  sudo ansible-playbook -i inventory.ini \
    experiments/custom_reshard_v2_orch_raft_chunked/workload.yml \
    -e redis_variant=custom -e pre_reshard_pause=10 -e rdma_migration_peer_stagger_ms=0 \
    -e experiment_name=custom_reshard_v2_orch_raft_chunked_${w} -e redis_workload=${w}
done
```

## Prepare-ahead (move PREP/REGISTERING out of the timed window)

The peers' RDMA connection + source-buffer `ibv_reg_mr` (~0.8s/donor) are pre-paid
during the pre-reshard pause via `RDMA MIGRATE-WARM` (peers only — warming the
orchestrator's own sg1 breaks its WRITE_FLIP; sg1's REGISTERING is pre-window
anyway). The warm registration runs **off the main thread** (else the big
`ibv_reg_mr` stalls Raft heartbeats > election_timeout×2 → leader change → the
next `WRITE_FLIP` returns a spurious `MOVED`). The migration worker then logs
`REGISTERING skipped (pre-warmed)` and skips that phase. This is wired into the
chunked reshard playbook; no extra flags needed.

## Editing redis source — IMPORTANT

`build_redis_custom` compiles each node's **own local** `/users/entall/rd/redis`
tree; it does **not** sync source from the controller. After editing `redis/src`
on redis0, propagate before building or the peers silently run the old binary:

```bash
for n in redis1 redis2 redis3 redis4 ycsb1; do
  sudo rsync -a redis/src/cluster_rdma.c redis/src/cluster.h root@$n:/users/entall/rd/redis/src/
done
sudo ansible-playbook -i inventory.ini tasks/build/build_redis_custom.yml -e redis_variant=custom
# verify: strings /users/entall/rd/redis_bin/bin/redis-server | grep '<your-new-log-string>'  (per node)
```

## Results & plotting

Results: `/tmp/experiments/<experiment_name>/` — `ycsb/ycsb0/tmp/ycsb_output_ycsb0`
(per-second throughput/latency) and `logs/<host>/tmp/redis_logs/*.log` (RDMA +
Raft events).

```bash
cd /users/entall/rd
D=/tmp/experiments/<experiment_name>
python3 plot_ycsb_timeseries.py "$D" --output out/ycsb_timeseries.png   # throughput+latency vs time, migration band
python3 plot_full_run.py        "$D" out/full_run.png                   # +CPU +cluster-NIC traffic
python3 plot_raft_migration_timeline.py "$D" out/raft_migration_timeline.png  # per-shardgroup phases + Raft commits (see note)
```
(`plot_raft_migration_timeline.py` lives in `/tmp/` this session — move it into the
repo if you want it permanent. Plot output dirs under `ansible/` need
`sudo mkdir` + `chown` to your user since `ansible/` is root-owned.)

## Verifying a run

- `failed=0` in the PLAY RECAP; **no `MOVED`** in the log (a MOVED at WRITE_FLIP
  means a donor's Raft leadership moved — usually a main-thread stall).
- All three donor shardgroups migrated their **own** ranges (not sg1 three times):
  ```bash
  grep -rhE 'MGN_TXN_START applied' "$D"/logs | grep -oE 'slots=[0-9]+-[0-9]+' | sort | uniq -c
  # expect ranges around 0-681, 5461-6142, 10922-11603 (+ round 2)
  ```
- Migration protocol on the Raft log: `MGN_TXN_START/DONE` (donors),
  `MGN_RECP_TXN_START/INDX_UPD/RECP_TXN_DONE` (recipient sg4).
- Prepare-ahead working: `grep -rh 'REGISTERING skipped (pre-warmed)' "$D"/logs`
  → 4 per run (2 peers × 2 rounds).

## Known gotchas

- **YCSB clock glitch**: if `ycsb_output_ycsb0`'s wall-clock span ≫ its `sec`
  counter (e.g. 80 min vs 260 s), the host clock hiccuped and "current ops/sec"
  (wall-clock-derived) reads bogusly low — re-run; the redis-log timeline is
  unaffected. Sanity check: first vs last `... N sec` timestamps.
- **Recipient finalization is intermittent**: some runs finalize only the first
  recipient session (`MGN_INDX_UPD`/`MGN_RECP_TXN_DONE` ×1 instead of ×6).
- **Inter-round gap** (~3s) is ansible orchestration (R1 NARROW + play transition
  + R2 WRITE_FLIP + dispatch), not migration work.
