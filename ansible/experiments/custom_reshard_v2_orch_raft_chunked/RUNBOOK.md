# AqRaft reshard experiments — runbook

How to reproduce the RDMA-reshard production experiments (4 backpatch workers,
delayed migration, scaling curve) and regenerate every figure. Self-contained so
it works after a fresh session.

---

## 0. Cluster topology

3 donor shardgroups migrate ~25% of the keyspace into a new recipient shardgroup
(sg4) over RDMA, in 2 chunked rounds. Each shardgroup is a 3-node raft group.

| Group (inventory) | Nodes | Role |
|---|---|---|
| `redis_masters` | **redis0** (local/control), redis1, redis2 | the 3 donor leaders |
| `redis_migrate` | **redis3** | recipient leader (sg4) — runs the backpatch worker pool |
| `redis_migrate_replicas` | redis4, ycsb1 | recipient chain replicas |
| `ycsb_nodes` | ycsb0 | YCSB load generator |

| SG | Port | Slots before | → migrates to sg4 |
|---|---|---|---|
| sg1 (redis0) | 8000 | 0:5460 | 0:1364 |
| sg2 (redis1) | 8001 | 5461:10921 | 5461:6825 |
| sg3 (redis2) | 8002 | 10922:16383 | 10922:12286 |
| sg4 (redis3) | 8000 | 0:16383 (empty) | owns 0:1364 ∪ 5461:6825 ∪ 10922:12286 (4095 slots) |

---

## 1. Prerequisites / access (IMPORTANT)

- **All outbound SSH / ansible must run as root** — the user keys are root-only:
  use `sudo ssh redisN` and `sudo ansible-playbook ...`. Plain `ssh`/`ansible`
  fails with `unreachable` / publickey errors.
- **You are on redis0**, the ansible control node (`ansible_connection=local`).
- Repo: `/users/entall/rd`. Ansible root: `/users/entall/rd/ansible`
  (inventory: `ansible/inventory.ini`). The active branch selects the inventory —
  these experiments run on branch **`aqueduct_broken`**.
- Source files under `redis/src` are often **root-owned**; to edit one, first
  `sudo chown entall:streamstore-PG0 <file>` (the build runs as root regardless).

---

## 2. Build & deploy the binary (only when you changed `redis/src`)

`build_redis_custom` compiles **each node's own local copy** of the source, so you
MUST rsync the source to the peers first, then build on all nodes.

```bash
cd /users/entall/rd

# 1. push changed source to every peer (root-only outbound)
for h in redis1 redis2 redis3 redis4 ycsb1; do
  sudo rsync -e ssh -a redis/src/ "$h":/users/entall/rd/redis/src/
done

# 2. build on all 6 nodes (donors + recipient + chain replicas)
cd ansible
sudo ansible-playbook -i inventory.ini tasks/build/build_redis_custom.yml

# 3. verify the new binary is everywhere (example marker check)
cd /users/entall/rd
for h in redis0 redis1 redis2 redis3 redis4 ycsb1; do
  echo -n "$h: "; sudo ssh "$h" "strings /users/entall/rd/redis/src/redis-server | grep -c rdma-backpatch-pool-size"
done   # expect 1 on every node
```

The cluster is **restarted on every workload run** (`workload.yml` starts with
`kill_processes` + `clean_runtime` + `start_redisraft_instances`), so you do NOT
restart it manually — just rerun the workload and the new binary + flags take
effect.

---

## 3. Run the experiment

Run from `/users/entall/rd/ansible`. One workload = one full cycle
(restart cluster → load 500K records → start YCSB → pause → reshard → collect).

### The canonical command — PIPELINED chain replication (recommended, fastest)

This is the fully-optimised path: 4 backpatch workers, delayed migration,
completion-driven, **recipient chain pre-warm** (automatic, see below), and
**pipelined chain-replication** (`rdma_chain_pipeline=yes` — forwards each slot's
snapshot to the followers as it's captured, overlapping CHAIN-REPLICATION with
TRANSFER+BACKPATCH instead of running it as a serial tail).

```bash
cd /users/entall/rd/ansible
sudo ansible-playbook -i inventory.ini \
  experiments/custom_reshard_v2_orch_raft_chunked/workload.yml \
  -e redis_variant=custom \
  -e pre_reshard_pause=20 \
  -e rdma_backpatch_pool_size=4 \
  -e rdma_migration_peer_stagger_ms=0 \
  -e ycsb_slotpoll_ms=100 \
  -e rdma_chain_pipeline=yes \
  -e redis_workload=workloada_prod \
  -e experiment_name=custom_reshard_v2_orch_raft_chunked_pipeline_workloada_prod
```

Repeat with `redis_workload=workloadb_prod` (and a matching `experiment_name`) for
the read-heavy workload. Both in one go:

```bash
cd /users/entall/rd/ansible
for W in workloada_prod workloadb_prod; do
  sudo ansible-playbook -i inventory.ini \
    experiments/custom_reshard_v2_orch_raft_chunked/workload.yml \
    -e redis_variant=custom -e pre_reshard_pause=20 \
    -e rdma_backpatch_pool_size=4 -e rdma_migration_peer_stagger_ms=0 \
    -e ycsb_slotpoll_ms=100 -e rdma_chain_pipeline=yes \
    -e redis_workload="$W" \
    -e experiment_name="custom_reshard_v2_orch_raft_chunked_pipeline_${W}"
  echo ">>> $W exit=$?"
done
```

> Drop `-e rdma_chain_pipeline=yes` to get the **non-pipelined** baseline (bulk
> chain-forward after each merge). Everything else (chain pre-warm, 4 workers,
> stagger=0) stays on. The flag defaults **off**, so omitting it is the safe path.

**Chain pre-warm is automatic** — no flag. The reshard playbook's PREPARE-AHEAD
step issues `RDMA CHAIN-WARM` to the recipient (redis3) during the pre-reshard
pause, pre-establishing the chain QPs to redis4+ycsb1 and pre-registering the
1.43 GB source pool off the critical path. (Mirrors the donor `MIGRATE-WARM`.)

> Tip: launch this as a background task; each workload takes ~8–12 min
> (load + 5-min YCSB run + migration + log collection).

### Key parameters

| `-e` flag | Value used | Meaning / why |
|---|---|---|
| `redis_variant` | `custom` | use the AqRaft custom binary (not vanilla). |
| `redis_workload` | `workloada_prod` / `workloadb_prod` | 50/50 vs 95% read, 500K records, `maxexecutiontime=300`. |
| `pre_reshard_pause` | `20` | seconds of steady-state before reshard fires. **18 → migration at ~YCSB sec 20; 20 → ~sec 24.** Gives a pre-migration baseline. |
| `rdma_backpatch_pool_size` | `4` | recipient backpatch merge worker pool (1–32). **Startup flag, read once at `initServer`** — NOT a runtime `CONFIG SET`. Default 1. |
| `rdma_migration_peer_stagger_ms` | `0` | **MUST be 0** for completion-driven (donors ~1–2 s apart). Default is **30000** → if omitted, the 6 sessions serialize 30 s apart → a bogus ~122 s window. |
| `ycsb_slotpoll_ms` | `100` | YCSB client slot-map poll interval; needed for the client to see the new shard and scale. |
| `rdma_chain_pipeline` | `yes` | **Pipeline CHAIN-REPLICATION with the merge** — forward each slot's snapshot to F1 as the backpatch worker captures it, overlapping the recipient→follower write with TRANSFER+BACKPATCH (vs one bulk forward at merge-done). Startup flag (`--rdma-chain-pipeline`, plumbed in `start_redisraft_instances.yml`). Default `no` (bulk forward). |
| `experiment_name` | `..._pipeline_<workload>` | result directory name under `/tmp/experiments/`. |

Which playbook: `workload.yml` uses the **chunked** 2-round orchestrator
(`reshard_cluster_rdma_v2_orchestrated_chunked.yml`) — the right variant for the
increasing-throughput scaling curve. (There is also `workload_nround.yml` /
`-e n_rounds=N` which removes the inter-round NARROW+WRITE_FLIP and cuts the
inter-round gap from ~1.2 s to ~0.05 s, but it's a different orchestration.)

---

## 4. Where results land

`/tmp/experiments/<experiment_name>/`:

- `migration_window.txt` — auto-computed cold-excluded window (sg1 FLIPPING → last
  donor DONE) + full window + cold tax. Printed at the end of `collect_results`.
- `ycsb/ycsb0/tmp/ycsb_output_ycsb0` — per-second YCSB throughput + latency.
- `logs/redis3/tmp/redis_logs/redis3_sg4.log` — recipient (sg4 leader) log:
  backpatch merge, chain-forward, `... pool workers started`.
- `logs/redis{0,1,2}/tmp/redis_logs/redis{0,1,2}_sg{1,2,3}.log` — donor leader logs
  with per-phase `state=PREP/REGISTERING/FLIPPING/TRANSFER/BACKPATCH`,
  `DONE n_slots`, `transfer_ms=...`.

### Sanity-check a run

```bash
D=/tmp/experiments/custom_reshard_v2_orch_raft_chunked_pipeline_workloada_prod
L="$D"/logs/redis3/tmp/redis_logs/redis3_sg4.log
# 1. failed=0 in the ansible recap (check the run output)
grep "pool workers started" "$L" | tail -1                 # expect "+ 4 pool workers"
grep -E "COLD-EXCLUDED|FULL" "$D"/migration_window.txt     # the migration window
# 2. PIPELINE actually ran (expect 6 each: 3 donors x 2 rounds)
grep -c "spawned pipelined forwarder" "$L"
grep -c "pipelined forward complete" "$L"
grep -c "src pool not pre-registered" "$L"                 # warm working → 0 cold fallbacks (benign if >0)
# 3. DATA INTEGRITY (run while the cluster is still up, before the next run):
#    leader (redis3) and both sg4 followers (redis4, ycsb1) must report the SAME DBSIZE.
for h in redis3 redis4 ycsb1; do
  echo -n "$h: "; sudo ssh $h "/users/entall/rd/redis/src/redis-cli -p 8000 DBSIZE"
done
```

Reference numbers (4 workers, 2-round chunked, delay20, chain-warm):

| variant | migration window (cold-excl) | scaling (pre → post) |
|---|---|---|
| **pipelined** (`rdma_chain_pipeline=yes`) | **~6.5 s** | ~105K→122K (+16%) / ~198K→226K (+14%) |
| chain-warm, non-pipelined | ~8.8 s | ~106K→120K (+13%) / ~198K→226K (+14%) |
| no warm, non-pipelined | ~9.4 s | similar |
| 1-worker (single backpatch worker) | workloada 11.0 s / workloadb 10.9 s | — |

Window progression: **9.4 s → 8.8 s (chain-warm) → 6.5 s (pipelined)**, ≈ −31% cumulative.
The pipelined run's CHAIN-REPLICATION still does its full ~0.55–0.84 s of work per
session — it's overlapped, so only the small exposed tail past merge-done counts.

---

## 5. Generate the figures

Both plot scripts live in the repo root (`/users/entall/rd`). Plots are written to
a dir you own; create one first (the repo `ansible/` dir is root-owned):

```bash
cd /users/entall/rd
PLOTS=ansible/plots_20260609_pipeline
sudo mkdir -p "$PLOTS" && sudo chown entall:streamstore-PG0 "$PLOTS"
```

### (a) Throughput + latency timeseries (smoothed, 0–100 s)

```bash
for W in workloada workloadb; do
  D=/tmp/experiments/custom_reshard_v2_orch_raft_chunked_pipeline_${W}_prod
  python3 plot_ycsb_timeseries.py "$D" --xmin 0 --xmax 100 --smooth 5 \
    -o "$PLOTS/ts_${W}.png"
done
```

`plot_ycsb_timeseries.py` flags: `--xmin/--xmax` (seconds), `--smooth N`
(centered rolling-median window over throughput **and** latency; smooths the
migration-start spike; default 1 = off). The migration band + the cold-excluded
window footer are drawn automatically from the logs.

### (b) Phase-breakdown Gantt (per-session, CONNECT…DONE COMMIT)

```bash
for W in workloada workloadb; do
  D=/tmp/experiments/custom_reshard_v2_orch_raft_chunked_pipeline_${W}_prod
  python3 plot_phase_gantt.py "$D" "$PLOTS/gantt_${W}.png"
done
```

`plot_phase_gantt.py <expdir> <out.png>` — data-driven from the donor + recipient
logs. Lanes: CONNECT, REGISTER, CH_OWNSHIP, TRANSFER, BACKPATCH,
CHAIN-REPLICATION, DONE COMMIT. Bars are the 6 donor sessions (`sg<donor>.<round>`,
navy=round1 / light-blue=round2), red border = cold `ibv_reg_mr` chain-pool
sessions, in-bar text = phase duration. Computer Modern, compact wide layout,
"migration time" span arrow on top. **Auto-detects a pipelined run** (`"pipelined
per-slot"` in the log): then it draws CHAIN-REPLICATION from the session start
(its true overlapping span) instead of just the `[merge_done → chain_wrote]` tail.
Sub-pixel phases (DONE COMMIT ~5 ms, CH_OWNSHIP ~12 ms) get a minimum render width
so they stay visible; the label always shows the true duration.

---

## 6. Gotchas (things that cost time before)

1. **`sudo` everything outbound** — plain `ansible-playbook`/`ssh` → all nodes
   `unreachable`.
2. **`rdma_migration_peer_stagger_ms=0`** — omitting it defaults to 30000 →
   sessions 30 s apart → ~122 s "window" that is NOT a real result.
3. **rsync source before building** — `build_redis_custom` builds each node's
   local source; edits on redis0 don't reach peers otherwise. Verify with a
   `strings | grep <marker>` on each node.
4. **`rdma_backpatch_pool_size` is a startup flag** (immutable config) — it only
   takes effect because the cluster is restarted each run; a runtime `CONFIG SET`
   does nothing.
5. **MIGRATE-WARM / registration must be off the main thread** — already handled
   in code; if you touch it, keep `ibv_reg_mr` async or the leader stalls and
   WRITE_FLIP comes back MOVED.
6. **Plot output dirs / source files are root-owned** — `sudo mkdir` + `chown`, or
   `sudo chown` the file before editing.
7. **YCSB clock is +6h vs the redis logs** — when correlating timestamps by hand,
   subtract 6 h from YCSB to match the redis/migration logs.
8. **`rdma_chain_pipeline` is a startup flag too** — plumbed as `--rdma-chain-pipeline`
   in `start_redisraft_instances.yml`. If you add it but forget to rebuild/redeploy,
   the recipient boots with it off and the run silently uses the bulk path (verify
   with the `pipelined forward complete` count = 6 in the sanity check).
9. **ALWAYS verify data integrity for pipelined runs** — a chain-forward bug can
   corrupt *follower* data while the leader looks fine and scaling looks normal.
   The `DBSIZE` match across redis3/redis4/ycsb1 (sanity check #3) is the real proof.
10. **pidstat now killed in teardown** — `kill_processes.yml` kills pidstat (it used
    to leak across runs → a multi-GB log that hung `collect_results`). If a run hangs
    in collection, check for stray `pidstat` on the nodes and a huge
    `systat_logs/*_pidstat.txt`.

---

## 7. File map

| Path | What |
|---|---|
| `ansible/experiments/custom_reshard_v2_orch_raft_chunked/workload.yml` | the experiment pipeline (chunked 2-round) |
| `ansible/tasks/cluster/reshard_cluster_rdma_v2_orchestrated_chunked.yml` | reshard orchestration; PREPARE-AHEAD issues donor `MIGRATE-WARM` + recipient `RDMA CHAIN-WARM` |
| `ansible/tasks/cluster/start_redisraft_instances.yml` | launches redis-server (`--rdma-backpatch-pool-size`, `--rdma-chain-pipeline`, …) |
| `ansible/tasks/cluster/pause_pre_reshard.yml` | the `pre_reshard_pause` |
| `ansible/tasks/teardown/kill_processes.yml` | teardown (now kills pidstat too) |
| `redis/src/cluster_rdma.c` | `RDMA CHAIN-WARM` cmd, the pipelined-forward thread + snapshot-ready signal |
| `redis/src/cluster_rdma_chain.c` | `rdmaLeaderChainForwardPerSlot` (bulk) + `rdmaLeaderChainForwardPipelined` |
| `migration_window.py` | computes the cold-excluded window (run by `collect_results`) |
| `plot_ycsb_timeseries.py` | throughput/latency timeseries |
| `plot_phase_gantt.py` | per-phase Gantt (auto-detects pipelined runs) |
