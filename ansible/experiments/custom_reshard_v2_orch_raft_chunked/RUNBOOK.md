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

### The canonical command (4 workers, delayed migration, completion-driven)

```bash
cd /users/entall/rd/ansible
sudo ansible-playbook -i inventory.ini \
  experiments/custom_reshard_v2_orch_raft_chunked/workload.yml \
  -e redis_variant=custom \
  -e pre_reshard_pause=20 \
  -e rdma_backpatch_pool_size=4 \
  -e rdma_migration_peer_stagger_ms=0 \
  -e ycsb_slotpoll_ms=100 \
  -e redis_workload=workloada_prod \
  -e experiment_name=custom_reshard_v2_orch_raft_chunked_pool4_delay20_workloada_prod
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
    -e ycsb_slotpoll_ms=100 \
    -e redis_workload="$W" \
    -e experiment_name="custom_reshard_v2_orch_raft_chunked_pool4_delay20_${W}"
  echo ">>> $W exit=$?"
done
```

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
| `experiment_name` | `..._pool4_delay20_<workload>` | result directory name under `/tmp/experiments/`. |

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
D=/tmp/experiments/custom_reshard_v2_orch_raft_chunked_pool4_delay20_workloada_prod
# failed=0 in the ansible recap (check the run output)
grep "pool workers started" "$D"/logs/redis3/tmp/redis_logs/redis3_sg4.log | tail -1   # expect "+ 4 pool workers"
grep -E "COLD-EXCLUDED|FULL" "$D"/migration_window.txt                                  # the migration window
```

Reference numbers (4 workers, 2-round chunked, delay20):

| | migration window (cold-excl) | scaling (pre → post) |
|---|---|---|
| workloada (50/50) | ~9.4 s | ~103K → ~115K ops/s (+12%) |
| workloadb (95% read) | ~9.5 s | ~177K → ~200K ops/s (+13%) |

1-worker baseline for comparison: workloada 11.01 s, workloadb 10.87 s.

---

## 5. Generate the figures

Both plot scripts live in the repo root (`/users/entall/rd`). Plots are written to
a dir you own; create one first (the repo `ansible/` dir is root-owned):

```bash
cd /users/entall/rd
PLOTS=ansible/plots_pool4_delay20
sudo mkdir -p "$PLOTS" && sudo chown entall:streamstore-PG0 "$PLOTS"
```

### (a) Throughput + latency timeseries (smoothed, 0–100 s)

```bash
for W in workloada workloadb; do
  D=/tmp/experiments/custom_reshard_v2_orch_raft_chunked_pool4_delay20_${W}_prod
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
  D=/tmp/experiments/custom_reshard_v2_orch_raft_chunked_pool4_delay20_${W}_prod
  python3 plot_phase_gantt.py "$D" "$PLOTS/gantt_${W}.png"
done
```

`plot_phase_gantt.py <expdir> <out.png>` — data-driven from the donor + recipient
logs. Lanes: CONNECT, REGISTER, CH_OWNSHIP, TRANSFER, BACKPATCH,
CHAIN-REPLICATION, DONE COMMIT. Bars are the 6 donor sessions (`sg<donor>.<round>`,
filled=round1 / open... here both rounds drawn solid w/ alpha), red border = the
cold `ibv_reg_mr` chain-pool sessions, in-bar text = phase duration. Computer
Modern font, golden-ratio canvas, "migration time" span arrow on top.

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

---

## 7. File map

| Path | What |
|---|---|
| `ansible/experiments/custom_reshard_v2_orch_raft_chunked/workload.yml` | the experiment pipeline (chunked 2-round) |
| `ansible/tasks/cluster/reshard_cluster_rdma_v2_orchestrated_chunked.yml` | the reshard orchestration |
| `ansible/tasks/cluster/start_redisraft_instances.yml` | launches redis-server (passes `--rdma-backpatch-pool-size`, etc.) |
| `ansible/tasks/cluster/pause_pre_reshard.yml` | the `pre_reshard_pause` |
| `migration_window.py` | computes the cold-excluded window (run by `collect_results`) |
| `plot_ycsb_timeseries.py` | throughput/latency timeseries |
| `plot_phase_gantt.py` | per-phase Gantt |
