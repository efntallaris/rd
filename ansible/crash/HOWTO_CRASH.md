# HOWTO — AqRaft crash-scenario experiments + artifact

This guide runs fault-injection experiments during an AqRaft RDMA cluster reshard
(donors **sg1/sg2/sg3** → recipient **sg4**) and produces a single HTML artifact
summarising all scenarios.

All commands run from `ansible/crash/` on the control node, as **root/sudo**
(the repo is root-owned). Topology comes from `inventory.ini` + `group_vars/all.yml`:

| shardgroup | leader | followers | port |
|---|---|---|---|
| sg1 (donor) | redis0 | redis1, redis2 | 8000 |
| sg2 (donor) | redis1 | redis2, redis0 | 8001 |
| sg3 (donor) | redis2 | redis0, redis1 | 8002 |
| sg4 (recipient) | redis3 | redis4, redis5 (chain) | 8000 |

## The scenarios

| id | what it injects | kills |
|----|-----------------|-------|
| **HEALTHY** | no crash — the reference baseline | — |
| **S1** | recipient LEADER crash mid-transfer | redis3 (sg4 leader) |
| **S2** | donor LEADER crash mid-transfer | redis0 (sg1 leader) |
| **S3** | donor FOLLOWER crash mid-transfer | redis1 (sg1 follower) |
| **S4** | recipient FOLLOWER (chain) crash | redis4 (sg4 follower) |
| **S5** | donor LEADER crash **after** its data has fully landed on the recipient | redis0 (sg1 leader) |

Each scenario's arm-marker + kill-target is defined in **`scenarios.env`**. The kill is a
backgrounded `crash_inject.sh` armed off a log marker (see that file); the reshard
itself is the standard orchestrated reshard playbook, unchanged.

- **S2 vs S5**: S2 kills the donor leader *while it is still transferring* (exercises the
  resume/recovery path). S5 waits until the donor's own migration worker logs
  `id=<n> DONE n_slots=<n>` — i.e. its data is confirmed applied on the recipient — then
  kills it (tests that a post-transfer donor loss is a non-event: a new sg1 leader is
  elected and NARROW/reconcile still complete the ownership handoff).

## Prerequisites (once)

1. Cluster provisioned and the custom redis binary deployed to all hosts:
   ```bash
   cd ansible
   sudo ansible-playbook -i inventory.ini tasks/build/deploy_redis_binary.yml
   ```
2. YCSB built on **both** ycsb0 and ycsb1, and the workload file present on both
   (see `memory/aqraft-30m-run-prereqs.md` for the jar/workload gotchas).
3. `python3` with `matplotlib` on the control node (for analysis + figures).

## 1. Run a single scenario

```bash
cd ansible/crash

# production scale (30M keyspace, 10-min run, 100 threads/client):
./run_crash_scenario.sh S5

# smaller/faster debug scale (500k keyspace) — good for a quick check:
./run_crash_debug.sh S5
```

Override scale/threads/pause for `run_crash_scenario.sh` via env:
```bash
YCSB_THREADS=100 WORKLOAD=workloada_prod_30m_run10min PRE_PAUSE=60 ./run_crash_scenario.sh S2
```
Each run prints a PASS/CHECK verdict (`verdict.sh`) and records the injection to
`/tmp/crash_inject/<LABEL>.txt`.

## 2. Run the whole campaign (HEALTHY + S1..S5)

```bash
cd ansible/crash
# defaults: all scenarios, 30M workload, 100 threads, 60s pre-pause
./run_all_scenarios.sh

# a subset / different scale:
ORDER="HEALTHY S2 S5" WORKLOAD=workloada_debug_500k YCSB_THREADS=50 PRE_PAUSE=20 ./run_all_scenarios.sh
```
Results (ycsb output + redis logs + run logs, one subdir per scenario) collect under
`/tmp/crash_inject/campaign/` (override with `RESULTS=/path`). Long-running — launch under
`nohup` if the session may drop:
```bash
nohup ./run_all_scenarios.sh > /tmp/crash_inject/campaign/sweep.out 2>&1 &
```

## 3. Analyse + produce the artifact

```bash
cd ansible/crash
# 1) compute per-scenario metrics + timeline figures
python3 analyze_crash.py /tmp/crash_inject/campaign
#    30M defaults assume pre=(25,55) plateau=(200,550) mig=60; for debug 500k use e.g.:
#    python3 analyze_crash.py /tmp/crash_inject/campaign --pre 20 40 --plateau 120 300 --mig 20

# 2) build the self-contained HTML artifact
python3 build_artifact.py /tmp/crash_inject/campaign
#    -> /tmp/crash_inject/campaign/crash_artifact.html
```
`analyze_crash.py` writes `metrics.json` + `<S>/fig.png`; `build_artifact.py` embeds the
figures and emits `crash_artifact.html` (theme-aware, no external assets). Open it in a
browser, or publish it via Claude Code's Artifact tool.

**Reading the numbers:** `rise` = post-migration plateau ÷ pre-migration baseline (both
vary a few % run-to-run, so compare **plateaus**). A scenario **PASSes** when it has 0 crash
signatures, 0 YCSB UPDATE errors, and a plateau above the pre-migration baseline.

## Troubleshooting

- **Marker never fires / no kill**: confirm the arm log path in `scenarios.env` exists on
  the arm host during the run (`/tmp/redis_logs/<host>_<sg>.log`); check `crash_inject.sh`
  output in the scenario's `run.log`.
- **Empty ycsb/log files after a run**: collection is best-effort; the leader-log snapshot
  (armed at t_kill+60s) is the fallback. Re-run if a scenario's `ycsb0=` count is 0.
- **Analysis windows look wrong** (rise/recovery off): the pre/plateau/mig windows are
  scale-dependent — pass `--pre --plateau --mig` to match your workload's run length.
- **Read-only workloads**: the reshard's recovery-drain needs client write activity; a
  pure read-only workload can fail to drain. Use a read/update workload (e.g. workloada*).
