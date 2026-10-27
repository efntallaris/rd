# Runbook: the crash-recovery suite of the 3 -> 6 scale-out, and what changed for the older runs

Last updated 2026-10-27. Part 1 is how to run the new experiments (the crash suite C1-C17 + S1-S9,
on 300k or 10M keys). Part 2 is every setting, default and behaviour that changed on 20-23 October,
and what that means for the experiments of `RUNBOOK_SCALEOUT.md` (3 -> 4 and 3 -> 6 campaigns of
5-6 October). Results: `experiments/2026-10-22_scaleout_3to6_crash_recovery_10m/README.md` (10M)
and `experiments/2026-10-20_scaleout_3to6_crash_recovery/README.md` (300k).

Everything runs on redis0 (the controller), as your user: the harness calls `sudo` itself. The repo
is root-owned: edit with `sudo`.

---

## Part 1 — running the new experiments

### 1.1 Prerequisites (once per cluster)

1. Cluster set up as in `ansible/lincheck/HOWTO_LINCHECK.md` (inventory `inventory_scale3to6.ini`,
   hosts redis0-5 and ycsb0-1). Sudo works on every host; the fresh-cluster notes (e.g. the broken
   `/usr/local` rdma-core) are in that file.
2. Build **redis and redisraft on every host from the same sources** (this branch). The crash suite
   relies on server changes of 20-23 Oct (Part 2); a host with an older build breaks recovery in
   ways that look like protocol bugs.
3. Build YCSB (`ycsb_client`, the redis binding: `RedisClient.java` changed, see Part 2) and copy the
   workload files to ycsb0 and ycsb1 under `/users/entall/rd/workloads/`. The 10M runs need
   `ycsb_client/workloads/workloada_prod_10m_run3min_uniform`; the 300k runs `workloada_lin_quick_uniform`.
4. **10M only:** the 6-group snapshot `ws10m_3to6` (loads the 10M keys once; the runs then restore
   it instead of loading):
   ```bash
   cd /users/entall/rd/ansible
   (source experiments/custom_scaleout_3to6/env.sh      # the same rdma_* flags as the runs
    sudo ansible-playbook -i inventory_scale3to6.ini experiments/custom_scaleout_3to6/dataset_create.yml \
      $EXTRA_ANSIBLE_ARGS -e redis_variant=custom -e redis_workload=workloada_prod_10m_run3min_uniform \
      -e dataset_snapshot=ws10m_3to6)
   ```
   (Single-recipient snapshots such as `ws30m` have no sg5/sg6 and cannot be used.)
5. matplotlib for your user, for the figures: `python3 -m pip install --user matplotlib`.
6. Nothing else running on the cluster (one run at a time; a run kills all redis instances first).

### 1.2 The whole suite

```bash
# 10M keys (PROFILE=full): 25 runs, ~11 min each, ~4.5 h
cd /users/entall/rd/experiments/2026-10-22_scaleout_3to6_crash_recovery_10m
setsid nohup ./run.sh > /tmp/suite10m.log 2>&1 < /dev/null &

# 300k keys (quick profile): ~6 min each, ~2.5 h
cd /users/entall/rd/experiments/2026-10-20_scaleout_3to6_crash_recovery
setsid nohup ./run.sh > /tmp/suite300k.log 2>&1 < /dev/null &
```

`run.sh` runs the scenarios in a row (`ansible/experiments/custom_scaleout_3to6/run_all.sh`), copies
each scenario's data into the experiment's `data/<S>/` (before a later run overwrites `/tmp`), then
draws the figures and the suite table (`figures.sh`). Progress, one line per finished run:

```bash
cut -c1-100 $OUT_ROOT/summary.txt        # OUT_ROOT is printed at the start; default /tmp/lincheck/crash_recovery_<date>
```

A line reads `C7 q=yes rc=0 OVERALL: PASS | linearizable ... | migration=PASS crashes=PASS replicas=PASS ...`:
OVERALL = lincheck (the 200-key probe history is linearizable) AND postcheck (migration complete on
the recipient leader, live replicas identical, no key of the dataset missing).

### 1.3 Some scenarios, or another configuration

```bash
./run.sh C7 S1                                   # only these; their data/ and figures are replaced
OUT_ROOT=/tmp/lincheck/try1 ./run.sh C3          # choose where the raw run goes
ROUNDS=6 SCALEOUT_MODE=sequential CHUNK_SLOTS=57 ./run.sh S1      # another migration configuration
SCALEOUT_EXTRA_ARGS="-e rdma_peer_probe_grace_ms=50" ./run.sh C7  # any extra ansible -e flag
ELECTION_TIMEOUT_MS=1000 ./run.sh S1             # Raft election timeout (default 500 ms)
```

To try something **without touching the experiment's `data/`**, call the runner directly with the
same environment (this is how the probe test of 23 Oct was run):

```bash
export ROUNDS=4 SCALEOUT_MODE=pairwise CHUNK_SLOTS=8 PROFILE=full DATASET_SNAPSHOT=ws10m_3to6
export WORKLOAD=workloada_prod_10m_run3min_uniform YCSB_THREADS=400
export SCALEOUT_EXTRA_ARGS="-e raft_election_timeout=500 -e rdma_peer_probe_grace_ms=50"
export OUT_ROOT=/tmp/lincheck/probe_test
/users/entall/rd/ansible/experiments/custom_scaleout_3to6/run_all.sh C7 S1 S4 C4
```

Environment of `run.sh` (defaults = the configuration of the 10M results):

| variable | 10M `run.sh` | 300k `run.sh` | what |
|---|---|---|---|
| `ROUNDS` | 4 | 4 | rounds per pair (4 x 682 slots) |
| `SCALEOUT_MODE` | pairwise | pairwise | pairwise / sequential / pipelined / parallel |
| `CHUNK_SLOTS` | 8 | 12 | slots per announced chunk (`rdma_transfer_chunk_slots`) |
| `ELECTION_TIMEOUT_MS` | 500 | 500 | Raft election timeout |
| `PROFILE` | full | quick | full = restore `DATASET_SNAPSHOT`; quick = load 300k keys |
| `DATASET_SNAPSHOT` | ws10m_3to6 | – | |
| `WORKLOAD`, `YCSB_THREADS` | workloada_prod_10m_run3min_uniform, 400 | workloada_lin_quick_uniform, 400 | per YCSB client |
| `SCALEOUT_EXTRA_ARGS` | | | more `-e` flags for the playbook |
| `OUT_ROOT` | /tmp/lincheck/crash_recovery_<date> | same | raw output of the runs |

### 1.4 The scenarios

All faults hit pair A: donor sg1 (leader redis0, followers redis1, redis2) -> recipient sg4 (leader
redis3, chain F1 redis4 -> F2 redis5), in its first round unless noted. The table of what each one
does is in `experiments/2026-10-20_scaleout_3to6_crash_recovery/README.md`; the definitions are in
`ansible/experiments/custom_scaleout_3to6/scenarios.env`.

- **Fail-stop** (a crashed node stays down): C1-C10, C14, C17, S1-S5, S8, S9. All pass.
- **Network / timeouts:** C15 (5 s TCP partition between the two leaders; RDMA queue pairs stay
  up), C16 (election 150 ms, raft request 20 ms, peer probe 50 ms, no fault). Both pass.
- **Beyond fail-stop:** C11-C13 restart a crashed recipient node. They pass lincheck and fail
  postcheck: the restarted replica is not refilled (README of the 10M experiment). Expected.

**Adding a scenario** (`scenarios.env`, one `case` entry):

```bash
  C18) # what it does
    LABEL="C18-short-name"
    HOOKS="redis3:after_recp_durable:1500"        # optional: one-shot pause(s) host:name[:ms]
    STEPS="arm redis3 sg4 'TEST HOOK after_recp_durable'; kill redis3 sg4" ;;
```

- `STEPS` (run by `ansible/crash/fault_steps.sh`, full description at its top), separated by `;`:
  - `arm HOST SG REGEX [N]`: wait until REGEX has appeared N times (default 1) in that instance's log;
  - `armany REGEX N HOST:SG ...`: the same over several logs, and the first to reach N becomes `@`;
  - `kill HOST SG` or `kill @`: kill -9;
  - `restart HOST SG` or `restart @`: relaunch with the saved start script (the raft log on disk is kept);
  - `stop HOST SG SECS`: SIGSTOP, with SIGCONT after SECS;
  - `partition HOSTA HOSTB SECS`: drop IP traffic between the hosts (TCP only; RDMA queue pairs stay up);
  - `cmd HOST SG ARGS...`: a Redis command, e.g. `CONFIG SET raft.election-timeout 2000`;
  - `waitsync HOSTA SGA HOSTB SGB SECS`: until both hold the same number of keys;
  - `sleep SECS`.
- Without `STEPS`, the single-kill form: `ARM_HOST`, `ARM_LOG`, `ARM_MARKER`, `ARM_COUNT`,
  `TARGET_HOST`, `PIDFILE` (as S1-S9).
- `HOOKS`: `rdmaTestHook(name)` in the server pauses once for `ms` (default 1500) when
  `/tmp/rdma_hook_<name>` exists, logs `TEST HOOK <name>` and deletes the file. Names:
  `before_open`, `before_init`, `before_txn_done`, `before_recp_durable`, `after_recp_durable`,
  `before_forward`, `follower_forward` (redis) and `before_recp_open` (redisraft).
  `run_crash_scenario.sh` creates the files before the run and removes them after.
- `SCEN_EXTRA_ARGS`: extra ansible flags for this scenario only (as C16).
- Add it to `SUITE` in the experiment's `run.sh` if it belongs to the suite.

### 1.5 Figures, table, results page

```bash
cd /users/entall/rd/experiments/2026-10-22_scaleout_3to6_crash_recovery_10m
./figures.sh              # all scenarios under data/; or ./figures.sh C7 S1
```

Writes into `figures/`: `paper_<S>_recovery.pdf` (one column of a two-column paper, 3.33 in) and
`paper_<S>_recovery_wide.pdf` (7 in), each with a `.png` preview; `<S>_recovery.png` (detailed
timeline), `<S>_table.txt` (every phase), `<S>_recovery.json` (input of the paper figure);
`suite_table.md` / `.csv` (one row per scenario). No cluster needed. The probe histories
(`data/<S>/lincheck/history.jsonl.gz`) are needed for the client-outage numbers.

The HTML results page (table with keys and GB moved, every paper figure, optionally every run of
every batch):

```bash
python3 experiments/tools/scaleout/build_suite_page.py \
  experiments/2026-10-22_scaleout_3to6_crash_recovery_10m /tmp/suite_page \
  "/tmp/lincheck/<out root 1>=Overnight suite=baseline build" \
  "/tmp/lincheck/<out root 2>=Rerun 1=what changed"
# -> /tmp/suite_page/index.html + figs/
```

### 1.6 Diagnosing a slow recovery

- The scale-out driver (`ansible/scaleout_pairs.py`) appends every call it makes to a server to
  `/tmp/scaleout_driver_trace.log` on redis0: start time (epoch), duration, target, command, first
  line of the reply. A long gap between two rounds shows up as one call with a long duration
  (that is how the dead-donor waits of 23 Oct were found). The file grows by ~150 KB per run;
  delete it when you like.
- Pair A's longest wait between rounds: TXN_DONE of a round to TXN_START of the next in the donor
  group's logs (`data/<S>/lincheck/logs/redis*/redis*_sg1.log`); fault-free it is ~0.02 s.
- Per-chunk timing in the recipient leader's log: `PERCHUNK REPL/INDEX mig_id=... seq=... done`,
  `PERCHUNK CHAIN sess=... seq=... start`.

---

## Part 2 — settings and behaviour that changed (20-23 Oct), and the older experiments

Everything below is on branch `aqueduct_broken`. The older experiments (`RUNBOOK_SCALEOUT.md`,
`ansible/campaign/run_campaign.sh`, 3 -> 4 and 3 -> 6, quick and 30M) run with the same playbook and
server, so they pick up these changes automatically. The table says what is new, its default, and what
to pass to get the old behaviour back if you need to compare with a result from before 20 Oct.

### 2.1 Server (redis) configuration

| setting | default | new / changed | effect | old behaviour |
|---|---|---|---|---|
| `rdma-chunk-notice-rdma` | yes | new (22 Oct) | the donor announces each landed chunk with an RDMA SEND on the migration queue pair instead of a TCP message; the recipient advertises support (`OK NOTICE` in the INIT reply), else the donor falls back to TCP. Log: `TRANSFER ... chunk notices over RDMA`, `DONE-SLOTS-CHUNK ... via=rdma` | `redis-cli -p <port> CONFIG SET rdma-chunk-notice-rdma no` on the donors (runtime; no playbook variable) |
| `rdma-chain-follower-prereg-pools` | 8 | new (20 Oct), immutable | follower ring pools pre-registered per recipient instance; `env.sh` passes `ROUNDS + 1` (max 8), which lets ROUNDS=4 (683 slots per round) fit in memory | `-e rdma_chain_follower_prereg_pools=8` |
| `rdma-chain-warm-on-promote` | yes (playbook) | extended (23 Oct) | a newly elected recipient leader now also warms its chain (connections + landing registration) when the round it inherits was already closed (`MGN-RECOVER recipient-closed`, term passed from redisraft) | `-e rdma_chain_warm_on_promote=no` (also turns off the older warm-up) |
| `rdma-peer-probe-grace-ms` | 50 (server), **-1 (playbook)** | unchanged, re-tested | parallel follower probe at chain set-up: followers that answer within N ms of the first are used, a dead one is left out at once. Off in the playbook since 6 Oct (S4 dip on 3 -> 4). Tested on 10M on 23 Oct: C7 -1.2 s, S1 -0.6 s, C4 -1.2 s, no dip in S4/C4 | turn on with `-e rdma_peer_probe_grace_ms=50` |
| test hooks | inactive | new | `rdmaTestHook(name)`: one `access("/tmp/rdma_hook_<name>")` per call; does nothing unless the file exists | – |

New log lines (for analysis scripts): `PERCHUNK ...` (per-chunk timing), `TEST HOOK <name>`,
`AqRaft recipient-closed: term=N -- chain warm-up started`.

### 2.2 Server behaviour (no setting)

- **Held follower batch counts as received** (`CHAIN-STATUS ... RECEIVED`), and a follower promoted
  while holding a round it has not applied yet applies it at once (`rdmaChainApplyHeldRange`): a new
  leader fetches the round from it instead of having the donor re-ship it (C6, C17).
- **Forward and apply of a held pool never overlap** (`chainFwdWriteBegin/End`).
- **`CHAIN-PREP` is re-sent after a reconnect**, so a restarted follower gets a fresh pool address.
- **Chain sessions get unique ids** (`chainSessAlloc` seeded from the clock): a re-formed chain no
  longer collides with an earlier session of the same round.

### 2.3 Playbook (`ansible/tasks/cluster/start_redisraft_instances.yml`)

- Each instance's start command is also written to `/tmp/redis_start_<host>_<sg>.sh` and run from
  there; `fault_steps.sh restart` re-uses it (redis rewrites its process title, so
  `/proc/<pid>/cmdline` no longer has the arguments). Note for edits: the heredoc needs the blank
  line before `AQRAFT_START`, and no apostrophe in that task (ansible's argument splitter).
- Passes `rdma_chain_follower_prereg_pools` when defined.

### 2.4 Harness

| file | change |
|---|---|
| `ansible/crash/run_crash_scenario.sh` | scenarios C1-C99 besides S1-S9; `STEPS` (runs `fault_steps.sh`), `HOOKS`, `SCEN_EXTRA_ARGS`; logs RESTARTED / STOPPED / PARTITIONED / IN SYNC events into `run.log` |
| `ansible/crash/fault_steps.sh` | new: the step injector (1.4) |
| `ansible/lincheck/run_lin_scenario.sh`, `run_all_lin.sh` | accept C-scenarios; wait for `fault_steps.sh` like `crash_inject.sh` |
| `ansible/experiments/custom_scaleout_3to6/scenarios.env` | C1-C17 |
| `ansible/experiments/custom_scaleout_3to6/env.sh` | mode `pairwise` accepted (default still sequential; the experiments' `run.sh` set pairwise); `rdma_chain_follower_prereg_pools=ROUNDS+1` |
| `ycsb_client/workloads/workloada_prod_10m_run3min_uniform` | new 10M workload |

### 2.5 Scale-out driver (`ansible/scaleout_pairs.py`, 3 -> 6 only)

Affects every 3 -> 6 run, the older campaigns included:

| change | before | now |
|---|---|---|
| mode `pairwise` | – | all rounds of pair A, then B, then C (the next pair's first hop is the link the previous pair's tail hop just freed) |
| leader lookup | members asked one after the other; a just-killed leader (accepts, never answers) held every lookup ~1.2 s | all members at once, answer 50 ms after the first leader replies, 3 s deadline |
| re-ship check (donor re-sends a round under id 8e17 + first slot) | once per second, from 2 s after dispatch | every 0.1 s from the start |
| dead donor | the driver kept asking it (status and re-ship check, 1-2 s timeout per call) | once a status call goes unanswered, only the recipient's durability (`MGN-RESUME-STATUS`) decides; the donor is not asked again |
| trace | – | `/tmp/scaleout_driver_trace.log` (1.6) |

Effect on 10M (pair A, longest wait between rounds, overnight -> final): C7 0.96 -> 0.14 s,
S1 0.48 -> 0.06 s, C17 0.82 -> 0.07 s, C3 1.30 -> 0.51 s. Fault-free runs are unchanged
(HEALTHY 7.99 s).

### 2.6 YCSB client (`ycsb_client/.../RedisClient.java`)

An error reply from the server (`JedisDataException`) no longer marks the endpoint dead; only
connection errors do. Rebuild YCSB on ycsb0/ycsb1 to get it.

### 2.7 Analysis tools (`experiments/tools/scaleout/`)

| file | what |
|---|---|
| `recovery_breakdown.py` | phases of a crash run from the logs (detailed figure, table, JSON). 27 Oct: a round's replication start is searched up to the round's end, not 0.5 s after its copy (a new leader's chain can take ~1 s; C7, C4, C13 showed no replication bar) |
| `paper_recovery.py` | paper figure from the JSON: fixed rows (Recipient, Donor, Recovery, Copy, Replication, Index update), legend, 3.33 in or any width |
| `suite_table.py` | one row per scenario (`suite_table.md` / `.csv`) |
| `build_suite_page.py`, `suite_page.css` | the HTML results page (1.5) |

### 2.8 Re-running an older experiment with the new defaults

```bash
cd /users/entall/rd/ansible/campaign
./run_campaign.sh small 3to6 <name> HEALTHY S1          # as in RUNBOOK_SCALEOUT.md
```

- 3 -> 6 now runs the new driver (2.5); its mode stays what the campaign passes (`SCALEOUT_MODE`,
  default sequential in `env.sh`). For the configuration of the crash suite add
  `ROUNDS=4 SCALEOUT_MODE=pairwise CHUNK_SLOTS=8`.
- Chunk notices go over RDMA (2.1). For a like-for-like comparison with a result from before
  22 Oct, set `rdma-chunk-notice-rdma no` on the donors after start-up.
- 3 -> 4 uses its own driver (`reshard_cluster_rdma_v2_orchestrated_nround.yml`), not changed;
  only the server changes (2.1, 2.2) apply.
- The parallel follower probe stays off unless you pass `-e rdma_peer_probe_grace_ms=50`.
