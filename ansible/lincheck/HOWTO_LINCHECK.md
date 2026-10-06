# HOWTO — linearizability + fault-tolerance check for the AqRaft migration

This harness checks that a migration (donors **sg1/sg2/sg3** → recipient **sg4**) is
**correct** under the crash scenarios: clients only ever see linearizable values, no
acknowledged write is lost, the migration completes, and every surviving replica ends up
with the same data.

> **This is an occasional correctness check, not part of every run.**
> Normal experiments and performance runs (`ansible/crash/run_crash_scenario.sh`,
> `workload_nround.yml`) do not use it and are unchanged. Run this after changing the
> migration, chain, recovery, merge or client-routing code, and before taking numbers you
> want to trust. It adds its own client load and is not meant for performance measurement.

All commands run on the control node (**redis0**) from `ansible/lincheck/`. The repo is
root-owned; the scripts use `sudo` themselves where needed. Topology, scenarios S1–S5 and
their kill points are the same as in `ansible/crash/HOWTO_CRASH.md`.

## What it checks

Each run produces one **OVERALL: PASS/FAIL**, which requires all of the following:

| check | what it verifies | how |
|---|---|---|
| **linearizable** | every GET/SET/DEL on the test keys is explained by one copy of each key changing at a single instant inside each op's call–reply window (incl. DEL's reply count) | `lincheck` (Go, Porcupine) on the recorded history, per key |
| **lost-write** | the final value of each key is one of the writes that may legally be last | `lincheck`, on the final read pass |
| **migration** | the playbook succeeded and the *current* sg4 leader reports all 3 donor ranges durable | `postcheck.py` → `RDMA MGN-RESUME-STATUS` |
| **crashes** | no crash signature in any node's log (the injected kill is a SIGKILL and leaves none) | `postcheck.py` |
| **replicas** | every live sg4 replica holds the same migrated keys + values (the durability claim behind `INDX_UPD`) | `postcheck.py`, local SCAN/GET via `RAFT.DEBUG EXEC` |
| **donors** | the live replicas of each donor group agree on their migrated range | `postcheck.py` |
| **bulk** | every YCSB key of the migrated slots (donor's frozen copy) is present on the sg4 leader | `postcheck.py` |

**What the history records:** `LinHistoryClient` runs 32 threads on ycsb0 doing 45% GET /
45% SET / 10% DEL on 200 keys that hash into the migrating slots, through the normal client
routing, for the whole run (before the flip, during transfer/merge, through the crash and
recovery, after NARROW), then reads every key once more. Every SET writes a unique value.
A write that times out is recorded as **unknown** (the checker lets it take effect at any
later time, or never) and is never re-sent.

**Not covered:** only the 200 test keys are checked for linearizability (the bulk data only
for presence and replica agreement); INCR/APPEND are off by default (`LIN_CTR_KEYS`); only
the single-node faults S1–S5 (no double failures).

## Files

| path | what |
|---|---|
| `ansible/lincheck/run_lin_scenario.sh` | one scenario: reshard (+ kill) with the history client, then `lincheck` + `postcheck.py` |
| `ansible/lincheck/run_all_lin.sh` | several scenarios in a row; appends one line per run to `summary.txt` |
| `ansible/lincheck/postcheck.py` | migration / crashes / replicas / donors / bulk checks (works on a live cluster) |
| `lincheck/` (`main.go`, binary `lincheck`, `testdata/`) | the history checker |
| `ycsb_client/redis/src/main/java/site/ycsb/db/LinHistoryClient.java` | the history recorder (ships in `redis-binding.jar`) |
| `ycsb_client/workloads/workloada_lin_quick` | the quick workload (300k keys, 90 s) |

## Prerequisites (once, and after the relevant code changes)

1. **Cluster free.** Nothing else may be running ansible or `crash_inject.sh`. The driver
   waits for other playbooks/injectors to finish and holds `/tmp/cluster_in_use` while it runs.
2. **Redis built from the current tree on all hosts** — as for any experiment:
   ```bash
   cd /users/entall/rd
   for h in redis1 redis2 redis3 redis4 redis5; do
     sudo rsync -a --exclude '*.o' --exclude '*.d' --exclude redis-server --exclude redis-cli --exclude '*.a' \
       redis/src/ root@$h:/users/entall/rd/redis/src/
   done
   cd ansible
   sudo ansible-playbook -i inventory.ini tasks/teardown/kill_processes.yml
   sudo ansible-playbook -i inventory.ini tasks/build/build_redis_custom.yml -e redis_variant=custom
   ```
   The sg4 instances start with `--rdma-tombstones yes` (set in `start_redisraft_instances.yml`);
   the harness relies on it.
3. **YCSB client with `LinHistoryClient`** on the YCSB hosts (redo after changing anything
   under `ycsb_client/`):
   ```bash
   cd /users/entall/rd/ansible && sudo ansible-playbook -i inventory.ini tasks/build/build_ycsb.yml
   ```
4. **Quick workload on the YCSB hosts** (once):
   ```bash
   for c in ycsb0 ycsb1; do
     sudo scp /users/entall/rd/ycsb_client/workloads/workloada_lin_quick root@$c:/users/entall/rd/workloads/
   done
   ```
5. **Checker binary** `lincheck/lincheck` exists. To rebuild it (Go 1.23 in `~/go-sdk`; the
   repo is root-owned, so build in a temp copy):
   ```bash
   t=$(mktemp -d); cp /users/entall/rd/lincheck/{main.go,go.mod,go.sum} $t/
   (cd $t && PATH=$HOME/go-sdk/bin:$PATH go build -o lincheck .)
   sudo install -m 755 $t/lincheck /users/entall/rd/lincheck/lincheck
   ```
   Self-test (expected: `valid` and `unknown_ok` PASS, the rest FAIL):
   ```bash
   cd /users/entall/rd/lincheck
   for f in testdata/*.jsonl; do printf '%-14s ' $(basename $f .jsonl); ./lincheck -history $f -out /tmp/lc_selftest | grep VERDICT; done
   ```

## Run

One scenario (about 6 min with the quick profile):
```bash
cd /users/entall/rd/ansible/lincheck
./run_lin_scenario.sh S2          # HEALTHY | S1 | S2 | S3 | S4 | S5
```

The full set (no-fault + S1–S5, about 35 min):
```bash
./run_all_lin.sh HEALTHY S1 S2 S3 S4 S5      # no arguments = the same list
```
Run it detached if your session may end: `nohup ./run_all_lin.sh > /dev/null 2>&1 &`.

### Options (environment variables)

| variable | default | meaning |
|---|---|---|
| `PROFILE` | `quick` | `quick`: 300k keys loaded fresh, 90 s YCSB run, 32 YCSB threads, 20 s pre-pause. `full`: 30M snapshot restore (`ws30m`), 3-min run, 100 threads, 60 s pre-pause |
| `QUORUM_READS` | `yes` | `--raft.quorum-reads`. With `no` a deposed leader may serve stale reads, which the checker will report |
| `QUORUM_READS_LIST` | `yes` | (`run_all_lin.sh`) e.g. `"yes no"` runs every scenario with both |
| `REPS` | `1` | (`run_all_lin.sh`) repetitions of the whole list |
| `LIN_KEYS` / `LIN_THREADS` / `LIN_RATE` | 200 / 32 / 50 | test keys, client threads, ops/s per thread |
| `LIN_DEL_PCT` | 10 | share of DELs (the rest: 45% GET, SETs) |
| `LIN_CTR_KEYS` / `LIN_INCR_PCT` | 0 / 0 | add INCR counters (checked with a counter model) |
| `WORKLOAD`, `YCSB_THREADS`, `PRE_PAUSE`, `EXTRA_ANSIBLE_ARGS`, `ROUNDS` | from `PROFILE` | override the background YCSB load / playbook |
| `OUT_ROOT` | `/tmp/lincheck/runs` | where results go |

## Results

Each run writes `$OUT_ROOT/<scenario>_q<yes|no>_<UTC timestamp>/`:

| file | content |
|---|---|
| `run.log` | timeline, the injector's `MARKER HIT` / `KILLED` lines, checker summaries, **`OVERALL: PASS/FAIL`** |
| `history.jsonl` | the recorded history (one op per line) |
| `lincheck.txt` | op counts by type/outcome, linearizable / violating / lost-write keys, `VERDICT` |
| `check/` | `lincheck_result.json` (per key) and `viz_<key>.html` for violating keys — open in a browser to see the conflicting ops |
| `postcheck.txt` | one `POSTCHECK <check> PASS/FAIL <detail>` line per check |
| `playbook.log`, `client.log` | ansible / history-client output |
| `logs/<host>/` | every node's redis log, copied during the run (the playbook deletes them on the hosts at the end) |

`summary.txt` gets one line per run:
```
2026-10-01T13:21:47Z S2 q=yes rc=0 OVERALL: PASS | linearizable keys=200  VIOLATING keys=0 ... lost-write keys=0 | migration=PASS crashes=PASS replicas=PASS donors=PASS bulk=PASS  <dir>
```
`rc` is the run's exit code: 0 PASS, 1 FAIL, 3 inconclusive (a key's check timed out).

**Check that the fault really happened**: `grep KILLED <dir>/run.log`. (The injector's own
record under `/tmp/crash_inject/` is root-owned and is not updated by these runs.)

### Reading a failure

- **VIOLATING keys** — open `check/viz_<key>.html`. To see whether only DEL reply counts
  are wrong: `lincheck -history <dir>/history.jsonl -out /tmp/x -ignore-del-count`.
- **lost-write keys** — `lincheck.txt` names the final value and the acknowledged write it
  should have been.
- **replicas FAIL** — `postcheck.txt` lists missing/extra keys per replica and, for some of
  them, which sg4 and donor replicas hold them; the full list is in
  `/tmp/lincheck/last_divergent_keys.txt`.
- **migration FAIL** — see the reshard task output in `playbook.log` (`RE-DRIVE INCOMPLETE`,
  `FINAL=…`) and the sg4 logs in `logs/`.

### Re-running only the checks

```bash
/users/entall/rd/lincheck/lincheck -history <dir>/history.jsonl -out /tmp/recheck
python3 /users/entall/rd/ansible/lincheck/postcheck.py --playbook-rc 0   # against the cluster as it is now
```
`postcheck.py` reads the live cluster, so it only describes a run while that run's cluster
is still up (i.e. before the next run starts).

## Aborting a run

Kill the run scripts by PID, then make sure nothing from the run survives — a leftover
injector kills a node in the next run, and the reshard's bash loop keeps re-driving
migrations into the next cluster:
```bash
for p in $(ps -eo pid=,args= | awk '/run_all_lin|run_lin_scenario/ && !/awk/ {print $1}'); do sudo kill $p; done
ps -eo pid,args | awk '/crash_inject\.sh|PS=1365; NR=/ && !/awk/'                 # kill these PIDs too
ps -eo pid,args | awk '($2 ~ /ansible-playbook$/ || $3 ~ /ansible-playbook$/) && !/awk/'   # and these
sudo ssh ycsb0 "pkill -f '[L]inHistoryClient'"
rm -f /tmp/cluster_in_use
```
Do not use `pkill -f <pattern>` from a shell whose own command line contains the pattern.

## Notes

- The quick profile does a fresh 300k-key load (no snapshot). `PROFILE=full` restores the
  30M snapshot; it takes ~20 min per scenario.
- The server-side pieces this check relies on (delete tombstones on sg4, the merged/tombstoned
  flags in the GET reply, TRYAGAIN for DEL/read-modify-write before a slot is merged) are part
  of the protocol and are always on; only the history client and the checks are optional.
- History of the fixes this harness found, with the backup taken before each: `ansible/lincheck/FIXES_LOG.md` (copy of the working log `/tmp/lincheck/CHANGES.md`).
