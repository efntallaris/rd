# HOWTO — AqRaft 30M crash / fault-injection scenarios

Fault-injection wrapper around the validated 30M background-merge reshard
(`HOWTO_30M_BGMERGE.md`). Kills a chosen node **mid-migration** (armed off a
log marker) and reports what happened. Branch `aqueduct_broken`. Run from
`/users/entall/rd` on the controller (redis0). The design + expected
recovery semantics live in `CRASH_SCENARIOS.md` (same directory).

> **Phase A (this HOWTO): baseline against the CURRENT binary.** No source
> changes. The point is to establish the "before": **S3 should PASS today**
> (the control), while **S1/S2/S4 reproduce the documented gaps**. After the
> Phase-B source fixes land, re-run the exact same commands to show the "after".

---

## 0. The four scenarios

| id | crash | kill target | armed off (recipient log `redis3_sg4.log`) |
|----|-------|-------------|--------------------------------------------|
| **S1** | recipient **leader** | redis3 `/tmp/redis_redis3_sg4.pid` | 6th `DONE-SLOTS-CHUNK` (mid, ~chunk 6/12) |
| **S2** | donor **leader**     | redis0 `/tmp/redis_redis0_sg1.pid` | 3rd `DONE-SLOTS-CHUNK` |
| **S3** | donor **follower** (control) | redis1 `/tmp/redis_redis1_sg1.pid` | 3rd `DONE-SLOTS-CHUNK` |
| **S4** | recipient **follower** | redis4 `/tmp/redis_redis4_sg4.pid` | 1st `forward FIRST-POST` |

All scenarios arm off the recipient leader's log (it records every
`DONE-SLOTS-CHUNK` and the leader-side `forward FIRST-POST` markers). We fire
on the **Nth occurrence** (`grep -m<N>`), not a `seq=` regex — `seq` is
per-donor-session `0..3`, so there is no global "seq=6".

## 1. Prereqs
Same as `HOWTO_30M_BGMERGE.md` (built binary with the bgmerge defaults,
ycsb jars + prod workload deployed; see `aqraft-30m-run-prereqs`). Plus these
scripts under `ansible/crash/` (already executable). `sudo` is passwordless
here; `sudo ssh redisN ...` uses root's keys, matching the HOWTO convention.

## 2. Run one scenario
```bash
cd /users/entall/rd/ansible/crash
./run_crash_scenario.sh S3      # start with the control — it should PASS
./run_crash_scenario.sh S1      # recipient leader crash
./run_crash_scenario.sh S2      # donor leader crash
./run_crash_scenario.sh S4      # recipient follower crash
```
Each run: arms the backgrounded killer → runs the base 30M reshard
(`experiment_name=crash_<id>`, ~35 min) → kills the target mid-migration →
prints the verdict. Injection record: `/tmp/crash_inject/<label>.txt`.

Recovery-validation variant (restart the killed node after N s — Phase B):
```bash
RESTART=yes POST_KILL_DELAY=5 ./run_crash_scenario.sh S1
```
(Single-instance relaunch is a v2 stub in Phase A; baseline uses `RESTART=no`.)

## 3. Read the verdict
`verdict.sh` (auto-run, or `./verdict.sh S3 crash_s3`) collects, read-only:
- **crash signatures** (`ASSERTION FAILED|REDIS BUG|Crashed by signal|SIGSEGV|dict.c:548`) on redis3/4/5 + redis0/1 — want **0**.
- **sg4 leader changes** (`State change: Node is now a leader`) on redis4/redis5 — S1 expects ≥1 (a follower promoted).
- **chain degrade** (`firing MGN_INDX_UPD anyway`) on redis3 — S4 baseline expects **>0**.
- **recipient DBSIZE** (redis3:8000, ~7,492,752 clean) + bg-merge `moved/skipped`.
- **YCSB** last-block throughput + UPDATE/READ `Return=ERROR` counts.
- **migration** `TXN_DONE` markers.

### Expected per scenario (baseline, current binary)
- **S3 — PASS today:** migration DONE, DBSIZE ~7.49M, crash-sig 0, UPDATE-err ~0, no sg4 leader change. (Losing 1 donor follower keeps 2/3 quorum; the follower isn't in the transfer path.)
- **S1 — gap:** redis3 down; redis4/redis5 may show "now a leader" but the migration is **not resumed** → UPDATE errors + short DBSIZE. (Fixed by Phase-B #1.)
- **S2 — gap:** donor `RDMA_MIG_FAILED`; recipient batch stuck; orchestration FAILED. (Fixed by Phase-B #2.)
- **S4 — gap:** migration completes but `firing MGN_INDX_UPD anyway` > 0 (chain degraded to Raft-only; killed follower missing bytes). (Fixed by Phase-B #4.)

## 4. After a run
The base experiment leaves everything under `/tmp/experiments/crash_<id>/`
(logs, ycsb, monitoring) exactly like a normal HOWTO_30M_BGMERGE run — reuse
its §2/§3 verification greps and `plot_phase_gantt.py` if you want plots.

## 5. Phase B (source fixes) — see `CRASH_SCENARIOS.md`
Build order: **#4 durability gate** (never fire `INDX_UPD` on timeout; re-form
the chain to a live majority) → **#1** recipient roll-forward (third watermark
`mgn_executed_idx` + `CHAIN-STATUS` gap-pull) → **#2** donor roll-forward
resume. Re-run each scenario after its fix; the same verdict flips gap → PASS.
