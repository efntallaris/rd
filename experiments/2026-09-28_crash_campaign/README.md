# Crash campaign — AqRaft reshard under node failures (2026-09-28)

**Question:** with the current recovery code, does the 30M reshard survive a single node crash at
each critical point without losing data or faking durability, and how fast does throughput
recover compared with a clean run?

## Goals

1. **Reference run (HEALTHY).** Clean reshard, no crash. Gives the baseline plateau, migration
   window and phase Gantt the crash runs are compared against.
2. **Every single-node crash is survived** (S1–S5): reshard rc=0, 0 crash signatures, recipient
   DBSIZE = 7,492,752, YCSB UPDATE errors ≈ 0, post-migration plateau above the pre-migration baseline.
3. **Durability is never faked.** `firing MGN_INDX_UPD` (timeout or fallback) = 0 in every run;
   `MGN_INDX_UPD` fires only after a real `chain-ack observed`.
4. **First cluster run of the query-first resume** (`RDMA MGN-RESUME-STATUS`, commit `d8e946f9`).
   It had been built but never run. Look for `AqRaft S2 resume-plan:` on the new donor leader
   (S2) and `AqRaft MGN-RESUME-STATUS:` on the sg4 leader (S1, S2).
5. **Figures:** per-scenario throughput timeline, YCSB throughput/latency, and phase Gantt for
   every run.

## Setup

- 30M keys, `workloada_prod_30m_run10min` (50/50 read/update), 2 YCSB clients × 100 threads,
  60 s steady state before the reshard.
- Donors sg1/sg2/sg3 (leaders redis0/1/2, each 3-way replicated), recipient sg4 (redis3 → redis4 →
  redis5 chain). One round, 4,095 slots (1,365 per donor).
- Flags: `rdma_chain_pipeline=yes rdma_chain_xsession=yes rdma_async_apply=yes
  rdma_transfer_chunk_slots=342 rdma_naive_durability=no`.
- Branch `aqueduct_broken`, commit `3b4c3099`, freshly built on all hosts on 2026-09-28.
  The first attempt (22:58 UTC) was aborted: the fresh build regenerated `redis/src/commands.def`
  from `commands/*.json` and dropped six AqRaft RDMA subcommands with no JSON file (`chain-warm`,
  `chain-status`, `mgn-recover`, `mgn-donor-rehome`, `mgn-resume-status`, `evict-slots`), so HEALTHY
  failed at `RDMA CHAIN-WARM` and no recovery path could have run. Rebuilt with the committed
  `commands.def`; `ansible/tasks/build/build_redis_custom.yml` now keeps it from being regenerated.
- Kills are injected by `ansible/crash/crash_inject.sh`, armed on a log marker
  (`ansible/crash/scenarios.env`), `kill -9` on the target's pidfile.

| run | kill | expected recovery |
|-----|------|-------------------|
| HEALTHY (`campaign_heal`) | none | reference |
| S1 (`crash_s1`) | recipient leader redis3, chunk 6/12 | promoted sg4 leader re-homes; donor re-ships to it |
| S2 (`crash_s2`) | donor leader redis0, at TRANSFER | new sg1 leader resumes the session |
| S3 (`crash_s3`) | donor follower (sg1 on redis1) | none needed, majority holds |
| S4 (`crash_s4`) | recipient follower redis4 | chain re-forms to redis5 |
| S5 (`crash_s5`) | donor leader redis0 after its DONE | handover only |

## Results

Campaign ran 2026-09-28 23:51 → 2026-09-29 03:20 UTC. Per-run counters, `metrics.json` and the
tail of each run log are in `RESULTS_AUTO.md`.

| run | reshard | sessions committed on sg4 | faked INDX_UPD | crash sigs | UPDATE errors | plateau vs pre | time to full tput | verdict |
|-----|---------|---------------------------|----------------|------------|---------------|----------------|-------------------|---------|
| HEALTHY | rc=0 | 3/3 | 0 | 0 | 0 | +24.5% | 27 s | pass |
| S1 recipient leader | **rc=2** | 2/3 (sg1; sg2 recovered on redis5) | 0 | 0 | n/a (YCSB log ends at 440 s) | +24.4% | 118 s | **fail: sg3 lost** |
| S2 donor leader | rc=0 | 1/3 (sg1, recovered) | 0 | 0 | 0 | +16.0% | 44 s | **partial: sg2/sg3 never dispatched** |
| S3 donor follower | rc=0 | 3/3 | 0 | 0 | 0 | +21.8% | 58 s | pass |
| S4 recipient follower | rc=0 | 3/3 (chain re-formed to redis5) | 0 | 0 | 0 | +21.3% | 20 s | pass |
| S5 donor leader after DONE | rc=0 | 2/3 (sg1, sg2) | 0 | 0 | 0 | +21.3% | 70 s | **partial: sg3 never dispatched** |

Against the goals:

1. **Reference run:** done. Migration 4.46 s end to end (Gantt), plateau +24.5%.
2. **Every single-node crash survived:** no. S3 and S4 pass fully. S1 fails, and S2/S5 exit 0 but
   move only part of the keyspace. The data that did move is complete and durable in every run,
   and no run showed a crash signature or UPDATE errors.
3. **Durability never faked:** yes. `firing MGN_INDX_UPD` (timeout or fallback) = 0 in all six
   runs; every committed `MGN_INDX_UPD` followed a real CHAIN-ACK.
4. **Query-first resume on the cluster:** it ran and answered correctly in both places it
   applies: S1 re-home (`MGN-RESUME-STATUS` on redis5: missing=1365) and S2 (`resume-plan` on the
   new sg1 leader: missing=1365, re-shipped all). Both crashes were mid-transfer, so the "skip
   durable slots" path is still unexercised.
5. **Figures:** all runs have a throughput timeline (`campaign_<S>.png`), YCSB and full-run plots.
   Gantt charts exist for HEALTHY, S1 (sg1 only), S3, S4, S5. S2 has none: its recipient-leader log
   was wiped before it could be saved.

**The two gaps found:**

- **The orchestrator is a single point of failure.** It runs inside the sg1 leader process on
  redis0 and dispatches the donors one after another. When redis0 dies (S2 during sg1's transfer,
  S5 right after it), the per-session recovery works, but the donors it had not yet dispatched are
  never migrated, and the playbook still reports success. S2 and S5 should be read as "partial".
- **A donor dispatched just before the recipient leader dies is lost (S1).** The promoted sg4
  leader only recovers sessions that already have a `RECP_TXN_START` in its log. sg3 was dispatched
  240 ms before the kill and had not registered yet, so nothing re-homed it.

Both point to the fix already proposed for the double-leader crash: store the sg4 member list in
`TXN_START` and let the donor (and the orchestration) find the current leader itself, with
retries, instead of depending on one process that can die.

### Findings recorded during the run

**HEALTHY — pass.** Playbook failed=0 on every host. sg4 leader: 3 × `chain-ack observed`,
0 × `firing MGN_INDX_UPD` (no faked durability), 3 × `MGN_INDX_UPD applied`, 3 ×
`MGN_RECP_TXN_DONE applied`; `MGN_TXN_DONE` applied on all 9 donor replicas; 0 crash signatures.

**S1 — reshard failed (rc=2), recovery gap found.** redis3 was killed at 18:52:32.16 (local
time in the logs). What happened:

- sg1's session had already finished (DONE 18:52:30.66).
- sg2's session was in flight. redis5 won the election at 18:52:34, found it
  (`RECP_TXN_START` with no DONE), asked `MGN-RESUME-STATUS` (first cluster run of the query-first
  resume: `durable=0 pending=0 missing=1365`, as expected mid-transfer), took the re-shipped slots,
  re-formed the chain around the dead redis3, got a real CHAIN-ACK, and committed `MGN_INDX_UPD` +
  `MGN_RECP_TXN_DONE` at 18:52:57 (applied=2,472,472). This part works.
- **sg3 was lost.** The orchestrator dispatched sg3 to redis3 at 18:52:31.92, 240 ms before the
  kill, and sg3 never logged `RECP_TXN_START` on sg4. The promoted leader only recovers sessions it
  has a `RECP_TXN_START` for, so nobody re-homed sg3; the orchestrator saw 1/3 donors terminal and
  ended `FINAL=TIMEOUT`. The playbook's drain check then waited 300 s for the sg4 leader's DBSIZE
  to hold still for 12 s and gave up. (Likely because sg3's writes were already redirected to sg4
  and kept adding keys; not confirmed, since the sg3 donor log was wiped by the next run.)
- Why the kill landed there: with `rdma_transfer_chunk_slots=342` each donor sends 4 chunks, not
  12, so the S1 arm marker (6th `DONE-SLOTS-CHUNK`) fires during sg2's transfer, right as sg3 is
  dispatched, not halfway through sg1 as `scenarios.env` assumes.
- Fix direction (same as the double-crash item in `FAULT_TOLERANCE_STATUS.md`): let the donor
  find the new recipient leader itself (sg4 member list in `TXN_START`, retry with backoff) instead
  of relying on the promoted leader to know every session.
- Missing evidence: the campaign script only keeps sg1 and sg4 logs, and a failed playbook skips
  the base collection, so S1's sg2/sg3 donor logs are gone. From S2 on, `grab_logs.sh` saves every
  host's full logs right after each playbook exits.

**S2 — reshard rc=0; sg1's session recovered; sg2/sg3 were not migrated.** redis0 (sg1 leader,
also the orchestrator) was killed at 19:24:39.80. The new sg1 leader (redis1) found `sess=1` in
flight at 19:24:41.10, re-dispatched it as `800000000000000001`, and ran the query-first resume
(`durable=0 pending=0 missing=1365`, re-shipping all 1365 slots: correct for a crash at TRANSFER).
sg4 committed `MGN_INDX_UPD` + `MGN_RECP_TXN_DONE` at 19:24:54 on both followers: applied=612,520
plus clobber_skipped=1,887,305 (keys the original session had already merged), i.e. all ~2.5M keys
of sg1's range. YCSB: ~81K ops/s per client, UPDATE-err=0, READ-err=1 per client.

- **sg2 and sg3 never started:** the orchestrator lived in the killed redis0 process, so nobody
  dispatched them. The playbook still ran NARROW + reconcile ("OK") and exited 0. rc=0 here
  means "sg1's range moved", not "the full reshard completed".
- Payload bug: the recovery's `INDX_UPD`/`RECP_TXN_DONE` say `slots=0-682` while `n_slots=1365`;
  the slot range stamped on the entry comes out as half the batch.
- Harness bug: the injector snapshots the log of the host it arms on (redis0_sg1 for S2), and
  `run_all_scenarios.sh` saves that snapshot as `redis3_sg4.log`. The real redis3 log for S2 came
  back empty, so recipient numbers come from the followers (redis4/redis5), which apply the same
  entries.

**S3 — pass (control).** sg1's replica on redis1 was killed mid-transfer. Reshard rc=0; all 3
sessions committed `MGN_INDX_UPD` + `MGN_RECP_TXN_DONE` on both sg4 followers; YCSB ~85K ops/s per
client, UPDATE-err=0, READ-err=1 per client. The collected redis3 log was empty again (wiped at end
of run), so from S4 on `grab_logs.sh` copies every host's logs every 15 s during the run.

**S4 — pass.** redis4 (F1) was killed while the leader was forwarding to it. The forward failed at
once ("Connection reset by peer"); the leader dropped redis4 and promoted redis5 as direct
successor (9 re-form lines: one per chain session and retry), re-forwarded, and waited for
redis5's real CHAIN-ACK before each `MGN_INDX_UPD`. All 3 sessions committed on redis5. Migration
took 14.13 s instead of 4.46 s (sg2's commit alone 9.6 s). YCSB ~84K ops/s per client, 0 errors.

**S5 — rc=0, partial.** redis0 was killed right after sg1's DONE. sg1 and sg2 committed on both
followers; sg3 has no `RECP_TXN_START` at all. Same cause as S2: the orchestrator died with
redis0 before dispatching sg3. YCSB ~87K ops/s per client, UPDATE-err=0.

## Fix validation (2026-09-29)

Fixes under test: bounded `REGISTER-RESULT` wait on the donor (fails the migration when the
recipient stops answering) and the playbook re-drive (after a round that does not end DONE,
re-drive every donor range that the current sg4 leader does not hold durably, via
`MGN-RECOVER donor` from that donor's current leader). Logs: `/tmp/crash_inject/fix_campaign/`.

**S1 (run 1) — sg3 now recovered; sg1 blocked by a port collision.** The re-drive worked for
the donor S1 used to lose: sg3 was re-sent to the promoted leader redis5 and became durable;
sg2 was recovered by the re-home path as before. sg1 had finished before the crash, but redis5
reports its range missing (its batch table is in-memory and empty after promotion), so it was
re-driven, and every attempt failed at once: `INIT-SERVER on 10.10.1.5:8000 failed:
rdmamig_server_create failed`. The recipient opens one listener per donor on the donor's
rdma port (17777 for sg1), and redis5, as a former chain follower, already had its chain
listener bound on 17777. Fix: chain followers now listen on rdma-migration-port + 1000
(the leader learns the port from `CHAIN-INIT-QP`). S1 is re-run with that fix after the
current batch.

**S2 (fix run) — pass.** redis0 killed at TRANSFER. The new sg1 leader resumed sg1's session
(`8e17+1`, re-shipped 1365 slots); the playbook re-drive dispatched sg2 (token 702) 1.6 s later and
sg3 (token 703) 4.6 s later, both to the live sg4 leader redis3. All three ranges committed
`MGN_INDX_UPD` on both followers (sg1 610,631 keys applied on top of the original session's
merge; sg2 2,474,265; sg3 2,496,368). rc=0, UPDATE-err=0 on both clients, ~83K ops/s per client.
Last night only sg1's range moved.

**S5 (fix run) — pass.** redis0 killed after its DONE. sg1 and sg2 committed normally; the
playbook re-drive dispatched sg3 (token 703) from its leader to the live sg4 leader, and it
committed on both followers. rc=0, UPDATE-err=0, ~83K ops/s per client. Last night sg3 never
started. (The `applied=` value in an `INDX_UPD` entry counts keys merged at commit time; with
async-apply the rest of the merge drains afterwards, so it is not the range total.)

**S1 (run 2, with the chain-port fix) — still fails, next cause found and fixed.** The port fix
worked: all three ranges, sg1 included, were re-driven to the promoted leader redis5 and fully
landed. But none became durable. On redis5, chain setup for each re-driven session found the dead
old leader redis3 unreachable and "excluded" it, yet left it in `peers[0]`, which the forward path
and the off-main source-pool pre-registration both use as the chain head. The pre-registration
therefore failed ("chain not established") and fell back to registering 2.86 GB on the main
thread; redis5 went silent for ~1.5 s, lost quorum (check-quorum step-down at 10:55:37.96),
and the chain forwards stalled and aborted. Fix: after wiring, chain setup compacts the live
followers to the front of `peers[]` (cluster_rdma_chain.c). Not yet re-run.

**Dataset snapshot restore — OOM found and fixed.** The first restore brought sg2/sg4 back but
sg1/sg3 never came up: the kernel OOM-killed them. Loading the RDB called `dbExpand(db, 10M)`
for the RESIZEDB hint, and in this fork the keyspace has one dict per slot even with cluster
mode off, so every per-slot dict was pre-sized to the full key count (sg2 alone: 686 GB
allocated, 40 GB resident). Fix in `rdb.c`: skip the whole-db hint when the keyspace has more
than one dict. After the fix a full restore (clean -> files in place -> start -> all groups up
with DBSIZE == manifest) takes 81 s instead of the 22-min YCSB load.

**S1 (run 3) — pass.** With the chain-port fix, the establish-time compaction of dead chain
followers and the re-drive, all three donor ranges committed `MGN_INDX_UPD` on both surviving
sg4 replicas (redis4, redis5): sg1 had committed before the crash (and was re-shipped once more
by the re-drive, harmless: existing keys are skipped), sg2 via the promoted leader's re-home
(`8e17+5461`), sg3 via the re-drive (second attempt, token 713). rc=0, UPDATE-err=0,
~85K ops/s per client. Run with the split two-client load and `INDX_UPD` after merge.

**Dataset snapshot — not usable for migration runs yet.** After the RESIZEDB fix the restore is
correct for the keyspace (81 s, DBSIZE == manifest), but a HEALTHY run from it merged only
455,575 keys on sg4 versus 7,447,483 after a YCSB load: the donors ship the same 1365 x 2 MB
blocks, but restored keys are not in those r_allocator blocks (each holds ~110 keys, the ones
YCSB rewrote during the run, instead of ~1,800). Routing `dbAddRDBLoad` through
`r_allocator_insert_kvobj` did not change it (likely the allocator is not initialized when the
RDB loads, so every insert falls back). Open.

**Split YCSB load.** Both clients load half the key range each (`load_ycsb.yml`), 100 threads
each: 28K inserts/s combined vs 22K with one client, so ~18 min instead of ~22. The limit is
the insert path on the servers, not the clients.

**AppendEntries piggyback (HEALTHY, S4) — pass.** With `rdma_chain_ack_via_raft=yes`, followers
report received batches on their RAFT.AE replies and the leader turns them into CHAIN-ACKs.
HEALTHY: 6 piggybacked acks (2 followers x 3 sessions); the first arrived 1-2 ms after the chain
forward finished, the same as the TCP ack. S4 (redis4 killed): 3 chain re-forms, redis5's acks
arrived via AppendEntries, 3 real acks, 0 faked, all three ranges committed with full merge
counts (~2.47-2.50M each: `INDX_UPD` is now logged after the leader merge). rc=0, UPDATE-err=0.

**Dataset snapshot — fixed.** The restored keys were missing from the migration because
`r_allocator_init()` ran after `loadDataFromDisk()` and reset every slot's block list, orphaning
the blocks RDB-loaded keys lived in. It now runs before the load (server.c), and `dbAddRDBLoad`
places string values in the r_allocator like live writes (db.c). A HEALTHY run restored from
`ws30m` then merged 2,492,421 / 2,481,831 / 2,495,172 keys per range (a YCSB-loaded run: ~7.45M
total), rc=0. Restore takes ~80 s instead of the ~18-min load.

**Follower crashes and client throughput (2026-09-29 evening).** Question: S3/S4 kill only a
follower, the system keeps progressing, so why did clients dip? Four runs restored from `ws30m`
(with leadership moved back to the usual nodes after the restore) and a new per-follower
AppendEntries round-trip log on every Raft leader (`AE-RTT node= n= avg= max=`, once per second).

| run | kill | client effect | surviving follower's AE round trip |
|-----|------|---------------|-------------------------------------|
| S4-quiet | redis4 at YCSB 25 s, before the migration | none (147-153K) | flat ~0.35 ms |
| S3-quiet | sg1 on redis1 at YCSB 25 s | none (157-165K) | 0.4 -> 0.6 ms |
| S4 | redis4 during the migration | 75K, then ~2 s with no samples, then 157K at 8.5 ms | avg 0.2 -> 2.4 ms, max 184 / 494 ms |
| S3 | sg1 on redis1 during the migration | none (163-187K) | flat ~0.3 ms |

- Losing a follower costs nothing by itself (both quiet runs, and S3 during the migration).
  Last night's S3 dip did not reproduce, so it was not caused by the follower loss.
- S4 during the migration is real: redis5 becomes sg4's only follower, so every sg4 commit
  (including writes to slots already redirected to sg4) waits for it, and at that moment its
  main thread stalls for ~0.5 s after each new session's `CHAIN-PREP` (15:22:21.008 and 21.561,
  then a burst of queued commands at 21.572). Likely cause: its landing pools must be registered
  for the new predecessor (the leader instead of redis4), a 2.86 GB `ibv_reg_mr` on the main
  thread. Fix direction: do follower-side pool registration off the main thread, as CHAIN-WARM
  already does for the first chain.
- Restores from a snapshot elect leaders at random; `restore_wait.yml` now moves each group's
  leadership back to raft id 1 (`RAFT.TRANSFER_LEADER`), since the orchestrator and the crash
  scenarios assume the usual placement.

**Recipient landing ring moved out of the migration.** In the S3 Gantt the first donor's
CONNECT took ~6.9 s and sg3's ~0.87 s. Causes: (1) the recipient's first REGISTER-BLOCK-SLOTS
created the whole 8-pool landing ring (8 x 3.29 GB mmap + ibv_reg_mr); (2) the ring did not
exist at CHAIN-WARM, so the chain's forward twins were registered during the migration while
holding the ring lock, which a later donor's registration waited on; and the claim code
re-created every empty ring slot, including one retired by adopt-in-place, on each registration.
Fixes: `--rdma-landing-prereg-pools N --rdma-landing-prereg-slots S` register N landing pools at
startup (off the main thread, device shared PD via the keeper cm_id), so CHAIN-WARM registers
their twins before the window; and a registration now claims a usable free pool and creates one
only when none is free. Result (HEALTHY from `ws30m`, `rdma_landing_prereg_pools=3`): all three
donors' registrations answered from a pre-registered pool in ~1 ms (was 6.9 s / 0.08 s /
0.87 s); first donor PREP to last commit 5.5 s (was ~13.9 s); no landing pool registered during
the migration. Figure: `figures/follower_crash/landing_heal_gantt.png`.

## Layout

- `run.sh` — runs the campaign on the controller (wraps `ansible/crash/run_all_scenarios.sh`).
- `grab_logs.sh` — side job: saves every host's `/tmp/redis_logs` after each scenario's playbook
  exits, before the next scenario wipes them.
- `collect.sh` — copies the raw output into `logs/`, runs `figures.sh`, writes `RESULTS_AUTO.md`
  (`--wait` blocks until the campaign is done).
- `figures.sh` — rebuilds `figures/` from `logs/`.
- `logs/` is not committed (~1.7 GB, `.gitignore`); it stays on the controller at
  `redis0:/users/entall/rd/experiments/2026-09-28_crash_campaign/logs/`.
- `logs/campaign/` — `/tmp/crash_inject/campaign`: per-scenario YCSB output, recipient-leader log
  (snapshot for S1), sg4 follower and sg1 logs, run logs, injection records.
- `logs/<run>/` — `/tmp/experiments/<run>`: full collected run (`ycsb/`, `logs/<host>/`).
- `figures/`
  - `campaign_<S>.png` — throughput timeline with kill and recovery marks
  - `<run>_ycsb.png` — throughput + latency, migration band
  - `<run>_gantt.png` — per-phase migration Gantt (donor PREP/REGISTER/FLIP/TRANSFER, recipient
    MERGE/CHAIN-FWD/COMMIT)
  - `<run>_full_run.png` — throughput, latency, CPU, NIC traffic
  - `metrics.json`, `crash_artifact.html`

Design and per-scenario semantics: `ansible/experiments/custom_reshard_v2_orch_raft_chunked/CRASH_SCENARIOS.md`,
`FAULT_TOLERANCE_STATUS.md`. Previous campaign: `experiments/2026-07_crash_campaign/`.
