# AqRaft Fault Tolerance — Status, Roadmap & How to Run (updated 2026-09-28)

Fault-tolerance work layered on the validated 30M background-merge reshard
(`HOWTO_30M_BGMERGE.md`). Design + per-scenario semantics: `CRASH_SCENARIOS.md`.
Run guide: `HOWTO_CRASH.md`. Harness: `ansible/crash/`. Branch `aqueduct_broken`.

---

## 0. Current status (2026-09-28)

| # | scenario (kill) | status | key result | commits |
|---|-----------------|--------|------------|---------|
| S1 | recipient **leader** redis3, chunk 6/12 | ✅ recovered | promoted leader 43K → 5.0M keys (30M), no loss; full throughput in ~38 s (3M, was 68 s), +28% plateau, UPDATE-err=0 | `2330de0a` `9b08cb78` `446fc348` `37b37ad6` `7d60b69a` `56054dcf` `d9beb430` |
| S2 | donor **leader** redis0, at TRANSFER | ✅ recovered | resume completes with real durability; rise +27% (was +8%), time-to-plateau 36 s (was 90 s), rc=0 (3M) | `8c960594` `31606286` `c0513617` `b8ca5025` `13b2ccd9` `e848e961` `33564e41` |
| S3 | donor **follower** redis1 | ✅ PASS (control) | migration unaffected; client stall removed (400 ms probes + dead-endpoint marks) | `e45d2df7` |
| S4 | recipient **follower** redis4 | ✅ PASS | chain re-form, real CHAIN-ACK, DBSIZE 7,492,752 exact, 0 errors; also survives death during chain establish | `557ee1dc` `1ccc7810` `5abd66f3` |
| S5 | donor **leader** redis0, after its DONE | ✅ added | post-transfer donor loss is a non-event: rc=0, no data loss, UPDATE-err=0 | `d53dfcd4` |

How recovery actually works (differs from the original §1a design):
- **S1 is donor-driven.** The promoted sg4 leader finds the in-flight session (RECP_TXN_START
  without RECP_TXN_DONE) and sends `RDMA MGN-DONOR-REHOME` to the donor; the donor detects the
  dead recipient in ~100 ms and re-ships the session's exact slots to the new leader, whose chain
  re-forms to the surviving follower. Peer-pull (increments 1–2: gap map + `CHAIN-STATUS`) only
  logs; survivors hold almost nothing of an in-flight batch because followers apply a batch only
  after its last chunk.
- **S2 resumes from the new donor leader.** The become-leader hook finds TXN_START without
  TXN_DONE and re-dispatches the exact slot range (`RDMA MGN-RECOVER`) under a recovery session id.
- **Orchestration** survives leader crashes: the playbook skips a dead orchestrator, waits for the
  recovery to drain on the live sg4 leader, then runs leader-aware NARROW + `reconcile_ownership.py`.
- **`mgn_executed_idx`** was not built as a Raft watermark; the node-local inventory's per-session
  `executed` flag (§1c-bis) plays that role.
- **EVICT is disabled** (`37c71861`): removes the ~8 s post-migration drop; donors keep migrated keys.

Uncommitted in the working tree (built locally, **not yet run on the cluster**):
- **Query-first resume:** before re-shipping, the donor asks the recipient
  `RDMA MGN-RESUME-STATUS <lo> <hi>` → per slot durable (merged + chain-acked + INDX_UPD
  committed) / pending (fully landed, still finishing → wait ≤ 60 s) / missing, and re-ships only
  non-durable slots. Used by S2 and (since today) the S1 re-home path. It also commits
  `TXN_DONE` for the original crashed session so a later promotion does not resume it again.
- For a mid-transfer crash the answer is "all missing" (followers hold nothing yet), so this
  only saves the re-send when the donor dies after its last chunk.

---

## 1. History: what was done (2026-07-08 → 07-14)

### 1a. Design (CRASH_SCENARIOS.md — complete)
Four crash scenarios with user-defined required behavior, all **roll-forward, never roll-back**
(status column as of 2026-07-09; see §0 for the current state):

| # | scenario | required behavior | status on 2026-07-09 |
|---|----------|-------------------|--------------|
| S1 | recipient **leader** (redis3) | on election: detect active migration → execute committed-but-unexecuted `INDX_UPD`s (third watermark `mgn_executed_idx`) → pull missing blocks from a surviving peer (`CHAIN-STATUS`) → re-form chain → donor hand-off | **designed, not implemented** — baseline GAP confirmed 2026-07-09 (redis5 elected leader, migration NOT resumed, DBSIZE 42k vs 7.49M). Reconciled build plan: `IMPL_PLAN_S1_S2.md` |
| S2 | donor **leader** (redis0/1/2) | on election: new donor leader RPCs the recipient for session status → resumes from remaining slots | **designed, not implemented** — baseline run 2026-07-09 (see NIGHT_SUMMARY.md §4). Build plan: `IMPL_PLAN_S1_S2.md` |
| S3 | donor **follower** | majority (2/3) holds → migration completes unaffected | **PASSES today (validated control)** |
| S4 | recipient **follower** (redis4/5) | predecessor detects death → **leader re-forms the chain** around the dead member → real majority ack → only then `INDX_UPD` | **✅ VERIFIED PASS (2026-07-09, run #3, script-verified)**: reform=1, chain-ack=3 (real durability), INDX_UPD=3, degrade=0, DBSIZE 7,492,752 exact, 0 crash/YCSB err. Fixed 3 harness bugs first (see §1d). |

**Core invariant (drives everything):** `INDX_UPD` committed ⟺ the session's data is
physically on a **majority** of the recipient group. Never fire it on a timeout or as a
fallback — wait/repair as long as needed; with no live majority, fail loudly.

### 1b. Key empirical findings (from real 30M runs)
- **The clean baseline violated the invariant**: every session fired
  `MGN_INDX_UPD anyway` on the 5s timeout — the tail's CHAIN-ACK arrives *after* 5s
  (follower merge of the 2.86 GB pool). Durability was faked on every validated run.
- **Two premature-fire paths**: 5s timeout (`anyway`, baseline) and synchronous
  forward-failure (`immediately as fallback`, fires when a chain follower dies).
- S4 baseline: killing redis4 mid-chain is **client-invisible** (exact DBSIZE, 0 UPDATE
  errors) but silently degrades durability — the killed follower (and downstream tail)
  miss the bytes. Gantt signature: migration 3.45s→6.08s, tail-hop row empty.

### 1c. Code landed (redis/src, deployed to all hosts)
- **Part A — real-ack gating** (commit `4eee2f4b`, **VERIFIED on a clean 30M run**):
  `chainPendingTick` never fires `INDX_UPD` on a deadline; waits for the genuine
  CHAIN-ACK. Verified: `firing anyway` 3→**0**, `chain-ack observed` 0→**3**, DBSIZE
  exact, throughput ~79k/client. Cost: +~0.9s migration-completion (honest durability).
- **Part B — chain re-form on dead member** (implemented, in verification):
  - `rdmaLeaderChainDropDeadHead()` (`cluster_rdma_chain.c`): drop dead head peers[0],
    promote the next live follower (leader already holds a QP + PREP'd pool to every
    follower — no new RDMA handshake needed).
  - Both forward-failure sites (`cluster_rdma.c`, pipelined + per-slot) now: re-form →
    re-forward to the survivor → await its REAL ack; if no live follower remains →
    leave batch un-finalized → donor poll fails the migration **loudly** (never fake).
  - Fixes found via testing: idempotent re-form for sibling batches sharing one session
    (`n_peers==1` → re-forward to survivor, don't refuse); NULL-peer crash guard
    (`sdsdup(NULL)` segfault when the dead follower died mid-establish).
- **Verification state of Part B: ✅ VERIFIED PASS (2026-07-09, run #3, script-verified).**
  Kill landed mid-forward (`poll_send for F1 failed after 105/1366 reaped`) → `RE-FORM — dropped
  dead head redis4 → promoted redis5` → re-forwarded → `chain-ack observed count=1,2,3 (real
  live-majority durability)` → `MGN_INDX_UPD applied` (=3, fired only after genuine ack) → migration
  DONE, DBSIZE 7,492,752 exact, degrade=0, fail-loud=0, crash-sig=0, YCSB err=0. It took 3 runs
  because runs #1/#2 exposed harness bugs (§1d) that were making verdicts LIE — those are fixed.
  Evidence: NIGHT_SUMMARY.md, S4_RERUN_2345_EVIDENCE.md, snapshot
  `/tmp/crash_inject/S4-recipient-follower_leaderlog.snap`. Figures `/tmp/plots/crash_s4_{gantt,ycsb}.png`.

### 1c-bis. Local bookkeeping WITHOUT Raft (2026-07-11) — foundation for S1/S2
User decision: bookkeep locally, never through Raft (node-local facts; consensus wrong + slow).
Implemented `rdmaSlotInventory` (cluster_rdma_chain.c): per-session received/merged bitmaps +
executed flag, apply-then-mark rule, hooks at follower chain-apply, leader DONE-SLOTS-CHUNK, and
the mergeBackpatchTick choke point. `RDMA CHAIN-STATUS <sess> <lo> <hi> [MERGED]` answers from it
(session-scoped + merge-aware; legacy allocator probe as fallback). Details: IMPL_PLAN_S1_S2.md §1c.

### ⚠️ FINDING (2026-07-11, exposed by the new inventory on its FIRST run): followers only
APPLY the FIRST donor batch. All donors number their own migrations (3 concurrent donors all
send sess=1); the follower per-session `applied` guard (cluster_rdma_chain.c:1260, Patch 29 —
meant for RETRIES of the same batch) misclassifies donor batches 2/3 as retries: evidence on
redis4+redis5: one `CHAIN apply: sess=1 n_slots=1365` then 2x `already applied — skipping
re-apply (retry)`. Verified with CHAIN-STATUS: leader holds 4095/4095 migrated slots, EACH
follower holds only donor #1's 1365. => the durability invariant is violated at the APPLY level
in EVERY run to date (acks confirm receipt into the pool, not adoption); a promoted follower is
missing ~2/3 of the migrated keyspace. LIKELY WORSE (needs a targeted read-check): the single
per-session landing pool means batches 2/3's RDMA-writes overwrite batch 1's bytes, which the
follower adopted IN-PLACE from that pool — probable corruption of the follower's batch-1 data.
FIX ✅ LANDED + VERIFIED (2026-07-11, run chainsess_verify): recipient-synthesized globally-unique
`chain_sess` per batch (7e17 namespace; backpatchBatch.chain_sess; all leader chain ops re-keyed;
followers unchanged — they key by the sess on the wire; inventory answers range-aggregate).
Verified: 3 unique sessions assigned; followers now log 3x `CHAIN apply` + 0x `already applied —
skipping`; follower inventory 1365/1365/1365 across all donor ranges (received AND merged);
3 real chain-acks; 0 sg4 elections (the 2 extra in-window CHAIN-PREP registrations did not
destabilize raft); 0 crash-sigs. Follower keyspace gap shrank 9,697 -> 8,016.

⚠️ RESIDUAL FINDING (open): ~8,016 keys (~3.2%) still missing on BOTH followers, and the missing
set is DETERMINISTIC (redis4=241,841 vs redis5=241,839 despite very different staged counts) =>
the same keys are absent everywhere. Suspect: the FOLLOWER-side block decoder
(rdmaBackpatchSlotFillShadow entry walker) systematically misses ~3.2% of entries that the
LEADER's different walker (rdmaBackpatchSlotWithStats / adopt-in-place) does adopt from the SAME
bytes. Needs: walker diff + a DONT_INTERCEPT debug read on a follower to sample the missing keys.

### 1d. Harness + docs (ansible/crash/, committed)
`crash_inject.sh` (marker-armed killer; fresh-log gate + alive-pid check),
`scenarios.env` (S1–S4 arm/target config), `run_crash_scenario.sh` (one-command:
arm → base 30M reshard → verdict), `verdict.sh` (crash sigs, leader changes, degrade
counts, DBSIZE, YCSB errors). **3 harness bugs fixed 2026-07-09 (verdicts were untrustworthy):**
(P1) `verdict.sh` read a **2h-STALE** collected redis3 leader log → false PASS; now fetches redis3
fresh + rejects empty. (P2) S4 kill armed on `landing F1-PD twins ensured` which fires for the WARM
pre-establish (sess=9e17), killing redis4 ~10s BEFORE the real forward → now arms on `spawned
pipelined forwarder`. (P3) recipient-LEADER log is WIPED at end-of-run → injector now snapshots it at
kill+120s to a durable path; verdict prefers the snapshot. Baselines: S3 PASS, S4 **PASS**, S1/S2 gap.

---

## 2. What NEEDS TO BE DONE

1. **Verify + commit the query-first resume** (§0, uncommitted): rebuild, run S1 and S2, check
   `AqRaft S2 resume-plan:` on the new donor leader and `AqRaft MGN-RESUME-STATUS:` on the sg4
   leader. Add a variant that kills redis0 on `state=BACKPATCH` to exercise the skip path.
2. **Double leader crash (donor + recipient) — HAZARD, analysis from code, untested.** Both
   recovery calls target the other side's dead old leader (`recipient=redis3` from TXN_START,
   `donor=redis0` from RECP_TXN_START), fail once, and are never retried; the session stays open
   on both sides. The playbook then runs NARROW for every donor without checking TXN_DONE, so
   reads of unwritten keys in that range go to sg4 and miss (keys survive only because EVICT is
   off). Fixes, in order:
   - gate NARROW (and EVICT) on the donor session's TXN_DONE — worst case becomes "unfinished";
   - donor-driven discovery: store the sg4 member list in TXN_START, try the next member when
     the recipient leader is dead, retry with backoff; this also replaces MGN-DONOR-REHOME.
3. **S1 durable set on the promoted leader** (design): redis4 derives the slots merged on a live
   majority (itself + `CHAIN-STATUS … MERGED` from redis5), commits INDX_UPD for them, and
   reports them durable to MGN-RESUME-STATUS. Small payoff today (only a narrow window between
   the chain ACK and INDX_UPD).
4. **Per-chunk durability** (design): CHAIN-FORWARDED + CHAIN-ACK + INDX_UPD per chunk instead of
   per batch, so a mid-transfer crash loses at most the chunk in flight and the resume query can
   skip committed chunks. Costs ~4× more Raft commits / ACK round trips per session.
5. **Residual follower key gap (open):** ~8,016 keys (~3.2%) missing on both followers,
   deterministic (§1c FINDING). Suspect the follower-side FillShadow entry walker.
6. **Async EVICT:** reclaim donor memory without the synchronous delete stall.
7. **Write redirect after S1:** donors' WRITE_FLIP still rotates `-MOVED` through the dead sg4
   node until reconcile; drop dead nodes at promotion time instead.
8. Optional: RESTART=yes variant (relaunch the killed node, verify Raft rejoin).

---

## 3. HOW TO RUN the fault-tolerance experiments

Everything as **root** from the controller (redis0), repo `/users/entall/rd`.

### 3a. One-command scenario run (~35 min each)
```bash
cd /users/entall/rd/ansible/crash
./run_crash_scenario.sh S3   # donor follower  (control — must PASS)
./run_crash_scenario.sh S4   # recipient follower (chain re-form test)
./run_crash_scenario.sh S1   # recipient leader (donor re-ship to the promoted leader)
./run_crash_scenario.sh S2   # donor leader     (resume from the new donor leader)
./run_crash_scenario.sh S5   # donor leader after its DONE (handover only)
# whole campaign (HEALTHY + S1..S5) + metrics/figures/HTML: see ansible/crash/HOWTO_CRASH.md
./run_all_scenarios.sh
```
Each: arms the killer (fires on the Nth marker in redis3's log, then `kill -9`s the
target's pidfile after verifying the pid is alive) → runs the exact HOWTO_30M_BGMERGE §1
reshard (`experiment_name=crash_<id>`) → prints the verdict. Injection record:
`/tmp/crash_inject/<label>.txt` (`result=KILLED|NO_MARKER|STALE_PID`).

### 3b. Read the verdict
```bash
./verdict.sh S4 crash_s4     # re-print anytime (read-only)
```
Key rows: crash signatures =0 · sg4 "Node is now a leader" · chain degrade
(`firing MGN_INDX_UPD` immediate/timeout — **must be 0 with Parts A+B**) ·
`chain-ack observed` >0 (real durability) · DBSIZE ==7,492,752 · YCSB UPDATE-err ≈0.
Full log greps: collected logs in `/tmp/experiments/crash_<id>/logs/...`;
sg4 followers (redis4/5) are fetched by the wrapper (base collection skips them).

### 3c. After changing C source (rebuild + redeploy)
```bash
cd /users/entall/rd
for h in redis1 redis2 redis3 redis4 redis5; do
  sudo rsync -e "ssh -o StrictHostKeyChecking=accept-new" -a --delete redis/src/ "$h":/users/entall/rd/redis/src/
done
cd ansible && sudo ansible-playbook -i inventory.ini tasks/build/build_redis_custom.yml
# then re-run the scenario (3a)
```

### 3d. Clean (no-crash) invariant check — Part A regression test
```bash
# run the base HOWTO_30M_BGMERGE §1 command with -e experiment_name=partA_clean, then:
R3=/tmp/experiments/partA_clean/logs/redis3/tmp/redis_logs/redis3_sg4.log
sudo grep -c 'firing MGN_INDX_UPD' $R3    # want 0  (no faked durability)
sudo grep -c 'chain-ack observed'  $R3    # want 3  (one real ack per session)
```

### 3e. Figures
```bash
cd /users/entall/rd
python3 experiments/tools/plot_phase_gantt.py      /tmp/experiments/crash_s4 /tmp/plots/crash_s4_gantt.png
python3 experiments/tools/plot_ycsb_timeseries.py  /tmp/experiments/crash_s4 --output /tmp/plots/crash_s4_ycsb.png
# reference figures: experiments/2026-07_crash_campaign/figures/
```
