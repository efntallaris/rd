# AqRaft S1 + S2 recovery — reconciled implementation plan (2026-07-08)

Grounded in a fresh read of the branch `aqueduct_broken` code (three code-maps:
`MAP_1_raft_wiring.md`, `MAP_2_recipient_recovery.md`, `MAP_3_donor_resume.md`).
This SUPERSEDES the line-numbers/assumptions in `CRASH_SCENARIOS.md` where they
disagree (all disagreements listed in §0). Nothing here is implemented yet unless
explicitly marked DONE — this is the build plan for S1 (recipient-leader) and S2
(donor-leader) roll-forward recovery.

## 0. Corrections to CRASH_SCENARIOS.md (verified against code)
1. **Part A already landed.** `CHAIN_PENDING_TIMEOUT_MS` (cluster_rdma.c:1666) is marked
   `(unused)`; `chainPendingTick` (:3078) finalizes ONLY on a real CHAIN-ACK (:3092-3112),
   no timeout fire. Doc's ":3042-3050 fire anyway" describes PRE-#4 binaries. => the current
   binary is the honest-durability one; S4 run is a valid #4 test.
2. **Part B (#4) landed.** `rdmaLeaderChainDropDeadHead` (cluster_rdma_chain.c:1663) re-form;
   forward-failure sites now FAIL-LOUD, never fake (cluster_rdma.c:2858-2867, 2963-2972).
3. **Two layers, NO shared symbols.** redisraft module (redisraft/src/*.c, includes only
   redismodule.h) vs cluster_rdma.c (redis-server binary). Session dicts (server.rdma_migrations,
   static backpatch_batches_by_key) are INVISIBLE to the module. Only bridge = loopback RESP cmd
   `RAFT.MGN-LOG` (cluster_rdma→module). => become-leader recovery needs a NEW REVERSE loopback cmd.
4. `rdmaApplySlotBlock` is DEAD code; real follower install = `r_allocator_register_existing_block`
   + `rdmaFollowerEnqueueSlotMerge` (cluster_rdma_chain.c:1008-1026, Patch 29).
5. **No CHAIN-STATUS RPC** exists (only leader-side aggregate DEBUG-CHAIN-STATUS :2262). Build it.
6. **No mgn_executed_idx** exists. Only whole-batch volatile booleans on the leader's backpatchBatch.
7. Line-number fixes: TXN_START built @cluster_rdma.c:6342 (not 6285); backpatchBatchKey @1779 (not 1776);
   WRITE_FLIP is a SEPARATE entry (applyWriteFlip raft.c:1114-15/2160) not the TXN_START apply;
   donor enum real order PREP→REGISTERING→FLIPPING→TRANSFER→BACKPATCH→DONE (cluster.h:237 comment stale);
   BACKPATCH-STATUS reply is 8 elems, does NOT expose `remaining`; become-leader raft.c:1313 ACCURATE.

## 1. Shared foundation (BOTH S1 and S2 need these) — build first
### 1a. Become-leader bridge (module → cluster_rdma)   [keystone]
- Module side (redisraft/src/raft.c `raftNotifyStateEvent`, RAFT_STATE_LEADER case ~1313):
  on promotion, scan applied Raft log (`raft_get_entry_from_idx` over 1..last_applied_idx,
  used already at raft.c:1370) for MGN_TXN_START whose `sess=` has no matching MGN_TXN_DONE.
  For each such sess, parse payload, and `RedisModule_Call` a NEW reverse command into
  cluster_rdma with the sess + role hint.
- cluster_rdma side: NEW command `RDMA MGN-RECOVER <role> <sess> <slots> <n> <recipient>`
  (register like other RDMA subcommands; DONT_INTERCEPT so it runs locally). Dispatches to
  recipient-recovery (S1) or donor-resume (S2) based on <role> or by inspecting local state.
- **SAFE FIRST INCREMENT (F2):** implement the scan + a LOG-ONLY MGN-RECOVER that just logs
  "AqRaft recovery: promoted leader detected in-flight sess=K (role=..) — recovery TODO".
  Compiles, changes no migration behavior, OBSERVABLE in S1/S2 runs (a leader IS elected).
  Proves the whole bridge before any risky recovery logic. DO THIS, VERIFY BUILD, OBSERVE.

### 1b. Session-keyed secondary index (recipient)   [S1 status + S2 resume both need it]
- Add `static dict *backpatch_batches_by_sess` (key = src_mig_id alone). Populate alongside the
  TWO existing dictReplace sites: cluster_rdma.c:1913 (DONE-SLOTS legacy), :2113 (DONE-SLOTS-INIT).
  b->src_mig_id already stored (:1503). Additive, no behavior change — SAFE.

### 1c. Bookkeeping = LOCAL STATE, NOT Raft   [S1+S2]  — ✅ IMPLEMENTED 2026-07-11
**DECISION (user): bookkeep locally, never through Raft.** The bookkeeping describes *node-local*
facts ("which blocks I hold, which merges I applied to MY keyspace") — every node's answer differs,
so consensus is semantically wrong and a Raft round-trip per slot prohibitively slow. What must be
global ALREADY IS (mgn-log TXN_START / INDX_UPD / TXN_DONE). On promotion:
`gap = (slots of committed sessions, from MY OWN raft log — local, snapshot-disable retains it)
       − (my local inventory)`.

Implemented as `rdmaSlotInventory` (cluster_rdma_chain.c): per-session {received bitmap, merged
bitmap, executed flag} + `rdmaInvMark{Received,Merged,MergedBySlot,Executed}` / query helpers,
~4 KB/session static. Hook sites (all apply-then-mark):
- follower receive: chain apply loop after `r_allocator_register_existing_block` success;
- leader receive: `rdmaDoneSlotsChunkCommand` (donor RDMA-wrote before sending the RPC);
- per-slot merged + executed: the single merge choke point in `mergeBackpatchTick`
  (`w->shadow==NULL` block / `merge_done=1` site; follower items resolve sess by received-bit).
`RDMA CHAIN-STATUS <sess> <lo> <hi> [MERGED]` now answers from this inventory (session-scoped,
merge-aware), with the old session-blind allocator probe as compat fallback.

**Safety without durability:** inventory lives and dies with the in-memory keyspace it describes
(sg4 = snapshot-disable); crash kills bytes+claims together. THE ONE RULE: mark only AFTER the
install/apply succeeded — a crash in between yields data-without-claim (harmless, idempotent
re-pull), never claim-without-data. CAVEAT: if snapshots are ever enabled, the inventory must join
the same persistence domain or be discarded at boot.

**Known collision (wire-protocol reality):** all donors number their own migrations (three
concurrent donors all send sess=1; CHAIN-FORWARDED carries sess only). Safe for the bitmaps because
donor slot RANGES are disjoint → range-scoped queries (how all consumers ask, via TXN_START
`slots=lo-hi`) are exact. `executed` bool is ADVISORY under collision; authoritative "executed for
session K" = all slots in K's range merged = CHAIN-STATUS MERGED over the range.

## 2. S1 — recipient-leader recovery (build order)
Precondition: #4 (S4) verified. Recovery reuses #4's re-form primitive.
1. [1a] become-leader bridge fires MGN-RECOVER(role=recipient, sess) on promoted redis4/redis5.
2. Detect active session: TXN_START seen, no TXN_DONE, and an INDX_UPD may or may not be committed.
   The clean recover-vs-retransmit boundary is the INDX_UPD commit point (doc §"clean boundary").
3. **CHAIN-STATUS RPC (NEW):** `RDMA CHAIN-STATUS <sess>` answered by each surviving sg4 peer with
   its per-slot holder bitmap. Substrate: g_chain_landing_registered[CLUSTER_SLOTS] (chain.c:961)
   — but that is process-global, NOT session-keyed; must extend to per-session slot tracking, or
   have the promoted leader infer from its own applied blocks + peer replies. Build the per-slot map
   slot→{holders}. Committed INDX_UPD ⇒ majority holds ⇒ every needed slot has ≥1 live holder.
4. **Gap-pull (inward):** for each slot the new leader lacks, establish an inward landing pool on
   itself and re-WIRE a surviving holder's successor := new-leader (or a transient recovery-link RPC),
   holder CHAIN-FORWARDEDs the slots, new leader installs via r_allocator_register_existing_block +
   rdmaFollowerEnqueueSlotMerge. **RISK: this is new RDMA QP/wiring in the inward direction — the
   transport currently bakes leader→…→tail. Segfault-prone; needs iterative HW testing.**
5. **Execute pending INDX_UPD:** run merge fully-applied, advance mgn_executed_idx. Idempotent
   don't-clobber merge => safe to re-apply partials.
6. **Re-form chain as head:** rdmaLeaderChainEstablish over survivors (exclude dead old leader);
   re-forward any slot lacking chain-majority before firing RECP_TXN_DONE (3-flag DONE on new leader).
   NEVER fire INDX_UPD/RECP_TXN_DONE without a real CHAIN-ACK (honor Part A invariant).
7. **Donor hand-off (=S2 path):** donor BACKPATCH-STATUS poll must re-resolve to the new leader
   endpoint for remaining chunks past the last INDX_UPD.

## 3. S2 — donor-leader resume (build order)
1. [1a] become-leader bridge fires MGN-RECOVER(role=donor, sess, slots, recipient) on promoted donor.
   (On donor-leader CRASH, migFail is NEVER called — resume MUST come from here. MAP_3 §4.)
2. New donor leader reads TXN_START payload from its own log (sess/slots-range/n/recipient). NOTE:
   payload has contiguous first-last only, not mig->chosen[] — enrich TXN_START payload with the full
   slot list (or chunk plan) for non-contiguous resume. WRITE_FLIP already applied on this replica
   (applyWriteFlip) so slots already redirect.
3. **Session status RPC (NEW, session-keyed, per-slot):** new donor leader → recipient leader
   "for sess=K which slots merged / which remaining?". Model on rdmaBackpatchStatusCommand
   (cluster_rdma.c:4174-4271) but look up via backpatch_batches_by_sess [1b] and surface `remaining`
   + per-slot state (existing reply omits remaining). Also closes the "batch stuck RUNNING" gap.
4. **Resume-from-TRANSFER:** re-run rdmaOutboundLinkOpen(4295) → rdmaMigratePrepHelper(5390) →
   rdmaReshardRegisterHelper(5589) → rdmaReshardTransferHelper(5852) for the REMAINING slots only,
   stamping the REPLICATED sess (not the new leader's node id) into DONE-SLOTS-CHUNK keys (5862-65)
   so the recipient's session-keyed batch matches. Then BACKPATCH → TXN_DONE.
   **RISK: QP+ibv_reg_mr rebuild + partial re-ship; segfault-prone; needs HW iteration.**
5. **Backstop abort:** if no new leader ever resumes (whole donor sg lost), recipient stuck batch
   needs an eventual timeout (recipient-side watchdog on a dead donor). Roll-forward covers common case.

## 4. Verification (per feature) — NEVER trust surface checks alone
DBSIZE-exact + 0 client errors does NOT prove correctness (S4 baseline was client-invisible yet
degraded). A genuine PASS requires the MECHANISM in the logs:
- S1 PASS: promoted redis4/5 logs "detected in-flight sess" → CHAIN-STATUS queried → gap-pull
  installed → pending INDX_UPD executed + mgn_executed_idx advanced → chain re-formed as head →
  REAL CHAIN-ACK → RECP_TXN_DONE. Migration DONE, DBSIZE 7,492,752, UPDATE-err≈0, crash-sig=0.
- S2 PASS: promoted donor leader logs "resume sess" → session-status RPC → resume-from-TRANSFER
  ships remaining → recipient batch completes (not stuck) → TXN_DONE. Same integrity checks.
Each: run_crash_scenario.sh <S> on the NEW binary; ALSO re-run S3+S4 for regression.

## 5. Risk register (why this is not an overnight-verifiable job)
- New inward RDMA QP/transport (S1 gap-pull, S2 resume) is the historically crash-prone area
  (Part B hit sdsdup(NULL) segfault, NULL-peer promote). Expect multiple build→deploy→35min-run
  debug cycles per feature.
- Raft-log scan on promotion must handle snapshot boundaries, multi-donor concurrent sess, and
  partial INDX_UPD. Correctness-critical; must not fire INDX_UPD without real durability.
- Two-layer loopback adds a moving part (module↔cluster_rdma) with its own failure modes.
=> Recommended: land the SAFE foundation (1a log-only F2, 1b index, 1c watermark field) verified,
   then implement gap-pull / resume-from-TRANSFER together WITH the user, iterating on HW.
