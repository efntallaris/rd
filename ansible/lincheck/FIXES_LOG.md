# rd-c7 (linearizability work) — changes to the shared tree
Backup of the tree before any of this: git branch backup/pre-lincheck-2026-09-30 (cf16a4a3),
tarball /mnt/aqsnap/backups/rd_src_pre_lincheck_2026-09-30.tgz

- ycsb_client/.../RedisClient.java: redis.retry.ambiguous (default true = old behavior), Outcome/OpResult,
  read() refactored into readCore() (same routing), new getValue/setValue/delValue/incrValue.
- ycsb_client/.../LinHistoryClient.java: new history recorder.
- lincheck/: new Go/Porcupine checker (+ testdata).
- ansible/lincheck/run_lin_scenario.sh: new runner (wraps crash/run_crash_scenario.sh unchanged).
- ansible/tasks/cluster/start_redisraft_instances.yml: --raft.quorum-reads {{ raft_quorum_reads | default('no') }} (default unchanged).

## 2026-10-01 ~00:55Z — chain forward gate (server fix), rebuilt on all 6 nodes
Backup before: branch backup/pre-fwdgate-2026-10-01 (3917fcff, includes rd-13/rd-12 edits) +
/mnt/aqsnap/backups/pre_fwdgate_*/{cluster_rdma.c,cluster_rdma_chain.c,redis-server.bin}
Bug: zero-copy chain forward RDMA-reads landing blocks while FillShadow (rewrites headers) and the
adopt-in-place merge (client overwrite/DEL decrRefCounts kvobjs in place) mutate them. Followers got
corrupted blocks -> "illegal decrRefCount ... refcount 0" panic on redis4+redis5 in a HEALTHY run with
SET/DEL churn on migrated keys; also "NO valid kvobjs staged" warnings (likely the 3.2% missing keys).
- cluster_rdma.c: backpatchBatch.fwd_final/fwd_held; pool worker parks items at do_fillshadow until
  chainFwdGateOpen() (called wherever the forward consumer drops its landing hold, incl. after re-form).
- cluster_rdma_chain.c: non-tail follower defers its chainApplyWorker until its CHAIN_WORK_FORWARD to
  the successor is done (chainWorkItem.deferred_apply).

## 2026-10-01 ~01:35Z — follower landing-block registration (server fix), rebuilt on all 6 nodes
Backup: allocator.c/allocator.h added to /mnt/aqsnap/backups/pre_fwdgate_*/
Bug: chainApplyWorker registered RECEIVED landing blocks with r_allocator_register_existing_block, whose
init_bloc_layout overwrites the first segment header with "one free 2 MiB segment" -> the walker skips the
block: followers staged ~10k of ~2.5M keys per session ("NO valid kvobjs staged"), donor freelist words
left unsanitized; then raft-applied SETs crashed both followers in r_allocator_insert_kvobj.
- rdma_migration/allocator.{c,h}: new r_allocator_register_filled_block (bookkeeping only, no writes).
- cluster_rdma_chain.c chainApplyWorker: uses it.

## 2026-10-01 — walker skips dead kvobjs (server fix), rebuilt on all 6 nodes
Bug: applySlotCb accepted orphaned (freed) kvobjs from registered donor blocks (refcount 0, header otherwise
intact). Donor overwrites/DELs leave old versions in shipped blocks -> stale values adopted (leader) /
refcount-0 kvobj adopted then overwritten -> "illegal decrRefCount" panic (followers).
- cluster_rdma.c applySlotCb: skip kv->refcount <= 0 (rate-limited NOTICE log).

## 2026-10-01 ~09:20Z — FillShadow walks only landing blocks (server fix), rebuilt on all 6 nodes
Backup: /mnt/aqsnap/backups/pre_landingonly_*/cluster_rdma.c
Bug: rdmaBackpatchSlotFillShadow walked ALL slot blocks, staging live kvobjs created by post-flip raft SETs;
a DEL before the merge freed them, then the merge installed the freed segment -> "illegal decrRefCount" on
the next DEL/SET (both followers, HEALTHY run, 02:46:52). Now uses r_allocator_get_landing_blocks_for_slot.
(The refcount<=0 skip from the previous entry never fired; kept as a guard.)

## 2026-10-01 ~10:05Z — recipient tombstones + authoritative read metadata (server + client), rebuilt all
Backup: /mnt/aqsnap/backups/pre_tombstones_*/ (db.c cluster.c cluster.h server.h config.c cluster_rdma.c
RedisClient.java start_redisraft_instances.yml)
- New config rdma-tombstones (default no; start_redisraft_instances.yml sets yes on sg4).
- db.c dbGenericDelete: records every deleted key (even if absent locally) under the importing-slot lock.
- cluster_rdma.c: tombstone set; all merge installs (bg drain, mergeBackpatchTick incl. follower merges,
  legacy direct) skip tombstoned keys. Tombstones are never removed yet (needs sg4 end-of-import marker).
- cluster.c addReplyGetWithMeta: 4th element flags (bit0 slot fully merged on this node, bit1 key tombstoned).
- RedisClient.readCore: a recipient nil is final when authoritative; donor fallback only otherwise;
  unreachable recipient -> UNKNOWN; stable path asks the recipient when the donor reports migrating.
- ansible/lincheck/run_all_lin.sh: foreign-campaign guard matched its own launcher's command text (hung); fixed.
Known remaining: DEL on sg4 before the merge returns 0 when the donor has the key (reply-count violation);
INCR/APPEND before merge.

## 2026-10-01T10:04:16Z — fix: tombstone dictType used positional init (field order wrong) -> DEL crashed sg4 leader; designated init. Rebuilt.

## 2026-10-01T10:36:03Z — run_lin_scenario.sh also kills the orphaned reshard bash loop (re-drove 70x sessions into the next run).

## 2026-10-01T10:38:08Z — quick test profile: ycsb_client/workloads/workloada_lin_quick (300k keys, 90 s, fresh load, no snapshot); run_lin_scenario.sh PROFILE=quick default, PROFILE=full = 30M.

## 2026-10-01 ~10:55Z — first clean quick HEALTHY migration; DEL/RMW admission gate (server fix)
Quick HEALTHY (HEALTHY_qyes_20261001_103808): migration rc=0, 0 crashes, followers merged all keys, 0 failed/
unknown ops, 0 lost writes; 175/200 keys linearizable. With -ignore-del-count (new lincheck flag): 200/200 PASS,
so every violation was DEL's reply count (DEL on sg4 before the merge replied 0 while the donor had the key).
Backup: /mnt/aqsnap/backups/pre_rmwgate_*/ (server.c cluster_rdma.c cluster.h)
- server.c processCommand: before RedisRaft's command filter (i.e. before the request is appended), sg4 nodes
  (rdma-tombstones) reply -TRYAGAIN to DEL/UNLINK/INCR*/DECR*/APPEND/GETDEL/GETSET/GETEX/SETNX/SETRANGE/
  EXPIRE*/PERSIST and SET NX|XX|GET|KEEPTTL|IFEQ when a key is absent, not tombstoned, and its slot is not fully
  merged on this node (rdmaRejectUnmergedRmw in cluster_rdma.c). Leader-local admission; the log stays deterministic.

## 2026-10-01T10:55:18Z — quick HEALTHY PASS (HEALTHY_qyes_20261001_104940): 200/200 linearizable, 0 lost, 0 fail/unknown, migration rc=0, no crashes.

## 2026-10-01 ~11:55Z — quick S1–S5: all linearizable (200/200, 0 lost); S1 migration did NOT complete
Kills verified in playbook.log (S1 redis3@6th chunk, S2 redis0@TRANSFER, S3 redis1, S4 redis4@forward, S5
redis0 after DONE). S1: new sg4 leader redis5 re-forwarded the recovered session under the SAME chain sess id;
follower redis4 returned its EXISTING landing pool (already merged in place = live keys) -> overwritten ->
SIGSEGV (siphash, SET) -> sg4 lost majority -> RE-DRIVE INCOMPLETE.
Backup: /mnt/aqsnap/backups/pre_poolretire_*/cluster_rdma_chain.c
- cluster_rdma_chain.c: followerRetirePool() — chainApplyWorker retires a ring pool once it registered its
  blocks (live storage; next claim of that index mmaps fresh); CHAIN-PREP for an already-applied session hands
  out a fresh pool.
- run_lin_scenario.sh: copies this run's injector lines from playbook.log (was copying stale root-owned files).

## 2026-10-01 ~12:10Z — S1 rerun PASS incl. migration completion; postcheck added to every run
S1_qyes_20261001_114804: kill redis3@6th chunk; redis5 promoted; redis4 got fresh pools for 2 re-deliveries;
migration rc=0; lincheck 200/200, 0 lost.
- ansible/lincheck/postcheck.py (new): migration (playbook rc + all 3 ranges durable on current sg4 leader via
  MGN-RESUME-STATUS), crashes (no crash signature on any node), replicas (live sg4 replicas' migrated-slot
  keyspaces identical, read locally via RAFT.DEBUG EXEC SCAN/GET), bulk (every donor YCSB key of the migrated
  slots present on the sg4 leader). First use on S1 state: all PASS (2 replicas x 75,045 keys identical; 74,880
  donor keys, 0 missing).
- run_lin_scenario.sh: OVERALL = lincheck AND postcheck; summary.txt shows both.

## 2026-10-01T12:44:24Z — S2 still FAILS replicas (reproduced: 33 keys on sg4 leader only; both surviving sg1 replicas hold them; recovery re-shipped all 1365 slots). Follower logs show "follower-merge: skipped N staged entries" only in recovery re-ships (S1/S2). run_lin_scenario.sh: periodic node-log grabber (collect_results deletes /tmp/redis_logs; run_crash_scenario then blanked the sg4 leader log). postcheck.py: donors check + divergence explainer.

## 2026-10-01 ~13:05Z — S2 root cause: per-slot block count included another session's stale landing blocks
S2 leader log: recovery session (8e17+1) "pool-filter fetched=2 in_pool=1" + "block count drift in_pool=1
expected=2 — using newest 1" for every slot. Phase C counted ALL landing blocks of the slot (incl. the crashed
session's) -> nb=2 -> phantom position; capture filled it with a duplicate that overwrote the NEXT slot's
(contiguous) position -> followers got a neighbour's block, missed this slot's keys (33 leader-only keys).
Backup: /mnt/aqsnap/backups/pre_poolfilter_*/
- cluster_rdma.c: batchPoolRange()/landingBlocksInRange(); Phase C counts only the batch's own pool;
  rdmaBackpatchSlotFillShadow(..., pool_lo, pool_bytes) walks only the batch's pool (leader pool worker) / the
  applied pool (follower apply); promotion recovery still walks all held blocks.
- cluster_rdma_chain.{c,h}: rdmaFollowerEnqueueSlotMerge gets the pool range.

## 2026-10-01T13:03:48Z — S2 PASS after pool-filter fix (all checks; 0 drift, 0 skipped entries, 3 replicas identical). Regression run of all 6 started.

## 2026-10-01T13:39:56Z — REGRESSION ALL PASS on current build: HEALTHY, S1, S2, S3, S4, S5 (quick profile) — lincheck 200/200, 0 lost; migration, crashes, replicas, donors, bulk all PASS. Kills verified per run.

## 2026-10-01 ~15:20Z — follower merge on background threads (performance)
30M campaign: after the migration the sg4 followers merged ~7.5M keys in the main-thread mergeBackpatchTick
(~19% of the main thread for ~30 s); sg4 leader AE round-trip 0.3 -> 1.5 ms; UPDATE latency +8%, throughput dip.
Backup: /mnt/aqsnap/backups/pre_followerbg_*/
- cluster_rdma.c: rdmaFollowerMergeSlotBackground() — FillShadow + bgMergeDrainShadowLocked on the caller
  (chainApplyWorker) thread, clears the slot's active flag, marks the inventory merged.
- cluster_rdma_chain.c: CHAIN-FORWARDED (main thread) marks the session's slots bg-merge-active before the apply;
  chainApplyWorker uses the background merge when rdma-merge-background is on (default), clears skipped slots.
  Promotion recovery (B#1 held-block merge) still uses the main-thread tick.

## 2026-10-01T16:22:57Z — follower bg merge: quick correctness set ALL PASS (HEALTHY, S1..S5). Starting 30M HEALTHY perf comparison.

## 2026-10-01T16:30:07Z — 30M HEALTHY with follower bg merge: no follower main-thread merge; after-migration 180-186 Kops/s (was 162-171), UPDATE 1.62 ms (was 1.81), sg4 AE RTT 0.99 ms (was 1.46). Recorded in experiments/2026-10-01_crash_campaign/README.md.

## 2026-10-01 ~17:35Z — perf investigation: warm-up registration throttle (experimental knobs) + ops sampler
Backup: /mnt/aqsnap/backups/pre_warmthrottle_*/
- server.h/config.c: rdma-warm-reg-batch / rdma-warm-reg-pause-us (MODIFIABLE, default 0 = off).
- cluster_rdma.c warmRegisterThread: optional pause every N live-block registrations; logs elapsed ms.
- start_redisraft_instances.yml: passes both to donor instances (vars rdma_warm_reg_batch / _pause_us, default 0).
- experiments/2026-10-01_crash_campaign/run_campaign.sh: EXTRA_MORE hook; per-instance total_commands_processed
  sampler (ops.txt per run).
Finding: 30M HEALTHY — donor leaders' main threads at ~100% CPU before AND after the migration, sg4 leader ~100%
after; servers saturated (client JVMs ~4 of 20 cores). 200/300-thread YCSB runs invalid (client start-up 85 s+ /
never started within the 3-min run).

## 2026-10-01T17:35:28Z — warm throttle confirms warm-up dip = donor MR registration (min 59 -> 107 Kops/s throttled). Per-leader rates: donors ~100K -> ~80K cmds/s at 100% CPU after migration (per-command cost up), sg4 95K; total +12%. In campaign README.

## 2026-10-01T19:51:52Z — early prewarm (rdma_prewarm_early, reshard_prewarm.yml) removes pre-migration dip; bottleneck = leader main thread (100% before+after); capacity 155 -> 190 Kops/s (+23%) at 200 thr/host; per-leader capacity -8% after. In campaign README (Follow-up 3).

## 2026-10-02 ~19:35Z — NARROW twice to the same recipient group left the earlier slots dangling (multi-round migrations)
Backup: redisraft/src/raft.c.bak_renarrow
Found by the first 3 -> 6 scale-out runs (experiments/custom_scaleout_3to6, 6 rounds per donor).
- Symptom: sg2 leader SIGSEGV in validateRaftRedisCommandArray (raft.c:238, sg->slot_ranges NULL) 8 ms after
  the round-2 "NARROW(donor) applied".
- Cause: the donor narrows after every round, naming only that round's range. The NARROW apply path replaced
  the existing external shardgroup entry (free + add) without clearing the slot-map entries that pointed at
  it, and the new entry did not own the earlier rounds' slots: use-after-free on the next command for an
  earlier slot, and those slots had no owner on the donor.
- Fix (raft.c, NARROW apply): carry the old entry's ranges over to the new one and drop every slot-map
  pointer to the old entry before freeing it. The donor log now shows "added (N slot ranges)" growing per round.
- Also present with one recipient and n_rounds > 1 (the window is the time until the playbook's final NARROW
  re-covers the range; a 2-round control run passed by luck). All earlier lincheck runs were single-round.
- postcheck.py: the crashes check now also reads the node logs the runner copied (--logs-dir); the playbook
  deletes them on the hosts before postcheck runs, so this crash had been reported as "crashes PASS".

## 2026-10-02/03 — fault-path fixes found by the 3 -> 6 scale-out crash scenarios (S1-S9)
Backups next to each file: *.bak_refresh, *.bak_chainlistener, *.bak_fwtree, *.bak_barrier_qp, *.bak_donorlink.
Before: HEALTHY, S3, S5 pass; S1, S2, S4, S6-S9 fail. Each fix was confirmed by re-running the scenario it targets.

1. Client (ycsb_client RedisClient.java, refreshSlotOwner): after losing a donor leader, the client re-resolved a
   slot from any node's CLUSTER SLOTS and accepted a recipient's claim for slots of rounds not migrated yet (the
   recipient is configured for its whole final range). Reads/writes of those slots went to the recipient: stale
   reads (S2 25 keys, S7 35, S8, S9). Now a recipient owner is accepted only if a donor reports it or the slot is
   known to be moving; donor nodes are asked first; answers naming a dead endpoint are skipped. S2, S7 PASS.
2. cluster_rdma_chain.c: the chain listener reused server.rdma_server, which on a former recipient leader is the
   donor-facing INIT-SERVER listener, while CHAIN-INIT-QP replied with rdma-migration-port + 1000. After a
   leadership change the new leader's connect to the old leader was refused and no later session could be
   replicated ("chain RE-FORM impossible ... no live QP"). Now a dedicated listener on that port. S4 PASS.
3. kvstore.c / fwtree.c: kvstoreFenwickRebuild runs on a worker thread after a batch while the main thread keeps
   deleting keys; a delete landing mid-rebuild was counted twice and a later delete tripped
   fwtree.c:68 ASSERTION FAILED on a promoted recipient leader (S1, S8). A decrement that would go below zero now
   rebuilds the tree from the dict sizes instead of asserting.
4. cluster_rdma_chain.c chainConnect: one failed 1 s connect dropped a live follower from the session; it now
   retries for up to 3 s (a killed process still refuses at once).
5. cluster_rdma.c promotion merge (MGN-RECOVER recipient, inside raft_become_leader): the landing barrier
   checksummed every held block at least twice on the main thread; a new recipient leader stalled > 1 s (watchdog
   stack), missed heartbeats and lost the leadership it had just won (repeated elections in S4/S6). The barrier is
   skipped on that path (nothing writes those blocks any more). S6, S9, S8 PASS.
6. cluster_rdma_chain.c: a chain QP that reported a failed write/completion (transport retry exceeded) kept being
   reused by later sessions (findLivePeerClient / findLiveSuccessorClient), so every later round failed to that
   live follower and it never received them (replicas diverged, S1). Broken clients are no longer reused.
7. cluster_rdma.c donor worker: (a) after a failed RDMA write the donor still sent DONE-SLOTS-CHUNK, so the
   recipient merged blocks that never arrived and reported them durable; the re-drive trusted that and 32,696
   keys of a pair were lost (S1). A chunk with a failed write is no longer announced. (b) The outbound link with
   the failed QP was reused by every later round; it is now marked broken and the next migration opens a new one
   (the startup source pool is handed over to it).
Also: scaleout_pairs.py treats a donor that answers nothing (crash report still being written) as down.
8. cluster_rdma.c chain capture: a slot is marked ready for the zero-copy chain forward only after its landing
   blocks have settled (rdmaLandingSettle, the barrier the local merge already applies). Kept as a safeguard; it
   was NOT the cause of the S9 divergence (see 10).
9. The broken-link replacement of fix 7b first removed the old link with dictDelete, which crashed in the dict
   free path (donor SIGSEGV in a re-run of S1); the new link is now swapped into the cache entry in place.
10. cluster_rdma_chain.c forwarder completion accounting (the S9 divergence): the send CQ is shared by all
   sessions on a QP. A session abandoned mid-forward (its donor died) left completions behind; the next
   session counted them as its own ("forward STALL ready=0/455 posted=0 reaped=269"), reached
   reaped == n_slots after posting only part of its blocks and announced the whole batch to the followers,
   which applied empty blocks ("donor blocks present but NO valid kvobjs", CHAIN apply staged=6213 instead of
   ~8200). YCSB updates re-created most of the missing keys afterwards, so only 7-11 keys still differed at
   the end. Now: leftovers are drained before posting, a session never counts more completions than it has in
   flight, and an aborting session reaps its own writes first.
11. cluster_rdma.c / cluster_rdma_chain.c: the follower's background merge (chainApplyWorker) now takes the same
   dict-resize hold as the leader's pool workers (rdmaMergeResizeHold). Without it the main thread could
   expand/rehash a slot dict while the merge thread expanded it: dict.c:1801 'dictIsRehashing(d)' assertion on an
   sg4 follower (one S1 run).
12. rdma_migration/rdma_server.c: the accept loop exited on EINTR ("rdma_get_request: Interrupted system call —
   exiting accept loop"), so after any signal (the Redis watchdog's SIGALRM) the node never accepted another
   RDMA connection and a new leader could not chain to it. It now continues on EINTR. Note: the watchdog
   (-e redis_watchdog_ms) therefore changed behaviour in earlier runs; do not use it with older binaries.
13. cluster_rdma_chain.c catch-up: (a) it now also covers configured followers that a RE-FORM dropped from
   the session (they were never caught up although alive); (b) when no follower can serve a lacking one, the
   leader re-sends the batch itself, retried ~15 s apart (cluster_rdma.c chainCatchUpWorker); the landing block
   list is kept after finalize for that. The leader re-send path has not been seen to complete in a run yet.
14. cluster_rdma_chain.c chainConnect: a refused connection (dead process) fails at once; only timeouts are
   retried. Retrying refusals made every session after a follower's death wait 3 s before its chain was set up.
15. cluster_rdma.c rdmaMergeSkipKey: a merge arriving after the slot's session was closed on this node installs
   nothing. A batch received as leader, not merged before being deposed, was merged at the next promotion AND
   again by its queued work item, after the session (delete tracking) had been closed: a key deleted in between
   came back (S1: DEL, three nil reads, then the donor's old value).
16. cluster_rdma_chain.c: a failed RDMA client is replaced, not only avoided: the leader reconnects to its chain
   head before a re-forward (leaderEnsureHeadClient), a follower asks its successor for the RDMA port when it
   has to open a new QP (the leader passes rdma=0 on the reuse path), and recipe/repair links skip failed
   clients. Before, one transport error cut a follower out of every later round (S2, S9: chain tail).
17. Harness: topology.yml sets raft_election_timeout 1000 / raft_request_timeout 100 for the 3 -> 6 topology
   (AppendEntries round-trips peak at 650-850 ms while three pairs transfer; with two live replicas after a
   host crash a 300 ms timeout deposed leaders at every round start). scaleout_pairs.py re-drives a round
   that stays pending after its donor stopped.
Result on the final build (2026-10-03 16:17-18:20Z): HEALTHY + S1-S9, two repetitions, 20/20 PASS
(/tmp/lincheck/scaleout3to6/final6/summary.txt).
Backups: *.bak_fwdbarrier, *.bak_resizehold, *.bak_cqdrain.
Open: RDMA writes between hosts occasionally fail (transport retry counter exceeded) or take seconds while
several transfers run at once; the fixes above make the system recover from that, they do not explain it.
Regression on the same build, existing single-recipient setup (2026-10-03 18:10-18:40Z): HEALTHY + S1-S5,
one run each, 6/6 PASS (/tmp/lincheck/scaleout3to6/base_regress/summary.txt).

## 2026-10-03 (evening): sequential transfers, saturating load

18. Transfers are sequential by default (`scaleout_mode=sequential`: one pair at a time within each
    round). All earlier results above were taken with the three pairs transferring at once, which is
    not the intended procedure. Sequential runs show no RDMA transport errors.
19. `scaleout_pairs.py` poll: a donor whose recipient leader died re-ships the round itself under
    id 8e17 + first slot and leaves the original id in BACKPATCH; the driver polled the dead id for
    120 s (S1 window 145 s). It now also reads the re-ship's status (S1 window 28 s).
20. `redisraft/src/cluster.c` handleShardGroupResponse: a RAFT.SHARDGROUP GET reply arriving after
    NARROW replaced (freed) the recipient's ShardGroup dereferenced it (SIGSEGV in
    compareShardGroups on the sg2 donor leader, S1 at 200 threads). Returns early when the
    connection is flagged terminating. Backup: cluster.c.bak_sgterm.
21. Load defaults for this experiment (env.sh): workloada_lin_quick_uniform, 400 threads per
    client. Leaders are at 100% CPU before and after; throughput ~155k -> ~305k ops/s.

Results, sequential, HEALTHY + S1-S9 once each: 32 threads zipfian 10/10
(/tmp/lincheck/scaleout3to6/seq1), 200 threads zipfian 10/10 (t200b), 400 threads uniform 10/10
(uni400all). Figure: experiments/2026-10-02_scaleout_3_to_6/figures/scaleout_3to6_ycsb_sequential.png

## 2026-10-03 (night): migration duration, 27.5 s -> 7.7 s fault-free

Quick profile, 3 x 2730 slots in 18 transfers of 455 slots (954 MB each as fixed 2 MiB blocks,
17.2 GB in total), 25 GbE RoCE with RoCE MTU 1024: the copy runs at 22.8 Gb/s payload, which is the
line rate at this MTU (~23.1 Gb/s after per-packet overhead); 18 copies take ~6.2 s.

22. Landing pools: `rdma_landing_prereg_pools` = ROUNDS (was 1). A pool is retired into the keyspace
    and not reusable; registering a new 1.1 GB pool inside every round cost 0.3 s. (env.sh)
23. `rdma_transfer_chunk_slots` 57 (was 342): the leader forwards to its follower per chunk, so
    small chunks let that copy follow the donor's closely. (env.sh, CHUNK_SLOTS)
24. `cluster_rdma.c` mergeBackpatchTick handled one slot per 1 ms timer tick: 455 ticks = 0.47 s per
    round for ~0.5 ms of work, and MGN_INDX_UPD / the donor's TXN_DONE waited for it. A tick now
    keeps taking items for 250 us. This was the largest single item (8.5 s). Backup: *.bak_mergetick.
25. `cluster_rdma_chain.c` early forward: the leader reports its progress to the chain head
    (CHAIN-PING with a large negative argument) and the head forwards those blocks to its successor
    at once instead of after the whole round; the recipe forward sends the remainder and stays the
    fallback for every failure. Backup: *.bak_earlyfwd.
26. `scaleout_pairs.py`: plain-socket client instead of one redis-cli process per call (0.1 s per
    transfer), last-known-leader first, 5 ms poll.
27. `scaleout_pairs.py --mode pipelined`: one copy at a time, but the next pair is dispatched when the
    previous copy ends (donor in BACKPATCH) without waiting for its commit. Copies never overlap
    (checked from the donor logs); idle time between copies ~50 ms.
28. `rdma_migration/rdma_client.c`: ack timeout 13 (33 ms) with retry_count 7, was 16 (268 ms) with
    3. The network drops RoCE packets now and then: over one 10-scenario suite the recipient NICs
    counted rx_discards_phy 2765 / 961 / 2000 and sent pause frames that no sender received
    (rx_global_pause 0 everywhere), i.e. the switch does not do flow control. This is the cause of
    the "unexplained" slow or failed RDMA writes noted on 2026-10-02/03. With the old timeout one
    transfer took 2.7 s instead of 0.34 s. Backup: *.bak_acktimeout.

Fault-free migration window: 27.5 s (start) -> 20.3 (22, 23) -> 11.4 (24) -> 9.4 sequential (26)
-> 7.7 pipelined (27). Results on the final build, uniform keys, 400 threads per client:
pipelined HEALTHY + S1-S9 10/10 (/tmp/lincheck/scaleout3to6/pipe3), windows 7.7 s fault-free,
7.9-17.2 s with crashes. Figure: figures/scaleout_3to6_ycsb_pipelined.png.
Not done: jumbo frames (RoCE MTU 4096 would give ~24.5 Gb/s, ~7% faster copies) and PFC/ECN need
the experiment network reconfigured; the sg2 pair (redis1 -> redis4) copies at 19.4 Gb/s instead of
22.3 for an unknown reason.
Sequential mode on the same final build: HEALTHY + S1-S9 10/10 (/tmp/lincheck/scaleout3to6/seq3), windows 9.4 s
fault-free, 9.2-16.8 s with crashes. Figure: figures/scaleout_3to6_ycsb_sequential.png.

## 2026-10-04: recovery-time breakdown, entry 28 reverted

Entry 28 (ack timeout 13, retry_count 7) is REVERTED to 16 / 3. These NICs do not time out faster
than ~0.54 s per try whatever is requested, so the change did not make a lost packet cheaper and a
write to a dead peer took 4.3 s to fail instead of 2.4 s (S6, leader -> dead chain follower). The
pipe3 and seq3 suites above ran with 13 / 7; seq4 (/tmp/lincheck/scaleout3to6/seq4) is the
sequential suite on the reverted build.

Where recovery time goes (seq3, sequential, fault-free window 9.4 s):
- Raft election in the group that lost its leader: 1.0-1.6 s after the kill (election timeout
  1000 ms); the driver waits for it before that pair's next transfer.
- Recipient leader failover: every later round of that pair pays ~0.5 s (S1, S6) to ~0.85 s (S9)
  in the donor's REGISTERING state, because its pre-registered source pool belongs to the link to
  the old leader and the new link registers each round's slots again (ibv_reg_mr).
- A write to a dead chain follower mid-copy: the RDMA retry timeout above, then re-form and
  re-forward to the survivor in 0.36 s (S6).
- With 0.4 s transfers and ~0.55 s between the injector's marker and its kill, the kill usually
  lands when the victim's pair is between rounds (S1, S2, S7, S8, S9: pair B or C was copying), so
  these runs no longer exercise the donor re-home / MGN-RECOVER paths that the 2026-10-03 runs did.

29. S2 (donor leader crash). `scaleout_pairs.py prepare` now sends MIGRATE-WARM to the donor's
    followers too (before the leader; `--warm-followers no` turns it off; settle 10 s). A follower
    promoted after the crash was cold and registered every block inside TRANSFER (57 MRs, ~85 ms per
    chunk): its copies took 0.70 s instead of 0.35 s for all remaining rounds, with its clients
    stalled. With warm followers: inwindow_reg=0, copies 0.34 s, window 12.0 -> 10.2 s, clients back
    to their pre-crash level ~4.5 s after the kill instead of ~7 s and no more dips until the end.
    Tried and dropped: warming the new leader after the crash, under load -- 4 s with the group
    frozen (window 14.1 s). What is left in S2 is the election (1.0-1.3 s, election timeout
    1000 ms) and the clients' reconnect (one YCSB client stayed at zero for ~3 s).
    seq4 (reverted ack timeout): 10/10, S6 window 16.0 -> 13.0 s. seq5: suite with warm followers.
seq5 (warm followers, sequential): HEALTHY + S1-S9 10/10; figure figures/scaleout_3to6_ycsb_sequential.png.

30. YCSB client (`RedisClient.java` refreshSlotOwner): CLUSTER SLOTS answers are shared by all
    worker threads of the JVM (one per probed node, reused for 100 ms), and a reply moves every slot
    of the range that still pointed at the same dead endpoint. Before, each of the 400 threads
    re-resolved each of the dead leader's ~2700 slots by itself on a new connection; thread dumps
    2.4-5.4 s after the S5 kill showed 280-350 of 400 threads waiting for a CLUSTER SLOTS reply.
    Clients at zero after a donor-leader crash: S5 ~6 s -> ~1.5-2 s (election at +1.1 s), S2 ~3 s
    -> ~2 s (election at +1.5 s). Backup: RedisClient.java.bak_sharedview. Suite: seq6.
31. Pair B is slow because of redis1, not the network: with sg5's leader moved to redis5
    (topology swap) redis1 -> redis5 still copied in 0.39-0.45 s and redis2 -> redis4 in 0.34 s.
    redis1's memory is half as fast as redis0's and redis2's (memcpy of 1 GiB 0.99 s vs 0.50 s,
    dd to /dev/shm 1.5 vs 3.1 GB/s, source-pool mmap+populate 5-7 s vs 1.2 s). Hardware; not fixed.
32. Seen, not fixed: after a recipient-leader failover the donor registers its source for the new
    link inside every round with the slots already write-flipped (0.85 s per round in S8, 1.3 s the
    first time): that is S8's saw-tooth. In S9 the driver's re-drive loop (fixed 3 s + 2 s sleeps)
    kept pair A from starting its next round for 4.4 s.
seq6 (shared slot view in the client, warm followers, sequential): HEALTHY + S1-S9 10/10; figure figures/scaleout_3to6_ycsb_sequential.png.
S5 removed from the experiment on 2026-10-04 at the user's request (it killed the donor leader after its last round was fully committed).
S6 and S7 taken out of the default scenario list and the figure on 2026-10-04 (user: may be added back later); still defined in scenarios.env.

2026-10-04, single-recipient (3 -> 4) setup, PROFILE=full (30M keys, snapshot ws30m, 100 threads per client), current build:
HEALTHY PASS (/tmp/lincheck/3to4_30m): 7.49M keys moved, 3 x 1365 slots (2.86 GB each), migration window 5.5 s, 142k -> 157k ops/s.
S1-S4 on the same setup (30M): 4/4 PASS (same OUT_ROOT).

33. Entry 25 (follower early forward) REMOVED 2026-10-04: `cluster_rdma_chain.c` restored from
    .bak_earlyfwd (the version with it is kept as cluster_rdma_chain.c.with_earlyfwd). On the 30M
    3 -> 4 run, S4 (recipient follower killed as the forward starts) failed: the dead follower had
    claimed the tail's landing pool for its early writes, the leader's re-forward after RE-FORM was
    refused ("chain head redis5:8000 is being written by another attempt"), the round was never
    committed and redis5 lacked it (5.34M of 7.49M keys). The quick 3 -> 6 runs did not hit it
    because their 0.4 s rounds are over before the injector's kill. The change had not shortened the
    commit wait anyway (a majority is the leader plus ONE follower).

34. `cluster_rdma.c` captureSlotSnapshot: no landing barrier when the capture runs on the main
    thread (rdmaDoneSlotsChunkCommand). The barrier added there on 2026-10-02 (.bak_fwdbarrier)
    checksums every block twice; with 30M keys (full 2 MiB blocks) that stopped the recipient
    leader's event loop for 244 ms per 342-slot chunk, 12 times per 3 -> 4 migration. Fault-free
    30M run: 142k -> minimum 18k for ~4 s before, 150k -> minimum 130k for 1 s after (200 threads;
    same as the 2026-10-01 figure). Backup: .bak_capturebarrier. The quick profile never showed it
    (blocks nearly empty). Lesson: check throughput on the 30M profile after server changes.
    30M 3 -> 4 results on 2026-10-04: HEALTHY, S1, S2, S3 PASS (before 33/34), S4 FAIL with early
    forward -> PASS after 33 (/tmp/lincheck/3to4_30m, 3to4_30m_b, 3to4_30m_c).

3 -> 6 quick suite on the build with 33 + 34 (2026-10-04 18:13-18:52Z): HEALTHY, S1, S2, S3, S4, S8, S9
7/7 PASS (/tmp/lincheck/scaleout3to6/run7). Windows: 12.1 / 14.1 / 13.1 / 12.1 / 11.9 / 19.8 / 16.0 s.
Fault-free 12.1 s, was 9.3 s with the early forward: commit wait per transfer 0.24 s instead of 0.08 s.

35. Merge after the copy, 30M (2026-10-04 night). Stack samples of the recipient leader during the
    merge: 41 of 64 pool-worker samples waited in cumulativeKeyCountAdd (kvs->shared_mu, taken per
    merged key) and 17 in rdmaTombstoneHas (g_tombstones_mu, per key). `cluster_rdma.c`: the Fenwick
    deferral now follows the backpatch-in-progress refcount (it used to end when the copy ended,
    which since the forward gate is when the merge starts); per-slot tombstone counts let the merge
    skip the lookup for slots without tombstones. Backup: .bak_mergelocks.
    3 -> 4, 30M, fault-free, 8.0 GiB: merge per donor 1.42 s (3 threads) -> 1.07 s (16 threads, old
    code) -> 0.36 s (16 threads, new code); migration window 5.9 -> 4.9 -> 4.1 s; with 57-slot chunks
    3.9 s (copies alone 3.2 s). Settings used from now on for big runs: rdma_backpatch_pool_size=16,
    rdma_transfer_chunk_slots=57 (3 -> 6 env.sh has both; for 3 -> 4 pass them in EXTRA_ANSIBLE_ARGS).
    What is left after each copy: the merge (0.35 s), which cannot start before the round's forward
    to the follower is over (forward gate), and the commit.
    Side finding: sampling the leader with gdb (pauses > 300 ms election timeout) caused leadership
    churn in sg4 and that run then FAILED linearizability (5 keys). A paused-then-resumed leader is
    not one of the crash scenarios; not investigated.

Big campaign 2026-10-04/05 on the build of entry 35 (16 merge threads, 57-slot chunks, 200 YCSB
threads per client, 30M keys): 3 -> 4 HEALTHY S1 S2 S3 S4 S5 S8 7/7 PASS (/tmp/lincheck/big/3to4),
3 -> 6 HEALTHY S1 S2 S3 S4 S8 S9 7/7 PASS (/tmp/lincheck/big/3to6, snapshot ws30m_3to6 created for
it). Report: experiments/2026-10-02_scaleout_3_to_6/BIG_CAMPAIGN_2026-10-04.md.

## 2026-10-05: setup out of the migration, no dips in the fault-free runs (30M)

36. `cluster_rdma.c` rdmaSrcPreregAdopt: a new link takes the startup source pool over from the idle
    link to the previous recipient leader. After a recipient failover every remaining round used to
    mmap + register a pool of its own inside the round, after its writes had been redirected
    (0.5-1.0 s per round). 3 -> 6 S9: setup per round 0.8-1.0 s -> 0.03 s, time near zero 3 s -> 1 s,
    migration 18.4 -> 14.9 s; S1 14.4 -> 12.3 s. Both pass all checks. Backup: .bak_poolowner.
37. Fixed sleeps removed from the recovery loops: `tasks/cluster/reshard_cluster_rdma_v2_orchestrated_nround.yml`
    (3 -> 4; donors still recovered one at a time) and `scaleout_pairs.py` redrive (3 -> 6). Polls
    every 0.1 s. Backups: .bak_sleeps. NOT yet measured on a crash run.
38. `cluster_rdma.c`: the ownership flip wrote one NOTICE line per slot under the topology write lock
    (and the recipient one per slot on its main thread); now one summary line (per-slot lines at
    VERBOSE). The flip also sent a CLUSTER MYID probe that always fails in AqRaft mode (~20 ms per
    round); skipped. Fault-free setup per transfer: 3 -> 4 0.07 -> 0.03 s, 3 -> 6 0.033 -> 0.017 s.
    What is left inside a round's setup: three Raft commits that open it, the landing-buffer
    exchange with the recipient (it must follow the write redirect: it sizes the buffers from the
    donor's frozen block list) and the flip RPC. Backup: .bak_myid.
39. YCSB client `redis.preconnect=ip:port,...` (harness var `ycsb_preconnect`; 3 -> 6 env.sh passes
    the nine recipient replicas): every worker thread opens its connection to those nodes at
    start-up. Without it all 400 threads connected to a recipient leader the moment it first served
    traffic and it accepted them before answering anything: a ~25% dip for 0.2 s (found with a
    5 Hz per-leader sampler + thread dumps; warm-up writes through the recipients, also added to
    `scaleout_pairs.py prepare`, did not remove it). Backup: RedisClient.java.bak_preconnect.
40. Merge threads: 16 merge fastest but starve the recipient leader's request handling while they
    run (3 -> 4 fault-free: 0.2 s samples down to 111k from 153k at each donor's merge). 8 threads:
    no sample more than 10% below the start level, migration 3.9 s (16: 3.7 s, 4: 4.5 s). Default
    now 8 (3 -> 6 env.sh MERGE_THREADS; pass rdma_backpatch_pool_size=8 for 3 -> 4).
41. 3 -> 6 with raft election/request timeout 300/50 ms: fault-free runs stable (no leader change
    after the start-up transfer). Passed on the command line so far (SCALEOUT_EXTRA_ARGS);
    topology.yml still says 1000/100 until the crash scenarios have been run with it.

Fault-free on this build, 30M, 200 threads per client (single runs; lincheck pass):
3 -> 4 (/tmp/lincheck/big/m8): 156k -> 173k, lowest second 149k, migration 3.9 s.
3 -> 6 (/tmp/lincheck/big/g1): 156k -> 277k, lowest second 154k, lowest 0.2 s sample 153k, migration 12.4 s.
Figures: experiments/2026-10-02_scaleout_3_to_6/figures/healthy/. Tools: experiments/tools/scaleout/
opsamp.py + opsview.py (per-leader request rate at 5 Hz).
The crash scenarios have NOT been rerun on this build.

## 42. Follower merge on several threads (2026-10-05)

Symptom: 3 -> 4, 30M, fault-free: throughput peaked at the end of the migration (170k) and then sat
at 156-166k for about 6 s before returning to 168-170k.
Cause: each recipient follower merged a round on one thread (~1.2 s for 2.5M keys) and only after
forwarding it, so the three rounds queued and the followers finished 3.4 s after the leader.
Fix: `chainApplyWorker` (`redis/src/cluster_rdma_chain.c`, backup `.bak_followerpar`) registers the
landing blocks as before, then merges the slots on up to 8 threads (`rdma_backpatch_pool_size`).
Result (`/tmp/lincheck/big/pf1`, lincheck pass, post-check not run): followers finish 1.2 s after
the leader instead of 3.4 s; per second after the migration 171, 170, 169, 170, 169, 171, 172k.
Not yet run: any crash scenario, 3 -> 6.

## 43. Per-slot forward gate: leader merge overlaps the replication to the follower (2026-10-05)

The forward gate (2026-10-01) held every slot's merge until the whole round had been forwarded, so
the merge started where the replication ended (0.5 s per round on 30M). Now a slot is merged as soon
as its block's write to the chain head has completed (`fwd_blk_done` / `fwd_slot_ok` in
`backpatchBatch`, `chainFwdBlockDone`, callback from `rdmaLeaderChainForwardPipelined`). If the
forward fails (head died) the forwarder opens the whole gate and waits for `merge_done` before the
re-forward, as `chainRepairWorker` does. Setting `rdma-fwd-slot-gate no` restores the old gate.
Backups `*.bak_slotgate`. Tested on the QUICK profile only: 3 -> 4 fault-free + S1 S2 S3 S4 S8 pass
(`/tmp/lincheck/sg/q34`, `c34`), 3 -> 6 fault-free pass (`q36`). In S4 the head died after the whole
round had reached it (waited 0 ms for the merge): a death mid-round is untested. 30M not run yet.
Gantt tools: `gantt_pairs.py` draws the merge from the first forwarded block when the log says slots
were released early; `chunkgantt.sh <run dir> <out.png>` feeds `plot_phase_gantt.patched.py` the
logs of all hosts per group (its recipient lanes are wrong after a failover: matched by order).

## 44. Dead-peer check on control connections (2026-10-05) -- BUILT ON redis0 ONLY, NOT DEPLOYED, NOT RUN

Finding (S1/S4/S8, quick 3 -> 4): every connection to a killed server is reset 2.7-2.8 s after the
kill, in the same millisecond for Raft, the chain and the clients. Until then a connect to the
victim succeeds and the first RPC waits. Likely cause (not measured directly): the kernel closes a
killed process's sockets after releasing its memory, which is slow with tens of GB registered for
RDMA. YCSB uses a 10 s socket timeout (`ycsb_timeout`), so its threads wait for that reset too.
Change: `rdmaPeerAnswers` (PING within `rdma-peer-probe-ms`, default 300, two tries, 0 = off) in
`chainConnect` and in `rdmaOutboundLinkOpen`. Backups `*.bak_peerprobe`.
Not covered: a peer that dies in the middle of an RPC or of a forward (S4: CHAIN-FORWARDED), Raft's
own connections, and the clients (try `-e ycsb_timeout=500`; the probe client uses 2000 ms).
Risk: a live follower whose main thread stalls for more than ~600 ms is treated as dead.

## 45. 2026-10-05 afternoon: recovery-path changes (quick profile only; 30M not run on any of them)

- `scaleout_pairs.py` `wait_tail_hop` (3 -> 6 driver, backup `.bak_tailhop`): before a pair's copy, wait
  until the previous pair's LAST replica holds the previous round if it is on the next recipient
  leader's host. Fixes the S1 saw-tooth (two 22 Gb/s flows into one port: copy 0.34 -> 0.67 s).
  Ask CHAIN-STATUS under session 7e17 + round, not 0 (0 = "0 slots" for good: 3 s timeout per round).
  Not covered: S9, where three flows meet on one host (follower's host, and a hop from two pairs back).
- S2 saw-tooth (3 -> 6): every copy whose SOURCE is redis1 lowers all leaders' rates 20-50% once
  redis1 hosts two donor leaders (per-leader sampler, `/tmp/lincheck/pp/f36`, ops_f36.txt). Not fixed.
- Server (`*.bak_parprobe`): `chainProbeFollowers` (all followers pinged in parallel, chain formed
  from those answering within `rdma-peer-probe-grace-ms` of the first) and chain warm-up on promotion
  (`rdma-chain-warm-on-promote`, in `rdmaRecipientRecover`). S1 3 -> 4: round recovered at +2.3 s
  instead of +3.1 s. BUT the parallel check gives S4 a 1 s dip to 25-50k (3 of 3 runs with it, 0 of 1
  with `-e rdma_peer_probe_grace_ms=-1`, none on the two earlier builds); cause not found.
  => the playbook passes -1 (off) by default now; the server default is still 50.
- `start_redisraft_instances.yml` now passes rdma_peer_probe_ms, rdma_peer_probe_grace_ms,
  rdma_chain_warm_on_promote, rdma_fwd_slot_gate (all `-e` selectable).
- 3 -> 4 driver (`reshard_cluster_rdma_v2_orchestrated_nround.yml`, `.bak_roletimeout`): role queries
  -t 0.3; MIGRATE-ALL-STATUS -t 0.3 and "no answer for 0.6 s" counts as orchestrator down (a killed
  server accepts silently for ~2.8 s). S5 re-send STILL starts at +2.87 s after that: not fixed.
- Scenario names: "both leaders" is S5 in both setups (was S8); 3 -> 4 old S5 is S5_DONOR_AFTER.
  3 -> 6 figure shows HEALTHY S1-S5 (S9 still runs, not drawn). Legend / axis changes in
  `plot_scaleout_ycsb.py` (`.bak_s5rename`).
- Still open: 0.2-0.25 s between "chain established" and the first forwarded block in EVERY round
  (not the twin registration); clients wait ~2.8 s on a killed leader (ycsb_timeout 10 s).
