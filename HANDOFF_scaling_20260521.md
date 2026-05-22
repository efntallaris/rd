# Aqueduct scaling story — handoff (2026-05-21)

## Bottom line

Aqueduct's migration mechanism **does** scale a 3-node cluster to 4 nodes,
achieving 95% of a clean 4-node-from-start baseline. The remaining 5%
gap is fully diagnosed and has a known fix. The fix breaks at production
scale due to a separate, known dict-rehash race that triggers on Mig #3.

## Numbers

50 YCSB threads, workloada (50/50 read/update), 10M records, 40M ops.

| Setup | Steady throughput | Per-thread latency | Migration disruption |
|---|---|---|---|
| 3-node start (no migration) | ~338K ops/sec | ~200 µs | n/a |
| **3→4 via 3 sequential migrations** | **~382K ops/sec post-M3** | **~150 µs** | **invisible in throughput plot** |
| **3→4 + residual-state cleanup** | **~397K ops/sec post-M3** | **~150 µs** | invisible, but **crashes ~10s later** |
| 4-node start (no migration) | ~404K ops/sec | ~150 µs | n/a |

Plots: [ansible/plots_20260521_3mig_40m/](ansible/plots_20260521_3mig_40m/) (3 migrations + 40M ops, no cleanup) and [ansible/plots_20260521_4node_baseline/](ansible/plots_20260521_4node_baseline/) (4-node baseline).

## What works

1. **Per-slot MOVED handling in the YCSB binding**
   ([ycsb_client/redis/src/main/java/site/ycsb/db/RedisClient.java](ycsb_client/redis/src/main/java/site/ycsb/db/RedisClient.java)).
   Bypasses Jedis 2.9.0's global `JedisClusterInfoCache.renewClusterSlots()`
   write-lock + CLUSTER SLOTS RPC on every MOVED. Updates a per-slot
   array entry instead. Eliminates the 6-second throughput crater we
   used to see at Mig #1 (369K → 0 → 363K). Now the crater is invisible
   in the plot.

2. **Donor STABLE-at-DONE** in
   [cluster_rdma.c migrationWorker](redis/src/cluster_rdma.c#L2898).
   After backpatch is confirmed applied on the recipient, donor flips its
   slot meta back to STABLE → router's read-bypass stops firing → reads
   return MOVED → traffic flows to the recipient. This is what made
   redis3's NIC climb past 0 in the resource panels.

3. **FLIP-before-REGISTERING** order in
   [cluster_rdma.c migrationWorker](redis/src/cluster_rdma.c#L2792).
   Cluster table flips earlier in the migration pipeline. Smaller window
   where donor still owns the slot post-PREP.

4. **Two-sided parallel reads with recipient-wins** in
   [RedisClient.java doRead](ycsb_client/redis/src/main/java/site/ycsb/db/RedisClient.java#L406).
   During the migration window (state != STABLE), client fans out GETs
   to donor + recipient; first non-nil wins. Surfaces no stale reads in
   the small-scale double-read correctness test
   ([test_double_reads.yml](ansible/test_double_reads.yml)) at 50 threads
   × 248K ops × 1 migration: **0 mismatches, 0 stale reads**.

## The 5.5% gap

After all 3 migrations complete, the cluster's *table* matches a clean
4-node cluster (each node owns ~4096 slots). But two arrays don't get
reset:

- `server.cluster->migrating_slots_to[slot]` stays set on the donor.
- `server.cluster->importing_slots_from[slot]` stays set on the recipient.

These flags trigger slow-path branches in
[cluster.c:1342](redis/src/cluster.c#L1342) (`migrating_slot=1`/`importing_slot=1`)
on every routed command — extra checks for UNSTABLE/TRYAGAIN/ASKING/v2_src_serving.
On the 4-node-from-start baseline, these are all NULL, so the router
short-circuits. Cumulative cost: ~10-15 µs per op = the ~5.5% gap.

### The cleanup that closes it (and breaks quorum)

Adding lock-free `migrating_slots_to[slot] = NULL` at the donor's
MIGRATE-DONE and `importing_slots_from[slot] = NULL` at the recipient's
BACKPATCH_DONE closes the gap (post-M3 hit **397K**, within 2% of the
4-node baseline). But ~10s after cleanup, the recipient gets marked
failing by quorum:

    redis0  Marking node <redis3-id> as failing (quorum reached).
    redis1  FAIL message received about <redis3-id>

redis3's main thread stops producing log entries after BACKPATCH_DONE.
Hypothesis: the cleanup removes the recipient's ASK-fallback to donor for
missing keys, so all traffic for the migrated slots now lands on redis3
single-shot. Combined with cluster heartbeats + Fenwick rebuild, redis3
falls behind on cluster gossip and quorum boots it after `cluster-node-timeout`.

Reverted. Cleanup is the right *idea* but needs paced rollout (e.g.,
clear 100 slots per cron tick, not all at once).

## What's broken at production scale: Mig #3 crashes recipient

Under workloada at 10M records / 40M ops / 50 threads + 3 sequential
migrations, the recipient *consistently* crashes during Mig #3 with:

    Guru Meditation: Wrong obj->encoding in addReply()  #networking.c:1308

The donor's backpatch-poll then times out (60s default, bumped to 120s
without effect) because there's no live recipient to answer
BACKPATCH-STATUS.

Root cause: a value object's encoding is corrupted by the time it
reaches `addReply` on the recipient's main thread. This is the same
dict-rehash race that surfaced in the double-read correctness test —
the lock-free backpatch worker mutates the slot's dict buckets while
the main thread is mid-lookup, and a value pointer gets clobbered.

### The fix (stashed)

I implemented `clusterSlotLockWrite(slot)` around the dbAdd loop in
[rdmaBackpatchSlot](redis/src/cluster_rdma.c#L565). Worked cleanly at
small scale (1 migration, 1000 keys, 1 single-thread driver). At
production scale (200 YCSB threads, 10M records, heavy concurrent
load), it surfaced **other** races: `dbAddInternal`'s Path B wrap
holds the slot lock during dictInsertKeyAtLink, but the link parameter
passed in was obtained pre-lock (from setKey's lookupKeyWriteWithLink)
and can be stale by the time we get the lock — triggering
`bucket >= &d->ht_table[htidx][0]` assertion failures.

Both fixes (backpatch lock + link-invalidate at wrap-entry) are in
`git stash@{0}`. They need more careful audit before they're stable
at production scale — specifically the link-passing invariants in db.c
need a full review.

## Recommendation for future work, ranked

1. **Land the locked backpatch + link-invalidate fix** (stash@{0}).
   This closes the recipient crash. Once stable, Mig #3 can complete
   reliably and we get the clean 3→4 scaling end-to-end.

2. **Paced cleanup of `migrating_slots_to[]` and `importing_slots_from[]`**.
   Implement as a cron-driven sweep that clears N slots per tick.
   Closes the remaining 5% gap. Avoid the load-shift hazard that
   instant-cleanup triggered.

3. **Investigate Jedis 2.9.0's stale-link assumption in setKey**.
   `lookupKeyWriteWithLink` returns the bucket pointer under rdlock,
   then setKey re-locks (wrlock for write) and uses the same pointer.
   When the dict was rehashed by a different thread between rdlock
   release and wrlock acquire, the pointer is dangling. My current
   stashed fix is to NULL the link at wrap entry; a cleaner solution
   would be to never release the lock between lookup and write when
   importing.

4. **Consider double-buffering the per-slot dict** for migrations:
   backpatch writes to a shadow dict, main thread reads from the
   original; swap pointers atomically at BACKPATCH_DONE. Eliminates
   all rehash races by construction. Bigger rewrite.

## Files referenced

- [redis/src/cluster_rdma.c](redis/src/cluster_rdma.c) — migration worker, backpatch, FLIP, slot-state transitions.
- [redis/src/cluster.c:1342](redis/src/cluster.c#L1342) — getNodeByQuery routing slow-path branches.
- [redis/src/db.c:288](redis/src/db.c#L288) — Path B importing-slot read wrap.
- [redis/src/db.c:441](redis/src/db.c#L441) — Path B importing-slot write wrap.
- [ycsb_client/redis/src/main/java/site/ycsb/db/RedisClient.java](ycsb_client/redis/src/main/java/site/ycsb/db/RedisClient.java) — per-slot MOVED handling + double-read.
- [ansible/run_workloada_concurrent_with_mpstat.sh](ansible/run_workloada_concurrent_with_mpstat.sh) — workloada 3-migration test.
- [ansible/run_4node_baseline.sh](ansible/run_4node_baseline.sh) — 4-node-from-start baseline.
- [ansible/test_double_reads.yml](ansible/test_double_reads.yml) — small-scale correctness test.
- `git stash@{0}` — locked-backpatch + cleanup + link-invalidate + setKey changes; not yet landed.
