# AqRaft migration performance — status & what's left (2026-05-28)

Branch: `aqueduct_broken`. Experiment: `custom_reshard_v2_orch_raft` (single-round) and
new `custom_reshard_v2_orch_raft_chunked` (2-round). Workload: `workloada_prod`
(500K records, 50/50 read/update, zipfian, maxexecutiontime=300).

## TL;DR

Chased "why does throughput drop (not rise) after migrating slots to a new shardgroup
(sg4)?" Found and fixed several distinct issues; one remains (the pool-free reclaim).
Post-migration throughput went from a broken ~65K → ~157K (≈ the ~179K pre-migration
baseline at 200 threads). The residual gap is the recipient's unfreed RDMA landing
pool (~11 GB RSS) causing memory-pressure CPU overhead — fix in progress.

## Fixed (committed in this change)

1. **redisraft `applyWriteFlip` slot-range extend** (`redisraft/src/raft.c`).
   Multi-round migration crashed the client with `ERR ... couldn't associate a
   shardgroup slot`. Cause: `applyWriteFlip` is idempotent on the shardgroup, so on the
   2nd round it set `write_redirect[...]→sg4` without adding the new range to sg4's
   `slot_ranges` → router rejects. Fix: append the flip range to the already-registered
   sg4's `slot_ranges` (NARROW later rebuilds it cleanly).

2. **raft snapshot fork stall (workaround)** (`ansible/tasks/cluster/start_redisraft_instances.yml`).
   Recipient RSS bloats to ~11 GB (unfreed RDMA pools); periodic raft log-compaction
   forks the whole process every ~60s → multi-second stalls → throughput collapse.
   Workaround: `--raft.snapshot-disable yes --raft.log-max-file-size 0` (default on via
   `rdma_disable_raft_snapshots`). NOT production-safe (log grows unbounded) — proper
   fix is the pool-free below, then re-enable snapshots.

3. **YCSB leader-routing fix** (`ycsb_client/.../RedisClient.java`).
   The donor's `-MOVED` round-robins across ALL sg4 nodes, so ~2/3 of migrated-slot
   reads landed on FOLLOWERS (redis4, ycsb1); with `follower-proxy`, follower reads cost
   ~11-15 ms vs ~230 µs to the leader. Fix: on read-collapse, resolve the recipient
   LEADER via CLUSTER SLOTS (range's first node) and pin `slotOwner`; the write-MOVED
   path retries to the `-MOVED` target but no longer pins followers into `slotOwner`
   (reads share it). Recovered post-migration ~115K → ~157K, READ latency 1624µs → 817µs.

4. **2-round (chunked) orchestrated experiment** (new
   `ansible/tasks/cluster/reshard_cluster_rdma_v2_orchestrated_chunked.yml` +
   `ansible/experiments/custom_reshard_v2_orch_raft_chunked/`). Runs WRITE_FLIP →
   MIGRATE-ALL → NARROW twice (first half / second half of each donor's slots,
   cumulative NARROW). Both rounds reach DONE.

5. **Diagnostics**: per-process `pidstat` wired into monitoring + collection
   (`start_monitoring.yml`, `collect_results.yml`); YCSB per-host read-latency +
   routing instrumentation in `RedisClient.java` (temporary; remove before release).

## In progress

6. **Recipient pool-free / efficient apply** (`redis/src/cluster_rdma.c`).
   ROOT cause of the residual cap: post-migration sg4 (redis3) holds **10.85 GB RSS**
   vs donors' 1.66 GB (the unfreed RDMA landing pools), for FEWER keys. Per-command cost
   is identical (GET ~1.34µs) — the overhead is cache/TLB pressure on the single
   event-loop thread, making sg4 burn ~40% more CPU/op (85% vs 60%) → residual bottleneck.
   - **Part 1 (DONE, verifying):** in `mergeBackpatchTick`, copy each migrated kvobj into
     the recipient's OWN compact r_allocator block via `r_allocator_insert_kvobj`
     (don't-clobber via `kvstoreDictFind` first — do NOT free a copy via
     `r_allocator_free_kv`, it SIGSEGVs). Live keyspace then no longer references the pool.
   - **Part 2 (TODO):** reclaim the landing pool's physical pages. Must `madvise(MADV_DONTNEED)`
     (or munmap) the CONTIGUOUS landing-pool region only — NOT per-slot blocks (those mix
     with client-write managed blocks). Plumb the pool {base,size} from the cached
     connection (`rdmaGetConnection(c)->aqueduct_pool_buf`; `struct rdmamig_buffer` has
     `buffer`+`size` in `rdma_migration/internal.h`) into the `backpatchBatch` at
     DONE-SLOTS, then `madvise` at `backpatchFinalize` (after BACKPATCH_DONE = all merges).
     The `alloc_bloc_t` bookkeeping is heap-allocated separately, and `reset_freelist_for_slot`
     removes the dead pool blocks from the freelist, so reclaiming the pool pages is safe
     (nothing walks them post-migration).

## What's left (ordered)

- [ ] Finish pool-free Part 2 (reclaim landing pool) → verify sg4 RSS ~11 GB → ~1.6 GB,
      sg4 CPU 85% → ~60%, post-migration throughput rises toward/above the 4-leader max.
- [ ] Re-enable raft snapshots (`-e rdma_disable_raft_snapshots=false`) once RSS is bounded;
      confirm no fork stalls.
- [ ] Thread sweep (200 → 400+) post-fix to demonstrate the 4-shardgroup scaling INCREASE
      (donors had headroom at 200 threads: ~60% CPU; cluster is latency-bound, not saturated).
- [ ] Explain the recurring `RDMA backpatch-shadow: slot=N skipped M staged entries
      (donor block misclassification)` warnings (~2730/run) — correctness smell, not yet root-caused.
- [ ] Remove temporary instrumentation (YCSB `AQRAFT-INSTR` counters / per-host latency)
      before any release.

## Key numbers (200 threads, 2-round chunked, snapshots off)

- Pre-migration baseline: ~179K ops/sec, READ ~597µs (3 donor leaders ~p90 97%).
- Post-migration (after leader-routing fix): ~157K, READ ~817µs.
- Per-leader CPU post: redis0/1/2 ~60%, redis3 (sg4) ~85%.
- Per-target read latency before leader fix: donors ~260-360µs, sg4 FOLLOWERS 11-15ms.
- Recipient RSS: 10.85 GB (sg4) vs 1.66 GB (donor) — the pool-free target.

## How to run

```
cd ansible
# build/deploy after source edits:
sudo ansible-playbook -i inventory.ini tasks/build/build_redisraft.yml      # redisraft.so
sudo ansible-playbook -i inventory.ini tasks/build/deploy_redis_binary.yml  # redis-server (build locally first: cd redis/src && make)
sudo ansible-playbook -i inventory.ini tasks/build/build_ycsb.yml           # YCSB jar
# run (2-round, 200 threads, 60s pre-reshard baseline):
sudo ansible-playbook -i inventory.ini experiments/custom_reshard_v2_orch_raft_chunked/workload.yml \
  -e redis_variant=custom -e pre_reshard_pause=60 -e ycsb_threads_run=200 \
  -e experiment_name=custom_reshard_v2_orch_raft_chunked_workloada_prod -e redis_workload=workloada_prod
```
Results: `/tmp/experiments/custom_reshard_v2_orch_raft_chunked_workloada_prod/`.
</content>
