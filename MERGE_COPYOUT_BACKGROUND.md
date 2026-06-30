# Backgrounding the recipient copy-out (merge keys) — analysis & path forward

**Context:** after an RDMA reshard, the sg4 **leader** spends ~20s draining migrated
keys out of the RDMA landing pool into its live keyspace. This is the cause of the
post-FLIP throughput depression observed in the 30M run (throughput sits *below*
pre-migration for ~20s, then climbs to the new max). This doc records why the
copy-out is on the main thread, what is already backgrounded, and the concrete fix.

Branch: `aqueduct_broken`. Recipient sg4 = redis3 (leader) + redis4/redis5 (chain
followers).

---

## 1. The symptom (measured, 30M workloada, 2× YCSB clients @ 50 threads)

Recovery to max throughput after the migration FLIP takes **~30s**, in two phases:

| phase | YCSB t | throughput | cause |
|---|---|---|---|
| pre-migration steady | 18–22s | ~71k ops/s | 3 donor leaders |
| NARROW hand-off | 23–31s | dip to 38k (1s) | donors surrender slots; clients re-resolve `CLUSTER SLOTS` + retry `-MOVED` |
| **copy-out depression** | **32–53s** | **~66k (below pre-migration)** | **sg4 leader main thread ~9–17% busy in `mergeBackpatchTick`** |
| new steady max | 54s+ | ~79k ops/s | pool drained; load now spread over 4 shardgroups |

Wall-clock alignment (sg4 leader log, `mergeBackpatchTick`): first tick t≈31s, last
tick t≈53s, ~22s window, ~842k keys copied out. Each 1s window logged
`main_thread_us ≈ 165000–186000` (16–19% of the main thread) — time stolen from
serving the migrated 25% of slots. In a closed-loop benchmark this directly caps
aggregate throughput during the drain.

> Note: this is **not** the closed-loop ceiling itself. Steady-state throughput is
> separately pinned at `threads / latency` (50 / ~633µs ≈ 79k/client); the copy-out
> only explains the *transient* ~20s sag below that ceiling.

---

## 2. What is ALREADY on a background thread

On the **followers** (redis4/redis5) the heavy per-slot apply work is already
offloaded to a detached pthread, so their event loop stays free to ack raft
`AppendEntries`:

- `chainApplyWorker` — [redis/src/cluster_rdma_chain.c:1137](redis/src/cluster_rdma_chain.c#L1137)
  (spawned per session; falls back to inline on `pthread_create` failure).
- Work done off-main: `r_allocator_walk_used_segments`, landing-pool block
  registration (mirrors the leader REGISTER-BLOCK path), shadow build, and the
  `ibv_reg_mr` pinning.
- Thread-safety vs concurrent client SETs to the same slots is provided by
  **per-slot recursive allocator mutexes** `r_allocator.mutexes[slot]`, held inside
  `r_allocator_register_existing_block` and across the block-walk / sanitize /
  freelist-reset critical section in `rdmaBackpatchSlotFillShadow`. Without it,
  concurrent allocator mutations corrupted the heap (recursive SIGSEGV at 400 YCSB
  threads). See comment at [cluster_rdma_chain.c:1123-1135](redis/src/cluster_rdma_chain.c#L1123).

Also already off-main: the leader's chain-source-pool `ibv_reg_mr(2.86 GB)`
pre-registration (Patch 15) — otherwise it blocked the event loop ~3s.

---

## 3. What is STUCK on the main thread — and why

The piece that taxes the sg4 **leader** is the *final* step: inserting migrated
kvobjs into the **live keyspace** (`kvstoreDictAddRaw` into `db->keys`), driven by
the chunked `mergeBackpatchTick` "merge queue" (throttled by
`--cluster-rdma-merge-keys-per-tick`, default 512).

It is deliberately on the main thread. Per the history comment at
[cluster_rdma.c:1351-1373](redis/src/cluster_rdma.c#L1351), two attempts to thread
it (branch `aqueduct-thread-migration`) were reverted:

- **Attempt 1:** worker started in `initServerConfig` → silently lost because that
  runs *before* `daemonize()` forks (pthreads don't survive fork).
- **Attempt 2:** worker started post-fork with a `recipient_backpatch_mu` that
  `processCommand` acquires for commands hitting importing slots. Reached slot
  ~1120 on the first DONE-SLOTS batch, then **segfaulted at 0x48** (NULL field
  access) — a race against one of the many *non-`processCommand`* main-thread paths
  that also touch `db->keys` / `migrating_slots_to` / `importing_slots_from`:
  `clusterCron`, the RDMA RESHARD-RECV-FLIP RPC handler, AOF, expiration,
  replication. The mutex only covered the command-dispatch path.

Root cause: Redis's **kvstore/dict is not thread-safe** at the dict-resize /
bucket level against live readers. The follower path (§2) sidesteps this because it
mutates allocator/landing structures, **not** the shared serving dict.

An even earlier off-main direct `kvstoreDictAddRaw` (`rdmaApplySlotBlock`) had the
same problem — it "raced the main thread" and installed kvobjs into an unregistered
pool → heap corruption. It was replaced by the build-shadow-off-main +
insert-on-main merge queue ([cluster_rdma_chain.c:963-978](redis/src/cluster_rdma_chain.c#L963)).

---

## 4. Can we background it? Yes — but it's a real change, not a flag

The history comment names the two proper fixes:

- **(a)** a broad audit + lock on *every* keyspace-mutation path (`clusterCron`,
  RPC handlers, AOF, expiration, replication, raft-apply), or
- **(b)** a kvstore redesign with **per-slot mutexes**.

Both were declared out of scope for the original PR; the chunked main-thread tick
is the interim mitigation.

**Recommended path: (b), scoped to the kvstore.** It is already half-proven — the
follower side (§2) does exactly per-slot-mutex-guarded off-main inserts at allocator
granularity. The leader's keyspace insert is harder only because it mutates the
shared Redis serving dict (`db->keys`), so the per-slot lock must also be taken by
the enumerated main-thread **readers/mutators** of that slot's keys, not just the
writer. Concretely:

1. Move the leader's per-slot copy-out (`rdmaBackpatchSlotWithStats` /
   `kvstoreDictAddRaw`) into `chainApplyWorker`-style detached workers.
2. Guard each slot's `db->keys` mutation with a per-slot kvstore lock.
3. Audit and take the same per-slot lock on the reader paths that touch an
   *importing* slot's keys: command dispatch, `clusterCron`, RESHARD-RECV-FLIP,
   AOF, expiration, replication, raft-apply. (Importing slots are a small subset,
   so the lock is only contended during the migration window.)

**Expected payoff:** removing the ~17% main-thread tax eliminates the ~20s
copy-out depression, so the system reaches max throughput in ~the NARROW-handoff
time (~8s) instead of ~30s. No steady-state throughput change (that's
generator-bound) — this is purely about shrinking the post-migration recovery.

---

## 5. Interim mitigation (no code change)

`--cluster-rdma-merge-keys-per-tick` (ansible var `rdma_merge_keys_per_tick`,
default 512, [config.c:3144](redis/src/config.c#L3144)) controls keys copied per
tick. Raising it drains the pool faster → shorter depression, but **larger per-tick
main-thread stalls** (it's still on the main thread). It does not fix the underlying
single-threaded copy-out; it only trades depression length for stall depth. See
HOWTO_30M.md §8.
