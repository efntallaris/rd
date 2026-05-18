# Phase 4 Plan — Keyspace-lock audit for thread-safe RDMA recipient apply

## Context

The source-side migration thread (commit `7654b280`) is shipped and verified: registration, FLIP, and EXEC all run on a dedicated worker thread, eliminating the ~4 s event-loop stall that the chunked-tick design used to expose. The recipient-side apply, however, **still runs on the main event loop** via `migrationApplyTick` (1 ms timer, ~8 slots/tick).

Two attempts to thread the recipient (commits `7f2045a1` and the recent revert in `b2c57b77`) both failed under multi-source load:

1. **Attempt 1** placed `recipientApplyWorkerStart()` in `initServerConfig`, which runs *before* `daemonize()` forks. Pthreads do not survive `fork()`; the worker was created in the parent and lost when the daemon child took over. The 10-slot smoke test masked it because the migration "completed" from the source's perspective.
2. **Attempt 2** fixed (1) by moving the start call into `initServer` (post-fork). The worker came alive, smoke test passed (`DBSIZE = 66`), but the full `custom_reshard_v2` workloada sweep at threads=32 **segfaulted** in the worker: `dmesg: redis-server[529802]: segfault at 48 ip ... error 4` — a NULL field deref from a partially-torn dict bucket. The worker had applied slot ~1120 / 1365 when a *different* source's `RDMA RESHARD-RECV-FLIP` RPC arrived on the recipient's main thread. That handler mutates `importing_slots_from[]` + calls `clusterDelSlot/AddSlot` for slots the worker was concurrently `dbAdd`-ing into. Our `recipient_apply_mu` only covered `processCommand`'s dispatch path; it did **not** cover the RPC handler, `clusterCron`, AOF replay, expiration, replication, or the many other event-loop callbacks that also touch `db->keys` / `migrating_slots_to[]` / `importing_slots_from[]`.

This matches the original author's note in `cluster_rdma.c`: "Reverted from pthread workers because the kvstore isn't thread-safe at the dict-resize / bucket level vs main-thread READ paths."

The architectural truth: **Redis's keyspace is mutated from too many event-loop sites for a single mutex to cover.** Threading the apply requires *all* of those sites to take the same lock. That's an audit + design step, not a single code change.

## Goal of Phase 4

Produce a complete call-site manifest of every read and write of the protected state, classify each, and propose a lock instruction. The output is one document the implementing PR consumes. **No code changes happen in Phase 4.**

## Protected state

Any of these fields, if mutated by one thread while another reads or writes, can crash the daemon or corrupt the dict:

1. `server.cluster->slots[CLUSTER_SLOTS]` — `clusterNode *` per slot (the routing table).
2. `server.cluster->migrating_slots_to[CLUSTER_SLOTS]` — set on source during FLIP; read on every redirect.
3. `server.cluster->importing_slots_from[CLUSTER_SLOTS]` — set on recipient during `RESHARD-RECV-FLIP`, cleared by worker after apply.
4. `server.db[i].keys` (kvstore) and its per-slot dicts — every key access.
5. `server.db[i].expires` (kvstore) — separate per-slot kvstore from `keys`; touched by every `setExpire` / `removeExpire` and by `activeExpireCycle`'s `kvstoreScan`. Same blast radius as `keys`.
6. `server.db[i].blocking_keys` (dict) — `signalKeyAsReady` inside `dbAdd`.
7. `server.ready_keys` — **global** list (not per-db) appended by `signalKeyAsReady` at [blocked.c:522](redis/src/blocked.c#L522) every time `dbAdd` fires. The blocking_keys lookup leads here; both must be covered.
8. `server.db[i].watched_keys` and module-keyspace-event subscribers (`notifyKeyspaceEvent`).
9. `kvstoreDictMetadata` per-slot stat counters (`alloc_size`, `cpu_usec`, `network_bytes_in/out` at [server.h:1191-1198](redis/src/server.h#L1191-L1198)) — mutated on every command via `clusterSlotStats*`; the worker dirties them whenever it applies a slot.
10. `clusterNode.slots[CLUSTER_SLOTS/8]` per-node bitmap — distinct from `cluster->slots[]`. Flipped inside `clusterAddSlot` / `clusterDelSlot` alongside the cluster-level entry. Reads from gossip handlers race the same write window.
11. `r_allocator` per-slot block lists (aqueduct's RDMA staging buffers).

Out-of-scope-but-watch: `server.pubsub_channels` / `server.pubsub_patterns` are mutated by `removeChannelsInSlot` (called from `clusterDelSlot`) and from module notification callbacks. The audit should flag any hits here; locking strategy can borrow the module-callback queue (see Risks).

## Audit method

Grep `redis/src/*.c` for each field. For each hit, record:

- `file:line` and the enclosing function.
- Which protected field is touched.
- **read / write**.
- **hot / slow** — is this on the command-dispatch hot path (every cmd) or a periodic / slow path?
- **Proposed lock instruction**: `rdlock`, `wrlock`, `none` (proven not racy), or `defer-to-main-thread` (e.g. module callbacks).

### Concrete grep recipes (to run during the audit)

```bash
# Slot routing table
grep -rn "server.cluster->slots\[" redis/src/

# Per-slot migration markers
grep -rn "migrating_slots_to\[\|importing_slots_from\[" redis/src/

# Kvstore + db->keys
grep -rn "->keys\b\|kvstoreDict\|db->keys\|kvstoreSet" redis/src/

# Blocking + watch + keyspace events
grep -rn "signalKeyAsReady\|blocking_keys\|watched_keys\|notifyKeyspaceEvent" redis/src/

# r_allocator
grep -rn "r_allocator_" redis/src/

# Slot mutation entry points
grep -rn "clusterAddSlot\b\|clusterDelSlot\b\|clusterAddSlotEx\b" redis/src/
```

The earlier source-side migration work already enumerated **35 read sites + 9 write sites** for `slots[]` alone, all in `cluster_legacy.c` + `cluster_rdma.c`. Extending to all **11 protected fields** (`keys`, `expires`, `blocking_keys`, `ready_keys`, `watched_keys`, `kvstoreDictMetadata`, `clusterNode.slots[]`, plus the four already-listed cluster-level entries, plus `r_allocator`) will likely produce **~120-180 call sites**, of which ~50 are kvstore-iteration sites that need the batch-and-drop scaffolding. Coverage spans `db.c`, `t_*.c`, `expire.c`, `cluster*.c`, `aof.c`, `replication.c`, `cluster_slot_stats.c`, `defrag.c`, `module.c`, `multi.c`, `notify.c`, `blocked.c`.

## Recommended lock design (to validate during the audit)

**Two-tier scheme**:

1. **`cluster_topology_lock`** — single `pthread_rwlock_t` covering "all slots" mutations. Multi-slot operations (`clusterResetAllSlots`, `clusterMoveSlotEx`, `clusterDelNodeSlots`, gossip `clusterProcessUpdate`) take this in **write** mode. Per-slot writers take it in **read** mode for the duration of their wrlock on `slot_locks[S]`. This is what makes the per-slot ordering rule expressible — see "Topology lock tier" below.
2. **`pthread_rwlock_t slot_locks[CLUSTER_SLOTS]`** (16 384 entries). The existing `cluster->slots_lock` (single rwlock added in the source-side PR) becomes the prototype for one cell of this array.

Lock discipline:

- **Read**: `topo_rdlock; slot_rdlock[S]; …; slot_unlock[S]; topo_unlock`. Many concurrent readers OK.
- **Write**: `topo_rdlock; slot_wrlock[S]; …; slot_unlock[S]; topo_unlock`. Exclusive on slot `S`, but other slots are free. Held briefly (microseconds for a dict insert; ~150 µs for a worker per-slot apply).
- **Apply worker**: per slot, takes the pair, performs all entries for that slot, releases. The topology rdlock blocks while a topology-tier writer (e.g. CLUSTER RESET) is queued, which is the desired behavior.
- **Topology mutation**: `topo_wrlock; … iterate slots …; topo_unlock`. Holder does not need per-slot locks because the topology wrlock excludes all readers + writers. Held for longer (milliseconds for an all-slots loop), but these ops are rare.
- **Lock ordering**: a per-slot writer may hold at most one `slot_lock` at a time and must hold `cluster_topology_lock` in rdlock mode while doing so. Cross-slot operations take the topology wrlock; they do not iterate `slot_locks[]`.

### Topology lock tier (closes the multi-slot-loop gap)

The per-slot scheme alone cannot serialize the existing multi-slot operations in `cluster_legacy.c`:

- `clusterResetAllSlots()` ([cluster_legacy.c:1095-1142](redis/src/cluster_legacy.c#L1095-L1142)) — loops 0..16383 calling `clusterDelSlot(j)`.
- `clusterMoveSlotEx()` ([cluster_legacy.c:5105-5110](redis/src/cluster_legacy.c#L5105-L5110)) — moves all of one node's slots to another.
- `clusterDelNodeSlots()` ([cluster_legacy.c:5118-5128](redis/src/cluster_legacy.c#L5118-L5128)) — clears all of a node's slots.
- `clusterProcessUpdate()` ([cluster_legacy.c:2360-2450](redis/src/cluster_legacy.c#L2360-L2450)) — gossip-driven sparse rebind.

All four require an atomic "many-slot" view. With the topology rwlock, they take it in write mode and don't touch `slot_locks[]` at all. Per-slot writers (apply worker, individual `CLUSTER SETSLOT`) take it in read mode so they exclude the topology wrlock but don't exclude each other.

Why rwlock over plain mutex: read is 99% of traffic on a healthy cluster. A plain mutex serializes command dispatch and kills throughput. Rwlock uncontended cost is ~2× plain mutex — still negligible against per-command latency.

Memory cost: 16 384 × `sizeof(pthread_rwlock_t)` ≈ **896 KB** (56 B × 16384 on Linux glibc) + one extra rwlock for the topology tier. Acceptable. If cache pressure shows up, fall back to a hashed pool (e.g. 256 locks, slot N uses `pool[N & 255]`).

## Phased implementation (out-of-scope for Phase 4, scoped here for the follow-up PR)

Each phase is independently shippable + testable:

1. **Phase 4a** — introduce **both** lock tiers in one shot: `cluster_topology_lock` (single rwlock) **and** `slot_locks[CLUSTER_SLOTS]` array. Add `topoLockRead/Write` and `slotLockRead(slot)/slotLockWrite(slot)` helpers (slot variants take the topology rdlock internally). Wrap the existing slot-table read/write sites in `cluster_legacy.c`, including the four multi-slot loops listed under "Topology lock tier" (these take the topology wrlock). Without both tiers 4a cannot ship coherently — the multi-slot loops break under per-slot locking alone. **No recipient threading yet.**
2. **Phase 4b** — extend the lock to `db->keys` **and** `db->expires` (separate per-slot kvstore). Wrap `lookupKey*` + `dbAdd*` + `dbDelete*` + `setExpire`/`removeExpire` + key-iteration sites. Apply the starvation-mitigation strategy chosen below to `activeExpireCycle` and `beginDefragCycle`. Apply stays on the main thread; goal is to land the lock infrastructure without behavior change.
3. **Phase 4c** — extend to `blocking_keys`, `server.ready_keys` (global), `watched_keys`, `signalKeyAsReady`. Introduce the **mandatory** module-callback queue (see Risks G3): worker queues `keyspace_event` records onto an SPSC ring, main thread drains and fires `moduleNotifyKeyspaceEvent` under main-thread serialization. `notifyKeyspaceEvent` itself splits into "main-thread direct call" and "worker enqueue" variants. Also covers `kvstoreDictMetadata` stat counters.
4. **Phase 4d** — finally move `migrationApplyTick` to a worker thread. Worker takes the topology rdlock + per-slot wrlock pair; enqueues notifications to the main thread; everything else is already covered by 4a-c.

Verification gates:

- **4a**: `redis-benchmark` baseline within 5% pre/post **plus** a stress run with concurrent client traffic + `CLUSTER FAILOVER` and `CLUSTER RESET` on a sandbox cluster, asserting no segfault and no torn slot map. Benchmark alone does not exercise the topology-tier paths.
- **4b**: `redis-benchmark` within 5% + full `custom_reshard_v2` sweep matches current 296K aggregate + an expire-heavy variant (set `active-expire-effort` to 10 during the sweep) to validate that the chosen starvation mitigation actually prevents worker stalls.
- **4c**: same as 4b + a script that hits BRPOP + keyspace-event subscribers during a sweep + a module that registers a keyspace-event callback and calls `RM_OpenKey` from inside the callback (validates the defer-to-main-thread queue under the exact scenario that deadlocks today).
- **4d**: the actual win — `custom_reshard_v2` deep dip ≤ 5% drop from baseline 357K post-recovery.

### Starvation mitigation (decision point for 4b)

`activeExpireCycle` ([expire.c:287](redis/src/expire.c#L287), `kvstoreScan` at line 431, time-budget exit at line 484) and `beginDefragCycle` ([defrag.c:1881-1901](redis/src/defrag.c#L1881-L1901)) hold rdlock for milliseconds while iterating `db->keys` / `db->expires`. A worker waiting on the writer lock behind them stalls the entire migration — the exact failure mode Phase 4 exists to remove. Pick one:

- **Batch-and-drop** (preferred): scan releases the rdlock between batches (every 64 keys) and re-acquires. Piggyback on the existing time-check loop; no new event-loop scheduling. Cost: tiny overhead per batch.
- **Skip migrating slots**: expire/defrag short-circuit slots present in `importing_slots_from[]`. Cheaper to implement; means expiration pauses for that slot during apply (usually fine — data is moving anyway). Cost: keys may live past their TTL by up to the migration window.

Default recommendation: batch-and-drop. Re-evaluate if it shows up in profiling.

## Critical files (the audit will enumerate definitively)

- `redis/src/cluster.h` — `slot_locks` array + topology-lock helpers
- `redis/src/cluster_legacy.h` — `clusterState` struct (existing `slots_lock` becomes the prototype for the array; new `cluster_topology_lock` lives here too)
- `redis/src/cluster_legacy.c` — slot mutation + gossip; topology-wrlock around multi-slot loops
- `redis/src/cluster_rdma.c` — RDMA RPC handlers + worker
- `redis/src/cluster.c` — `getNodeByQuery`, `clusterRedirectClient`
- `redis/src/db.c` — `lookupKey`, `dbAdd`, `dbDelete`
- `redis/src/server.h` — `redisDb` (per-slot `expires`, `kvstoreDictMetadata`), `server.ready_keys`
- `redis/src/server.c` — `processCommand`, init
- `redis/src/notify.c` — `notifyKeyspaceEvent` split into direct vs worker-enqueue variants (G3)
- `redis/src/kvstore.c` — internals (may stay untouched if we lock above it)
- `redis/src/expire.c`, `aof.c`, `replication.c`, `defrag.c` — slow paths needing rdlock + batch-and-drop scaffolding
- `redis/src/module.c`, `multi.c` — module callbacks (defer-to-main-thread queue) + transactions
- `redis/src/blocked.c` — `signalKeyAsReady` + `server.ready_keys` mutation

## Risks

- **Lock ordering bugs** are the #1 killer of multi-threaded refactors. Discipline: never hold two slot_locks; per-slot writers hold `cluster_topology_lock` in rdlock mode + exactly one `slot_lock`; multi-slot ops hold the topology wrlock and no slot_locks. Build-time enforcement via a tiny static analyzer is overkill here — code review + the phased rollout suffice.
- **Performance regression** from rwlock overhead on the hot read path. Mitigate: benchmark after each phase. Revert any phase that tanks throughput.
- **G3. Module callback re-entrancy** (`moduleNotifyKeyspaceEvent` → user `RedisModuleNotificationFunc` → `RM_OpenKey` → `lookupKey*`). The current API explicitly permits modules to call `RM_OpenKey` from inside notification callbacks ([module.c:4136](redis/src/module.c#L4136), [module.c:9363-9405](redis/src/module.c#L9363-L9405)). With the worker holding `slot_locks[S]` wrlock, firing a notify directly leads to:
  - **Same-slot lookup** → re-entrant rdlock-while-wrlock on the same `pthread_rwlock_t`. POSIX permits a deadlock here; glibc *does* deadlock.
  - **Different-slot lookup** → AB-BA potential against the main thread.
  
  **Mandatory mitigation** (not "may need" — verified): worker enqueues `keyspace_event` records onto an SPSC ring; main thread drains and fires the module callback under main-thread serialization. Scoped into Phase 4c, validated by the module-callback test in 4c's verification gate.
- **AOF / replication** generate side effects (writes to the AOF buffer in `feedAppendOnlyFile` / [aof.c:1409,1444](redis/src/aof.c#L1409); `server.slaveseldb` mutation in `replicationFeedSlaves` / [replication.c:588,629](redis/src/replication.c#L588)). Single-threaded today; cannot be safely called from a worker. The current `rdmaApplySlot` ([cluster_rdma.c:500](redis/src/cluster_rdma.c#L500)) correctly does **not** call `propagate()`, so 4d's worker is already AOF-safe for the apply itself; the risk reappears in 4c if a notification fires command propagation. Module-callback queue + the "no propagate from worker" invariant together close this.
- **Slow-path starvation** (`activeExpireCycle`, `defrag`) holding rdlock for ms at a time can stall the apply worker. Addressed by the batch-and-drop scaffolding in 4b; flagged here so the audit doesn't forget it during the field-by-field walk.
- **The original chunked-tick stays as a fallback** until Phase 4d ships and passes verification. We will not remove `MIGRATION_SLOTS_PER_TICK` until the threaded path is proven.

## Out of scope

- Threading any other path beyond `migrationApplyTick` (no general multi-threaded Redis).
- Removing the chunked-tick fallback until 4d is verified.
- Refactoring kvstore internals — we lock above kvstore, not inside it.

## Phase 4 deliverable

A single document at `docs/recipient-apply-thread-safety-audit.md` containing:

1. Every grep hit (~80-120 entries), classified hot-read / slow / mutation.
2. Per call site, the proposed lock instruction + a risk score (Low / Medium / High).
3. A summary table per file showing how many sites of each class.
4. A short "open questions" section for design choices that need human input before the implementing PR starts.

Once that document exists, the implementing PR (Phase 4a → 4d on a new branch off this one) knows exactly which lines to touch and in what order.

## Branch

`aqueduct-thread-migration` has been merged into `aqueduct` (tip `df636734`) and the branch deleted. Phase 4 audit lands directly on `aqueduct` as a docs-only commit, or on a fresh `aqueduct-recipient-apply-audit` branch off `aqueduct` if we want CI isolation. Implementing PR (4a → 4d) lands on `aqueduct-recipient-apply-thread` off the audit's tip commit.

---

## Reference notes — RedisRaft internals (research, no action yet)

Researched 2026-05-17; saved here so future planning can cross-reference without re-mapping the redisraft tree at `/users/entall/rd/redisraft/`. These notes are research only — they do **not** change Phase 4's scope, which is aqueduct's `migrationApplyTick` keyspace-lock audit, not redisraft.

- **Dump the raft log**: no `RAFT.DEBUG DUMPLOG` exists. Debug subcommands at [`redisraft.c:1296-1320`](redisraft/src/redisraft.c#L1296) are COMPACT, NODECFG, USED_NODE_IDS, EXEC, COMMANDSPEC. The on-disk log is RESP multibulk; format at [`log.c:23-49`](redisraft/src/log.c#L23); append site at [`LogAppend log.c:660`](redisraft/src/log.c#L660). To dump live, either add a new debug subcommand or hook `serverLog` from `LogAppend`.

- **Leader's view of followers**: emitted by `INFO raft` at [`redisraft.c:1761-1787`](redisraft/src/redisraft.c#L1761). Iterates `raft_get_node_from_idx` and prints `id`, `state` (`ConnGetStateStr`), `voting`, `addr`, `port`, `last_conn_secs`, `conn_errors`, `conn_oks`. No dedicated `RAFT.NODE LIST` command.

- **AppendEntries**:
  - Follower recv handler: [`cmdRaftAppendEntries redisraft.c:897`](redisraft/src/redisraft.c#L897); parses fields at lines 937-943, deserializes entries at 953-965, calls libraft's [`raft_recv_appendentries` raft.h:1032](redisraft/deps/raft/include/raft.h#L1032).
  - Leader send callback: [`raftSendAppendEntries raft.c:896`](redisraft/src/raft.c#L896); builds `RAFT.AE` and dispatches via `redisAsyncCommandArgv` (line 948); wired into libraft's callback table at [`raft.c:1339`](redisraft/src/raft.c#L1339).
  - Send-loop driver: timer [`callRaftPeriodic raft.c:1484`](redisraft/src/raft.c#L1484) at `rr->config.periodic_interval` (typically 100 ms), calls `raft_periodic` at line 1518 which triggers outbound AppendEntries. Also event-driven on log append + leadership changes.
  - Response handler: [`handleAppendEntriesResponse raft.c:853`](redisraft/src/raft.c#L853).
