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
5. `server.db[i].blocking_keys` (dict) — `signalKeyAsReady` inside `dbAdd`.
6. `server.db[i].watched_keys` and module-keyspace-event subscribers (`notifyKeyspaceEvent`).
7. `r_allocator` per-slot block lists (aqueduct's RDMA staging buffers).

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

The earlier source-side migration work already enumerated **35 read sites + 9 write sites** for `slots[]` alone, all in `cluster_legacy.c` + `cluster_rdma.c`. Extending to all 7 protected fields will likely produce **~80-120 call sites** across `db.c`, `t_*.c`, `expire.c`, `cluster*.c`, `aof.c`, `replication.c`, `cluster_slot_stats.c`, `defrag.c`, `module.c`, `multi.c`.

## Recommended lock design (to validate during the audit)

**Per-slot rwlock**: `pthread_rwlock_t slot_locks[CLUSTER_SLOTS]` (16 384 entries). The existing `cluster->slots_lock` (single rwlock added in the source-side PR) becomes the prototype.

Lock discipline:

- **Read**: `pthread_rwlock_rdlock(&slot_locks[S])` around the read window. Many concurrent readers OK.
- **Write**: `pthread_rwlock_wrlock(&slot_locks[S])` exclusive. Held briefly (microseconds for a dict insert; ~150 µs for a worker per-slot apply).
- **Apply worker**: takes wrlock per slot, performs all entries for that slot, releases.
- **Lock ordering**: a caller may hold at most one `slot_lock` at a time. Cross-slot operations (CLUSTER FAILOVER, gossip-driven slot reassignment) take them in slot-ascending order to avoid AB-BA.

Why rwlock over plain mutex: read is 99% of traffic on a healthy cluster. A plain mutex serializes command dispatch and kills throughput. Rwlock uncontended cost is ~2× plain mutex — still negligible against per-command latency.

Memory cost: 16 384 × `sizeof(pthread_rwlock_t)` ≈ 880 KB. Acceptable. If cache pressure shows up, fall back to a hashed pool (e.g. 256 locks, slot N uses `pool[N & 255]`).

## Phased implementation (out-of-scope for Phase 4, scoped here for the follow-up PR)

Each phase is independently shippable + testable:

1. **Phase 4a** — promote `cluster->slots_lock` (single rwlock) to `slot_locks[CLUSTER_SLOTS]` array. Add `slotLockRead(slot)` / `slotLockWrite(slot)` helpers. Wrap the existing slot-table read/write sites in `cluster_legacy.c`. **No recipient threading yet.**
2. **Phase 4b** — extend the lock to `db->keys`. Wrap `lookupKey*` + `dbAdd*` + `dbDelete*` + key-iteration sites. Apply stays on the main thread; goal is to land the lock infrastructure without behavior change.
3. **Phase 4c** — extend to `blocking_keys`, `watched_keys`, `signalKeyAsReady`, `notifyKeyspaceEvent`. Module callbacks may need a defer-to-main-thread queue (verify during audit).
4. **Phase 4d** — finally move `migrationApplyTick` to a worker thread. Worker takes `slot_locks[slot]` wrlock per slot; everything else is already covered by 4a-c.

Verification gates:

- **4a**: `redis-benchmark` baseline within 5% pre/post.
- **4b**: `redis-benchmark` within 5% + full `custom_reshard_v2` sweep matches current 296K aggregate.
- **4c**: same as 4b + a script that hits BRPOP + keyspace-event subscribers during a sweep.
- **4d**: the actual win — `custom_reshard_v2` deep dip ≤ 5% drop from baseline 357K post-recovery.

## Critical files (the audit will enumerate definitively)

- `redis/src/cluster.h` — `slot_locks` array + helpers
- `redis/src/cluster_legacy.h` — `clusterState` struct
- `redis/src/cluster_legacy.c` — slot mutation + gossip
- `redis/src/cluster_rdma.c` — RDMA RPC handlers + worker
- `redis/src/cluster.c` — `getNodeByQuery`, `clusterRedirectClient`
- `redis/src/db.c` — `lookupKey`, `dbAdd`, `dbDelete`
- `redis/src/server.c` — `processCommand`, init
- `redis/src/kvstore.c` — internals (may stay untouched if we lock above it)
- `redis/src/expire.c`, `aof.c`, `replication.c`, `defrag.c` — slow paths needing rdlock
- `redis/src/module.c`, `multi.c` — module callbacks + transactions

## Risks

- **Lock ordering bugs** are the #1 killer of multi-threaded refactors. Discipline: never hold two slot_locks; helpers take exactly one; cross-slot ops use ascending order. Build-time enforcement via a tiny static analyzer is overkill here — code review + the phased rollout suffice.
- **Performance regression** from rwlock overhead on the hot read path. Mitigate: benchmark after each phase. Revert any phase that tanks throughput.
- **Module API** (`moduleNotifyKeyspaceEvent`) calls module-defined code. Module callbacks are not guaranteed reentrant w.r.t. `db->keys`. Likely solution: worker queues notifications, main thread fires them. Decide during audit.
- **AOF / replication** generate side effects (writes to the AOF buffer, command propagation to slaves). Single-threaded today; cannot be safely called from a worker. Worker must enqueue these for the main thread.
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

Phase 4 audit lands on `aqueduct-thread-migration` as a docs-only commit. Implementing PR lands on `aqueduct-recipient-apply-thread` off the audit's tip commit.

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
