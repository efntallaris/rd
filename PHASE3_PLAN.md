# Phase 3 Plan — RDMA reshard ownership flip

## Where Phase 2 left us

After `RDMA MIGRATE-PREP` + `RDMA RESHARD` + `RDMA RESHARD-EXEC`:
- The recipient's r_allocator slot blocks contain the source's slot data (verified byte-for-byte against `cluster-rdma-reshard-debug-bytes`).
- The recipient's `rdmaDoneSlotsCommand` has applied those bytes into its keyspace via `dbAdd`.
- **But the cluster slot map is unchanged.** `CLUSTER SHARDS` / `CLUSTER NODES` on every node still says the source owns the slots — every client request for those keys still routes to the source; the recipient's copy is invisible.

Phase 3 = atomically transfer ownership of the migrated slots from source to recipient so clients actually start hitting the recipient.

## Goal

After Phase 3, the sequence used in `reshard_cluster_rdma.yml` becomes:

```
RDMA MIGRATE-PREP   <recipient> <port> <slot> [<slot> ...]
RDMA RESHARD        <recipient> <port> <n-slots>
RDMA RESHARD-PRE    <recipient> <port> <n-slots>      ← NEW: SETSLOT MIGRATING + IMPORTING
RDMA RESHARD-EXEC   <recipient> <port> <n-slots>      (Phase 2, unchanged)
RDMA RESHARD-COMMIT <recipient> <port> <n-slots>      ← NEW: SETSLOT NODE + broadcast
```

After `RESHARD-COMMIT` returns, the cluster's authoritative routing for those N slots is the recipient. A client request hitting any master gets a `MOVED <slot> <recipient-host:port>` redirect.

**What Phase 3 does NOT solve** (deferred to Phase 4): concurrent writes that hit a migrating slot during the data-transfer window. With the SETSLOT MIGRATING/IMPORTING + ASK-redirect machinery in place, *non-existent-key* writes route correctly. But writes that hit a *pre-existing key in the source* during the window go to the source, the recipient's snapshot misses them, and after COMMIT those updates are lost. Standard upstream MIGRATE doesn't have this problem because it deletes each key from the source as it transfers it; our bulk RDMA path keeps the source copy until COMMIT, so the window is wider.

## Surface

Two new source-side subcommands, mirroring the args of `RDMA RESHARD-EXEC`:

```
RDMA RESHARD-PRE    <recipient-host> <recipient-port> <n-slots>
RDMA RESHARD-COMMIT <recipient-host> <recipient-port> <n-slots>
```

- Picked slots: same algorithm as `rdmaReshardCommand` / `rdmaReshardExecCommand` — walk self's owned slots, lowest first, take N.
- Precondition for PRE: `RDMA RESHARD` already registered the source MRs (so the slot list is stable across PRE/EXEC/COMMIT for this call).
- Precondition for COMMIT: PRE must have been called (slot must be in `MIGRATING` state). Optionally also require EXEC to have completed, but enforcing that requires extra per-slot state.
- Reply on each: `+OK <n> slots <pre|committed> (slots A..B)`.

## Handler behavior

### `rdmaReshardPreCommand` (~80 lines)

1. Parse args (host, port, n_slots) — verbatim from existing `rdmaReshardExecCommand`.
2. Pick N lowest owned slots.
3. Look up `L = dictFetchValue(server.rdma_outbound_links, "host:port")`. Look up the recipient's cluster node-id from `L->ctrl` via `CLUSTER MYID` (cache after first use).
4. For each chosen slot:
   - **Local**: call the existing internal SETSLOT MIGRATING helper from `cluster_legacy.c` (probably `clusterSetSlot` — see "Risks" below). Sets `server.cluster->migrating_slots_to[slot] = recipient_node`.
   - **Remote (recipient)**: send `CLUSTER SETSLOT <slot> IMPORTING <source-node-id>` over `L->ctrl`. Source's own node id is `server.cluster->myself->name`.
5. Reply summary.

### `rdmaReshardCommitCommand` (~120 lines)

1. Parse args + pick slots (same as PRE).
2. Look up L + recipient node-id (must be cached or re-fetched).
3. For each chosen slot:
   - **Local**: clear MIGRATING state, set NODE pointer to recipient via the internal `clusterSetSlot` helper.
   - **Remote (recipient)**: `CLUSTER SETSLOT <slot> NODE <recipient-node-id>` over `L->ctrl`. This clears IMPORTING and makes recipient the owner from its own perspective.
4. **Broadcast**: for each *other* master in the cluster (`server.cluster->nodes` iterated, skipping self and recipient), send `CLUSTER SETSLOT <slot> NODE <recipient-node-id>`. Use a fresh `redisConnect`+`redisCommandArgv` per peer (no cached `L->ctrl` to them). Failures are warnings — gossip will propagate eventually.
5. After all SETSLOTs land, bump local cluster epoch (`clusterBumpConfigEpochWithoutConsensus`) so peers accept the change without consensus.
6. Persist cluster state via `clusterSaveConfigOrDie(1)`.
7. Reply summary with timing.

## Critical files

| File | Change |
|---|---|
| `src/cluster_rdma.c` | Add `rdmaReshardPreCommand` + `rdmaReshardCommitCommand` after `rdmaReshardExecCommand`. ~200 lines total. Reuse the slot-walk + argv-build patterns already established. |
| `src/server.h` | Add prototypes `void rdmaReshardPreCommand(client *c);` and `void rdmaReshardCommitCommand(client *c);` next to `rdmaReshardExecCommand`. |
| `src/commands/rdma-reshard-pre.json` | New file. Container=RDMA, function=`rdmaReshardPreCommand`, arity=5, mirror `rdma-reshard-exec.json`. |
| `src/commands/rdma-reshard-commit.json` | Same, function=`rdmaReshardCommitCommand`. |
| `src/cluster_legacy.c` (read-only inspect) | Locate the internal SETSLOT helper that `clusterCommand` uses. Probably `clusterSetSlot` or similar. Don't modify — just call from cluster_rdma.c. |
| `src/cluster.h` | Add `sds recipient_id;` field to `rdmaOutboundLink` to cache the recipient's cluster node id (set lazily on first PRE call). |
| `ansible/tasks/cluster/reshard_cluster_rdma.yml` | Insert PRE step between RESHARD and EXEC, COMMIT step after EXEC. Pattern mirrors the existing EXEC task block. |

## Reused pieces (do not re-implement)

- `clusterLookupNodeByName` / `server.cluster->myself` / `server.cluster->nodes` — for resolving node ids and iterating peers.
- `redisCommandArgv` over `L->ctrl` — same pattern as `rdmaMigratePrepCommand`'s REGISTER-BLOCK-SLOTS round-trip.
- `clusterBumpConfigEpochWithoutConsensus`, `clusterSaveConfigOrDie` — already used elsewhere in cluster_legacy.c for SETSLOT NODE handling.
- Slot-walk + lookup-L + atomicity-check pattern — copy directly from `rdmaReshardExecCommand`.
- `cluster-rdma-reshard-debug-bytes` (existing) — repurpose: when on, also log per-slot SETSLOT transitions (`"RDMA RESHARD-PRE: slot=X local=MIGRATING remote=IMPORTING ok"`) so we can trace ownership flips.

## What changes in the ansible reshard_cluster_rdma.yml

Insert two new shell tasks. PRE goes between RESHARD and EXEC; COMMIT goes right after EXEC. Same `redis-cli RDMA RESHARD-{PRE,COMMIT} {{ reshard_target }} {{ master_port }} {{ reshard_slots_per_source }}` shape.

Order after Phase 3:

```yaml
- RDMA MIGRATE-PREP <recipient> <port> <slot ids>     # existing
- RDMA RESHARD      <recipient> <port> <n>            # existing
- RDMA RESHARD-PRE  <recipient> <port> <n>            # NEW (Phase 3)
- RDMA RESHARD-EXEC <recipient> <port> <n>            # existing (Phase 2)
- RDMA RESHARD-COMMIT <recipient> <port> <n>          # NEW (Phase 3)
```

## Verification

1. **Build clean**: `cd /users/entall/rd/redis && sudo make -j$(nproc)`. Expect two new symbols in `nm`: `rdmaReshardPreCommand`, `rdmaReshardCommitCommand`. New entries in `commands.def`.

2. **Smoke (one source, 10 slots)** — using the existing YCSB-string workload + cluster bring-up via custom_reshard's setup:

   ```
   redis-cli -p 8000 RDMA MIGRATE-PREP redis3 8000 0 1 2 3 4 5 6 7 8 9
   redis-cli -p 8000 RDMA RESHARD      redis3 8000 10
   redis-cli -p 8000 RDMA RESHARD-PRE  redis3 8000 10
   # at this point CLUSTER NODES on redis0 should show slots 0..9 with
   # '[->-recipient_id]' (migrating), and on redis3 show '[<-source_id]'
   redis-cli -p 8000 RDMA RESHARD-EXEC redis3 8000 10
   redis-cli -p 8000 RDMA RESHARD-COMMIT redis3 8000 10
   # after this:
   #   redis-cli -p 8000 CLUSTER SLOTS  shows redis3 as owner of 0..9
   #   redis-cli -p 8000 GET foo (foo in slot 12345) → still served by redis0
   #   redis-cli -p 8000 GET <any-key-in-slot-0..9> → returns MOVED to redis3
   ```

3. **Full custom_reshard sweep**: after wiring the ansible task, run `bash run_custom.sh workloada` with the YCSB-string workload. Expect:
   - YCSB run phase issues MOVED redirects as slots flip during the reshard window. Jedis cluster client follows them. No `WRONGTYPE` / unhandled-`MOVED` errors should reach YCSB's error count.
   - End-of-sweep `DBSIZE` on redis3 ≈ (records loaded × N_reshard'd_slots / total_slots), matching the proportion of records that landed in the moved slot ranges.
   - `CLUSTER SLOTS` on any redis_master shows redis3 as the owner of slots `[0..1364]`, `[5461..6825]`, `[10923..12287]` (each source's first 1365 slots, per current `reshard_slots_per_source=1365`).

4. **Compare apples-to-apples to vanilla_reshard** — re-run the full 2×2 YCSB sweep (`bash run_all_experiments.sh`). With Phase 3 in place, `custom_reshard` now does the same logical work as `vanilla_reshard` (moves data + flips ownership). Expected results:
   - Custom throughput drop during reshard ≪ vanilla's 35% drop (RDMA-WRITE is off the source's main thread; the SETSLOT calls are quick metadata ops).
   - Custom tail latency ≪ vanilla's 3+ sec max (no per-key MIGRATE blocking the event loop).
   - Recipient ends up with the actual key counts for the migrated slots (verifiable via DBSIZE on redis3).

5. **Idempotency / safety**:
   - Calling COMMIT without PRE → error ("slot X not in MIGRATING state").
   - Calling PRE twice in a row → second call is a no-op (already MIGRATING) with a warning log; not an error.
   - PRE + EXEC + cluster restart before COMMIT → on startup, source replays cluster config; slots remain in MIGRATING state. Operator can re-run COMMIT.

## Suggested commit shape (3 commits)

1. `rdma_migration: add RDMA RESHARD-PRE (Phase 3, SETSLOT MIGRATING+IMPORTING)` — handler + JSON + server.h proto + commands.def regeneration.
2. `rdma_migration: add RDMA RESHARD-COMMIT (Phase 3, SETSLOT NODE + broadcast)` — handler + JSON + proto + commands.def.
3. `ansible: wire RDMA RESHARD-PRE and -COMMIT into custom_reshard task` — two extra shell blocks in `reshard_cluster_rdma.yml`.

## Risk / open questions to think about before starting

- **Internal SETSLOT helper signature**: confirm exactly which function in `cluster_legacy.c` to call from `cluster_rdma.c` to do the local MIGRATING/NODE transitions without going through the command dispatcher. Candidates: `clusterSetSlot`, `clusterUpdateSlotsConfigWith`. Lock semantics matter — most cluster state mutations expect the main thread (we are on it; OK).
- **Recipient node id resolution**: how to get the recipient's cluster node id from the source. Easiest is `CLUSTER MYID` over `L->ctrl` once and cache. Add a `sds recipient_id` field to `rdmaOutboundLink` (set lazily on first use in PRE).
- **Broadcast cost**: if the cluster has many masters, COMMIT does `N_slots × N_other_masters` round-trips. For N=1365 slots × 3 other masters, that's ~4K RPCs serially. Optimisation: batch each peer with `CLUSTER SETSLOT <s> NODE <id>` for many slots via pipelined hiredis. Worth it if measurements show this dominates COMMIT time.
- **Source's own data after COMMIT**: SETSLOT NODE on the source clears its ownership of those slots, but doesn't delete the keys. They sit there orphaned, taking memory until the next `r_allocator_free_kv` or `FLUSHSLOTS`-equivalent. Either: delete them in COMMIT (then EXEC failure becomes data loss), or leave them and rely on r_allocator cleanup at slot teardown. Choose explicitly.
- **CLUSTER FAILOVER during the window**: if the source goes down between PRE and COMMIT, slots stay in MIGRATING state on whatever replicas exist (or get orphaned). Acceptable for a research prototype; document as a known sharp edge.

If you spend more than 60 minutes stuck on the internal SETSLOT plumbing (the cluster_legacy.c side), fall back to issuing `redis-cli -p 8000 CLUSTER SETSLOT <slot> MIGRATING <id>` against `localhost` from within the handler via hiredis — slower but it's the same RPC the operator would do manually, with known-correct semantics.
