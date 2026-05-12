# Phase 2 Plan — RDMA reshard data transfer

## Where Phase 1 left us

`RDMA RESHARD <recipient-host> <recipient-port> <n-slots>` on each source master now:
1. Bootstraps an RDMA control link to the recipient (via the prior `RDMA MIGRATE-PREP` it depends on),
2. Allocates a 1 MiB staging buffer per slot, registers each via `ibv_reg_mr`, caches the `rdmamig_buffer *` in `L->source_buffers[slot]`.

The recipient has already done its symmetric step during `MIGRATE-PREP`: allocated 1 MiB landing buffers per slot, registered them, returned `(VA, rkey)` tuples that are cached in `L->buffers[slot]`.

So after Phase 1, on the source side we hold for each slot:
- A locally-registered **staging buffer** (`L->source_buffers[slot]->buffer`, length `RDMAMIG_BLOCK_SIZE_BYTES`)
- The recipient's **landing VA + rkey** (`L->buffers[slot].ptr`, `L->buffers[slot].rkey`)

Phase 2 = fill the staging buffer with the slot's key data and RDMA-WRITE it to the recipient.

## Goal

Add the data-plane step that takes a slot whose source MR is already registered (Phase 1) and ships its contents to the recipient. After Phase 2, calling the sequence on a source master:

```
RDMA MIGRATE-PREP redis3 8000 0          # bootstrap link (already exists)
RDMA RESHARD     redis3 8000 1365        # register source MRs (Phase 1)
RDMA RESHARD-EXEC redis3 8000 1365       # NEW: encode + RDMA WRITE + DONE-SLOTS for those 1365 slots
```

results in 1365 slots' worth of key data physically landed in the recipient's memory and applied into its keyspace via the existing `RDMA DONE-SLOTS` recipient handler.

What Phase 2 **does not do**: flip slot ownership via `CLUSTER SETSLOT NODE`. The data lands but the cluster topology still says the source owns the slots. The recipient's `rdmaDoneSlotsCommand` already does `dbAdd` for the keys, but without the ownership flip the recipient's keys aren't reachable via redirects from clients. That's Phase 3.

## Surface

```
RDMA RESHARD-EXEC <recipient-host> <recipient-port> <n-slots>
```

- Subcommand of the existing `RDMA` container.
- Source-side. Operates on the same N slots that the previous `RDMA RESHARD` registered (picked the same way: walk self's owned slots, lowest first).
- **Precondition**: the corresponding `RDMA MIGRATE-PREP` + `RDMA RESHARD` must have already run. The command errors `-ERR slot X not pre-registered; call RDMA RESHARD first` if `L->source_buffers[slot] == NULL` for any slot in scope.
- **Reply**: `+OK <n> slots transferred (bytes <total>, errors <err>)`.

## Handler behavior (`rdmaReshardExecCommand`)

Lives in `src/cluster_rdma.c`, appended after `rdmaReshardCommand`. ~80 lines.

1. **Parse args** (same shape as `RDMA RESHARD`): host, port, n_slots.
2. **Identify the N slots**: same walk as `rdmaReshardCommand` — `server.cluster->slots[i] == server.cluster->myself`, lowest first, take N. Error on shortfall.
3. **Look up the cached link** `L = dictFetchValue(server.rdma_outbound_links, "host:port")`. Error if missing.
4. **Verify all chosen slots are pre-registered**: for each slot, require `L->source_buffers[slot] != NULL` AND `L->buffers[slot].ptr != 0`. If any is missing, error before doing any RDMA work (atomicity).
5. **Lock `L->mu`**.
6. **For each slot in order**:
   - `void *staging = rdmamig_buffer_data(L->source_buffers[slot]);` (accessor we'll add — see below)
   - `uint32_t n_entries = rdmaEncodeSlotEntries(c->db, slot, staging, RDMAMIG_BLOCK_SIZE_BYTES);` (existing function in cluster_rdma.c — already used by `rdmaTransferSlotsCommand`)
   - `int rc = rdmamig_client_post_write(L->source_buffers[slot], staging, L->buffers[slot].ptr, L->buffers[slot].rkey, RDMAMIG_BLOCK_SIZE_BYTES);`
   - `rdmamig_client_wait_send(L->client);` (blocks until completion)
   - Log per-slot: `serverLog(LL_NOTICE, "RDMA RESHARD-EXEC: slot=%d entries=%u rc=%d", slot, n_entries, rc);`
   - If rc != 0, increment err counter but keep going (mirrors existing TRANSFER-SLOTS pattern).
7. **After all slots written, notify the recipient via the cached `L->ctrl` hiredis channel**:
   - Build `RDMA DONE-SLOTS <s1> <s2> ... <sN>` argv via `redisCommandArgv`.
   - Send synchronously, parse `+OK`. Log on error but don't fail the whole call (the data is on the recipient even if the apply step fails — we want to know it landed).
8. **Unlock**, reply summary.

### One small piece of plumbing needed first

The `rdmamig_buffer` struct's `buffer` field is currently private (only the impl file `rdma_buffer.c` touches it). For Phase 2 we need to read it back to encode into. Two cleanest options:

**Option A** (preferred): add one public accessor to `src/rdma_migration/include/rdma_migration.h`:
```c
char *rdmamig_buffer_data(const rdmamig_buffer *b);
```
and implement in `src/rdma_migration/rdma_buffer.c`:
```c
char *rdmamig_buffer_data(const rdmamig_buffer *b) { return b->buffer; }
```
One-line addition; doesn't break encapsulation in any meaningful way (`rkey` is already exposed via `rdmamig_buffer_rkey`).

**Option B**: keep a parallel `void *source_staging[CLUSTER_SLOTS]` array in `rdmaOutboundLink` populated by Phase 1 at the same time as `source_buffers[]`. Slightly redundant but avoids the API change.

Go with Option A. It's cleaner.

## Critical files

| File | Change |
|---|---|
| `src/rdma_migration/include/rdma_migration.h` | **add prototype** `char *rdmamig_buffer_data(const rdmamig_buffer *b);` next to `rdmamig_buffer_rkey`. |
| `src/rdma_migration/rdma_buffer.c` | **add 1-line impl** of `rdmamig_buffer_data`. |
| `src/server.h` | **add prototype** `void rdmaReshardExecCommand(client *c);` next to `rdmaReshardCommand`. |
| `src/cluster_rdma.c` | **add handler** `rdmaReshardExecCommand` (~80 lines, logic above). Reuses `rdmaEncodeSlotEntries`, `rdmamig_client_post_write`, `rdmamig_client_wait_send`. |
| `src/commands/rdma-reshard-exec.json` | **new file**, container=RDMA, function=`rdmaReshardExecCommand`, arity=5, args mirror `rdma-reshard.json`. |

No changes to the recipient — `rdmaDoneSlotsCommand` already exists at `src/cluster_rdma.c:481` and does the right thing on receipt of `RDMA DONE-SLOTS <slot> ...`.

Also update the ansible task `tasks/cluster/reshard_cluster_rdma.yml` to call `RDMA RESHARD-EXEC` after `RDMA RESHARD` on each source. One extra `shell` task block, mirroring the existing one.

## Reused pieces (do not re-implement)

- `rdmaEncodeSlotEntries` — `src/cluster_rdma.c:200`, already used by `rdmaTransferSlotsCommand`. Format: `[u32 n_entries] [u32 keylen][key] [u32 vallen][val] …`. The recipient's `rdmaApplySlot` knows how to decode it.
- `rdmamig_client_post_write` / `rdmamig_client_wait_send` — `src/rdma_migration/rdma_client.c`.
- `rdmaDoneSlotsCommand` (recipient handler) — `src/cluster_rdma.c:481`, applies landed segments into the kvstore.
- The `rdmaOutboundLink` struct's existing `buffers[]` (recipient VA+rkey) and `source_buffers[]` (sender registered MRs) — both already in place.
- `redisCommandArgv` for the DONE-SLOTS round-trip — already used by `rdmaMigratePrepCommand`.

## Out of scope for Phase 2 (Phase 3+)

- **Slot ownership flip** via `CLUSTER SETSLOT NODE <node-id>` after the data lands. Until this happens, the recipient has the keys but the cluster still routes to the source. Phase 3 = orchestrate this per-slot, equivalent to what `redis-cli --cluster reshard` does at the tail of each slot migration.
- **MR teardown** — still leaks one staging MR per slot per call.
- **Multi-block per slot** — Phase 2 assumes ≤ 1 MiB of encoded data per slot. Slots with more keys will lose data past the truncation point. Phase 2.5 (or 3) can split into multiple blocks (the recipient's `REGISTER-BLOCK-SLOTS` already accepts `<slot> <nblocks>` pairs).
- **Concurrent writes during reshard** — Phase 2 writes once. Any YCSB updates that hit a slot AFTER `rdmaEncodeSlotEntries` snapshot but before ownership flip will be lost. Phase 3 needs to handle this via SETSLOT MIGRATING and ASK redirects (same machinery as upstream MIGRATE).

## Verification

1. **Build**: `cd /users/entall/rd/redis && sudo make -j$(nproc)`. Expect clean build; new subcommand in `commands.def`.
2. **Smoke (one source)** — bring up 3-master cluster + redis3 (recipient), load a tiny amount of data (use `/rd/workloads/workloada_smoke` — 10k records / 20k ops), then on redis0:
   ```
   redis-cli -p 8000 RDMA MIGRATE-PREP redis3 8000 0
   redis-cli -p 8000 RDMA RESHARD     redis3 8000 10
   redis-cli -p 8000 RDMA RESHARD-EXEC redis3 8000 10
   ```
   Expect `+OK 10 slots transferred (bytes N, errors 0)`. Source log should show 10 `RDMA RESHARD-EXEC: slot=X entries=Y rc=0` lines. Recipient log should show `RDMA DONE-SLOTS: applied K keys across 10 slots` (existing log line).
3. **Functional check** — `redis-cli -h redis3 -p 8000 DBSIZE` should show K keys (whatever was in those 10 slots) AFTER the EXEC call. Before the call: 0.
4. **Cluster-wide via the ansible task** — extend `tasks/cluster/reshard_cluster_rdma.yml` with a `RDMA RESHARD-EXEC` step right after the existing `RDMA RESHARD` step, then re-run the custom_reshard smoke with `redis_workload=workloada_smoke`. Expect all 3 source masters to transfer their 1365 slots; redis3 ends up with the union of all keys in those slot ranges.
5. **Compare to baseline** — at smoke scale, run vanilla_reshard side-by-side with custom_reshard (RDMA-backed); confirm the RDMA path completes faster on the data-move step. The reshard_cluster_rdma task logs the per-source wall time of `RDMA RESHARD-EXEC` — that's the apples-to-apples number to put next to vanilla's per-slot timing log.
6. **Idempotency / error paths** —
   - Calling `RDMA RESHARD-EXEC` without prior `RDMA RESHARD` → error.
   - Calling twice in a row → second call re-encodes + re-WRITEs the same data (acceptable for Phase 2; Phase 3 will track applied-state).
   - Recipient down between `RDMA RESHARD` and `RDMA RESHARD-EXEC` → `rdmamig_client_post_write` returns nonzero; log and continue, then the final DONE-SLOTS round-trip will surface a clearer error.

## Suggested commit shape

Three commits to keep history readable:
1. `rdma_migration: expose rdmamig_buffer_data accessor` — header + 1-line impl.
2. `rdma_migration: add RDMA RESHARD-EXEC (Phase 2, data transfer)` — new handler + JSON + server.h proto.
3. `ansible: wire RDMA RESHARD-EXEC into custom_reshard task` — adds the per-source EXEC call after RESHARD in `tasks/cluster/reshard_cluster_rdma.yml`.

## Risk / open questions to think about before starting

- **rdmaEncodeSlotEntries return semantics** — it currently writes from byte 0 of the buffer with a leading `u32 n_entries` count. Confirm the recipient's `rdmaApplySlot` is the matching decoder. If not, the existing `rdmaTransferSlotsCommand` is already using a different encoder/decoder pair that we should reuse.
- **Block size** — `RDMAMIG_BLOCK_SIZE_BYTES` is 1 MiB. With 50M records × ~120 bytes per entry encoded ≈ 6 GiB across 16384 slots ≈ 375 KiB/slot avg, so one block is fine for the AVG case but hot slots (zipfian) may overflow. Decide whether Phase 2 silently truncates or errors. Recommended: error + log slot id, so we can find the overflowers.
- **YCSB-during-RDMA write semantics** — Phase 1 currently runs `RDMA RESHARD` between `start_ycsb_async` and `wait_ycsb`. If YCSB writes a key DURING the encode-then-WRITE window, the recipient ends up with stale data. For Phase 2 smoke this is acceptable noise; Phase 3 must address it via ASK redirects.

If you spend more than 30 minutes stuck on any of these, surface them before continuing — they each have a reasonable cop-out (error loudly + defer the proper fix to Phase 3).
