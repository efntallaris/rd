# Plan — Make `r_allocator` the primary store for OBJ_STRING dict entries

## Context

Today the aqueduct fork does a **shadow write**: every `dbAdd` of an OBJ_STRING value into a cluster-mode keyspace writes the (key, value) into both jemalloc-backed redis structures (the persistent home) *and* a slot-keyed `r_allocator` block (a secondary copy used for RDMA migration). The cost is doubled — 2× memory, 2× per-insert work.

The cluster-rdma-allocator-shadow flag we just enabled for custom variants exposes this cost on every measurement.

**Goal:** When `cluster-rdma-allocator-shadow=yes`, make `r_allocator` the **primary** store for OBJ_STRING key/value bytes. The dict still maps slot-hash → kvobj as today, but the kvobj's `ptr` (the value sds) lives inside an `r_allocator` block instead of jemalloc. Jemalloc is no longer used for the persistent storage of those bytes — only for the dict skeleton and the robj header machinery that the rest of redis already touches.

Phase 2 of RDMA RESHARD (the data-transfer step) can then RDMA-WRITE the r_allocator block directly with no encode/copy detour, which is the real payoff.

Scope decisions (locked):
- **String values only.** Lists / hashes / sets / zsets keep their existing jemalloc-backed internals.
- **Gated by `cluster-rdma-allocator-shadow`.** When the flag is off, today's pure-jemalloc path runs verbatim — vanilla variants keep working from the same binary.
- **Ownership marked via `OBJ_ENCODING_R_ALLOCATOR`** — one new encoding constant, checked at the free sites.

## What stays jemalloc-backed

- Dict structure: bucket array, dictEntry nodes — too pervasive to move; gain is small.
- robj headers themselves *for the input objects from command parsing* — transient, freed once the persistent r_allocator-backed robj takes over inside dbAddInternal.
- All non-OBJ_STRING types: lists, hashes, sets, zsets, streams.
- Server-side metadata: client buffers, latency histograms, etc.

What moves into r_allocator:
- The **persistent** robj header for the dict's key (`ptr_key_meta` returned by `r_allocator_insert_kv`)
- The **persistent** robj header for the dict's value (`ptr_val_meta`)
- The key sds (header + bytes)
- The value sds (header + bytes)

## Critical files

All paths under `/users/entall/rd/redis/`.

| File | Change |
|---|---|
| `src/server.h` | **add** `#define OBJ_ENCODING_R_ALLOCATOR 16` next to the existing OBJ_ENCODING_* family. |
| `src/rdma_migration/allocator.c` (around line 581 — body of `r_allocator_insert_kv`) | **change payload layout**: when writing the key and value into the block, lay them out *with* sds headers (so `ptr->ptr` is usable directly by `sdslen()` and friends). Set `ptr_key_meta->encoding = OBJ_ENCODING_R_ALLOCATOR` and `ptr_val_meta->encoding = OBJ_ENCODING_R_ALLOCATOR`. Set `ptr_key_meta->ptr` / `ptr_val_meta->ptr` to point past the sds header into the bytes (so existing sds accessors work without modification). |
| `src/rdma_migration/include/rdma_migration.h` *(or allocator.h)* | **add accessor** `void *r_allocator_seg_for_robj(robj *o);` that, given a robj inside a block, returns its segment-header pointer so `r_allocator_free_kv` can be called on it. Implementation walks back from the robj address using known offsets from the block layout (see allocator.c comment near `r_allocator_insert_kv`). |
| `src/object.c` `freeStringObject()` (~line 524) | **intercept**: if `o->encoding == OBJ_ENCODING_R_ALLOCATOR`, call `r_allocator_free_kv(slot, r_allocator_seg_for_robj(o))` and **return** — do NOT call `sdsfree(o->ptr)` (would crash; sdsfree assumes jemalloc). Slot is recoverable from the segment metadata or from a parallel call site that passes it. |
| `src/object.c` `decrRefCount()` (~line 603) | **intercept**: when refcount drops to 0 and the type-specific free runs `freeStringObject`, **skip the trailing `zfree(alloc)`** if the robj's encoding is `OBJ_ENCODING_R_ALLOCATOR` (the robj memory is inside the segment that `freeStringObject` just freed via r_allocator_free_kv — calling zfree on it would corrupt the allocator). |
| `src/db.c` `dbAddInternal()` (lines 412–447) | **swap-in path** under `server.rdma_allocator_shadow && server.cluster_enabled && val->type == OBJ_STRING && val->ptr != NULL`: after r_allocator_insert_kv returns `ptr_key_meta` / `ptr_val_meta`, use **those** as the kvobj that goes into `kvstoreDictSetAtLink` (line 447) — not the jemalloc-allocated input. `decrRefCount` the input key/val robjs so their transient jemalloc allocations are freed. |
| `src/db.c` `dbGenericSet()` overwrite path (~line 678) | Make overwrite call `r_allocator_free_kv` on the **old** value before kvobjSet replaces it. Without this, every UPDATE leaks a segment. |
| `src/db.c` `dbDelete` family | Confirm decrRefCount path runs cleanly — the freeStringObject intercept above should handle it. |

## Behavior matrix

| Config | What runs |
|---|---|
| `shadow=yes && cluster && OBJ_STRING` | r_allocator-primary path: kvobj lives in registered block, dict entry points at it, freeStringObject routes to r_allocator_free_kv. |
| `shadow=yes` but non-string | Existing jemalloc path (no shadow either — current code skips non-strings). |
| `shadow=no` | Existing jemalloc path verbatim. No r_allocator activity. |
| Non-cluster mode | Existing jemalloc path verbatim. |

## sds-layout change inside the block

This is the single hardest part. The current `r_allocator_insert_kv` writes the key as `<u32 size><raw bytes>` (no sds header), same for value. To make `robj->ptr` usable as a normal sds, the block needs:

```
... <ptr_val_meta robj> <sds-header for value> <value bytes> <\0> ...
                          ^                    ^
                          |                    +- ptr_val_meta->ptr points HERE
                          +- header is what sds.h's macros look at via s[-1]
```

Use the smallest viable sds header type (`sdshdr5` or `sdshdr8`, depending on payload size) — match what `sdsnewlen()` would choose. Reuse the size-class logic from `sds.c` so the rest of redis can't tell the difference between a jemalloc sds and an r_allocator sds.

Affected code in allocator.c: the helpers `add_data_with_header` / `add_data_no_header` near the inline writes inside `r_allocator_insert_kv`. The `data_offset` field in the inline robj should be set to point at the byte AFTER the sds header.

## Reused pieces (do not re-implement)

- `r_allocator_insert_kv` body (allocator.c:581) — already does the segment allocation, freelist bookkeeping, and slot mutex. We're only changing the payload write inside it.
- `r_allocator_free_kv` (allocator.c:737) — already fully implemented. Currently never called; this plan makes it the primary free path.
- The slot-level mutex inside `r_allocator` (allocator.c:617) — already protects insert; same lock will protect free, since `r_allocator_free_kv` takes it too (verify when wiring).
- `sdshdr5` / `sdshdr8` types and the `SDS_TYPE_*` flags in `src/sds.h` — reuse for layout in the block.
- `kvobjSet` / `kvstoreDictSetAtLink` in `src/db.c` and `src/kvstore.c` — they only care that the `ptr` they're given is a valid sds-shaped buffer; they don't introspect allocation origin.

## Out of scope (later)

- **All non-OBJ_STRING types** (lists / hashes / sets / zsets): they still go through jemalloc.
- **AOF rewrite / RDB save reading r_allocator-backed sds**: existing code paths use sdslen + raw byte copy (e.g., rdbSaveStringObject reads via sdslen → memcpy). Those work unchanged because the sds layout in the block is sds-shaped. Verify experimentally — if anything calls `zfree` on the buffer (rdb has a `sdsfree` pattern in some helpers), patch via the same encoding check.
- **Module API (RM_StringDMA etc.)**: similar — returns ptr + sdslen. Works as long as the buffer is sds-shaped. Patch only if a specific module breaks.
- **LRU / LFU eviction**: the robj's `lru` field still works; we're not repurposing it. Eviction → decrRefCount → freeStringObject (intercept) → r_allocator_free_kv. Should "just work" once the free path is wired.
- **I/O threads / bio (lazyfree) threads**: bio's freeObjAsync ultimately decrRefCounts on a worker thread. `r_allocator_free_kv` takes the slot mutex, so calling it from any thread is safe; verify no main-thread-only assumption was added downstream.
- **Phase 2 of RDMA RESHARD (data transfer)**: with this change in place, Phase 2 becomes trivial — the source already has the slot's keys laid out in registered memory; just `ibv_post_send` an RDMA-WRITE of the relevant block to the recipient. No encode step.

## Implementation order (3 commits to keep history readable)

1. **`object: add OBJ_ENCODING_R_ALLOCATOR + intercept in freeStringObject / decrRefCount`** — changes confined to server.h + object.c. With nothing producing the new encoding yet, this is a no-op at runtime.
2. **`rdma_migration: lay out keys+values as sds inside allocator blocks, set ENCODING_R_ALLOCATOR`** — changes `r_allocator_insert_kv` body to write sds-shaped payload and set the encoding on the inline robjs. Adds `r_allocator_seg_for_robj`. Still no caller change, so still a no-op end-to-end.
3. **`db: swap to r_allocator-backed kvobj when shadow=yes (string only)`** — modifies `dbAddInternal` and `dbGenericSet`'s overwrite branch to use `ptr_val_meta` returned by `r_allocator_insert_kv` as the dict's kvobj, decrRefCount the jemalloc inputs, and call `r_allocator_free_kv` on overwrite. This is the commit that flips the behavior.

Splitting it this way means each commit individually compiles + leaves behavior unchanged or strictly forward-compatible, and a bisect can pinpoint which step broke if something does.

## Verification

1. **Build clean**: `cd /users/entall/rd/redis && sudo make -j$(nproc)`.
2. **Basic operation** (no shadow): start one redis-server with default config (`shadow=no`); SET / GET / DEL a key. Expect normal behavior, no `r_allocator` log lines beyond init.
3. **Basic operation** (shadow on): restart with `--cluster-rdma-allocator-shadow yes --cluster-enabled yes`; SET k v; GET k → "v"; OBJECT ENCODING k → expect either "embstr"/"raw" if encoding falls back, OR "r_allocator" if we expose the new name. The point is the GET round-trip works.
4. **Overwrite test**: SET k v1; SET k v2; GET k → "v2". The r_allocator stats (logged at startup, or via a new INFO section if we add one) should show the v1 segment as freed.
5. **Delete test**: SET k v; DEL k; GET k → nil. Segment freed.
6. **AOF replay**: `BGREWRITEAOF`; restart the server; data should still be there. The AOF reader doesn't care that the original buffer was r_allocator-backed (the rewrite serializes via raw bytes).
7. **RDB save/load**: `BGSAVE`; kill -9; restart; data intact.
8. **YCSB workloada at smoke scale** (10k records / 20k ops via `/rd/workloads/workloada_smoke`): run custom_baseline; expect no crashes. Compare throughput vs the previous custom_baseline run (which double-wrote). Should be **higher** now since each SET only does one write of the bytes instead of two.
9. **VmPin** on the redis-server proc: should bump as keys are inserted (since r_allocator blocks are registered). Compare to the previous shadow=yes runs where VmPin only bumped during `RDMA RESHARD` calls.
10. **Compare apples-to-apples to vanilla_baseline at the same scale**: confirm vanilla still runs (same binary, shadow flag absent → jemalloc path). Throughput numbers should be in the same ballpark or better; latency similar.
11. **Cross-check with `RDMA RESHARD` Phase 1**: the source's `RDMA RESHARD` now returns slot blocks that already contain the live data — no need to encode anything for Phase 2. That's the structural payoff.

## Risk / open questions to think about before starting

- **Concurrent decrRefCount on the bio (lazyfree) thread** vs. main-thread inserts on the same slot: `r_allocator_free_kv` takes the slot mutex (confirm in the implementation at allocator.c:737+). If yes, fine.
- **sds inside r_allocator must use `SDS_TYPE_*` from sds.h verbatim** — any drift means sdslen returns wrong values. Test by reading `sdslen` of a value right after `r_allocator_insert_kv` returns and asserting equality with the input length.
- **kvobjSet branch logic at db.c:445** — there's a branch ("reuse val->ptr if refcount==1, dup if RAW encoding, panic otherwise"). The r_allocator-backed robj has its own encoding (OBJ_ENCODING_R_ALLOCATOR), which won't hit either branch. Need to add a case for it (just use the ptr directly, no copy).
- **Module API breakage**: if any loaded module's code path frees a returned sds via `zfree` directly (rather than via the documented free APIs), it'll crash. Audit by grepping `src/module.c` for `zfree` near string return paths before shipping.
- **The "insert only, no free" smoke comment** at db.c:421 needs to be **removed** in commit 3 — overwrites and deletes now free correctly via the intercept.

If you spend more than 60 minutes stuck on the sds-layout change (commit 2), drop the encoding marker into the inline robj's `lru` field instead of the encoding field — there are 8 unused bits in robj you can hijack — and proceed; come back to layout fidelity later.
