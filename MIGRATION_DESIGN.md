# RDMA Migration Protocol Refactor — Working Design

> **Status:** WORK-IN-PROGRESS. The protocol is being walked through and implemented incrementally.
>
> **This is NOT a vanilla migration handler.** It is a refactor of the existing RDMA-based migration in the aqueduct fork (`redis/src/cluster_rdma.c` + `redisraft/`), replacing the ad-hoc phase coordination in `migrationWorker` with a clean Raft-logged message protocol on both donor and recipient. RDMA remains the bulk transport; hiredis-TCP remains the control plane (matching `rdmaOutboundLink.ctrl` in the existing design).

---

## Context

The aqueduct fork already does inter-cluster slot migration over RDMA. The flow is driven by `migrationWorker` in [redis/src/cluster_rdma.c](redis/src/cluster_rdma.c), which steps through PREP → REGISTERING → FLIPPING → TRANSFER → BACKPATCH → DONE. Coordination state is ad-hoc — held in worker-local variables, with hiredis control-channel RPCs (PREP, DONE-SLOTS, BACKPATCH-STATUS) but no consensus-logged history of "which session is in flight, in which phase."

The refactor adds a Raft-logged message protocol so:
- Every replica (donor + recipient) sees the same view of migration sessions through the Raft log.
- Phase transitions are explicit, durable, and recoverable.
- Recipient-side propagation of TRANSFER data uses chain replication for bandwidth offload.
- Migration coordination doesn't share the KV log's snapshot/retention.

The bulk data movement (RDMA-WRITE into pre-registered landing buffers, `DONE-SLOTS`, backpatch worker) is unchanged. We only add the protocol-logging layer around it.

---

## Message protocol (target)

| # | Side | Trigger | Log entry appended | Notes |
|---|---|---|---|---|
| 1 | Donor leader | Receives `RAFT.MIGRATE` from client/orchestrator | `MGN_TXN_START` (donor log) | Kicks off migration session. |
| 2 | Recipient leader | Receives `RAFT.MGN-PREP` from donor, prepares landing buffers + connection | `MGN_RECP_TXN_START` (recipient log) | Appended **before** replying OK to donor. |
| 3 | Donor → Recipient leader | RDMA-WRITE bulk data into recipient's landing buffer, then `DONE-SLOTS` control message | (no entry — RDMA primitive) | Recipient leader propagates to followers via chain. |
| 4 | Recipient leader | Majority of followers ack TRANSFER (matchIndex caught up via AppendEntries response) | `MGN_INDX_UPD` (recipient log) | Acknowledges majority durability of transferred data. |
| 5 | Recipient leader | (Overlap — not a log entry) | — | BACKPATCH apply on recipient leader runs in parallel with chain replication. |
| 6 | Recipient leader | Both: (a) BACKPATCH on leader complete, (b) chain majority has the data | `MGN_RECP_TXN_DONE` (recipient log) | Recipient signals donor "migration done." |
| 7 | Donor leader | Receives done-signal from recipient | `MGN_TXN_DONE` (donor log) | Closes out the session. |

---

## Implementation status

### Step 0 — Observability (COMPLETE)

Added `raftLogTypeName()` in [util.c](redisraft/src/util.c) and `LOG_NOTICE` lines at both append-time ([log.c:947-951](redisraft/src/log.c#L947-L951)) and apply-time ([raft.c:1026-1030](redisraft/src/raft.c#L1026-L1030)). Every Raft log entry on every replica now prints `(idx, term, type-name, size)` to the Redis log.

### Step 1a — `MGN_TXN_START` scaffolding (COMPLETE)

- New log type `RAFT_LOGTYPE_MGN_TXN_START` at [redisraft.h:589-595](redisraft/src/redisraft.h#L589-L595).
- `RAFT.MIGRATE <slot_start> <slot_end> <recipient_dbid>` command in [migrate.c](redisraft/src/migrate.c), registered in [redisraft.c](redisraft/src/redisraft.c) and [commands.c](redisraft/src/commands.c).
- Apply handler stub in [raft.c](redisraft/src/raft.c).
- Verification: [test_mgn_step1a.py](redisraft/tests/integration/test_mgn_step1a.py).

### Step 1b.1 — Recipient `RAFT.MGN-PREP` handler (COMPLETE)

- New log type `RAFT_LOGTYPE_MGN_RECP_TXN_START`.
- `RAFT.MGN-PREP <slot_start> <slot_end> <donor_dbid>` command, deferred-reply via `RaftReq` so client gets OK only after the leader applies.
- Verification: [test_mgn_step1b.py](redisraft/tests/integration/test_mgn_step1b.py).

### Step 1b.2 — Donor sends `RAFT.MGN-PREP` asynchronously (COMPLETE)

- `DonorSendMgnPrep()` + `MgnPrepDonorState` + four callbacks (`mgnPrepIdleCallback`, `mgnPrepFreeCallback`, `sendMgnPrepCommand`, `handleMgnPrepResponse`) in [migrate.c](redisraft/src/migrate.c).
- Donor's `MGN_TXN_START` apply handler, **leader only** (`raft_is_leader` guarded), looks up the recipient `ShardGroup` via `GetShardGroupById`, opens a hiredis-async connection, sends `RAFT.MGN-PREP`.
- Verification: [test_mgn_step1b2.py](redisraft/tests/integration/test_mgn_step1b2.py) — full two-cluster handshake works end-to-end.

---

## Open: Step 1c — TRANSFER integration

**Not** "donor enumerates keys + sends batched RPCs." TRANSFER in RDMA is bulk RDMA-WRITE into pre-registered landing buffers, followed by one `DONE-SLOTS` control message that enqueues the recipient's backpatch worker. Per-key fanout happens *inside* the recipient (backpatch worker iterates buffer, RESTOREs into dict) — never on the donor↔recipient wire.

The integration question is **which side drives**:
- (a) `RAFT.MIGRATE` (our new command) → its apply handler triggers the existing `migrationWorker` and walks through the existing RDMA phases, with the new protocol entries logged at each phase boundary.
- (b) Existing `RDMA MIGRATE` continues to drive `migrationWorker`; the worker simply also calls into the redisraft module to log `MGN_TXN_START` / `INDX_UPD` / `MGN_TXN_DONE` at the right points.

Decision pending. After that, 1c implements:
- Recipient leader appends `MGN_INDX_UPD` once chain-majority (or AppendEntries-majority pre-chain) has the TRANSFER data durably.
- Recipient leader overlaps BACKPATCH with chain replication.
- Recipient leader appends `MGN_RECP_TXN_DONE` once both BACKPATCH and chain-majority are done.
- Donor receives the done-signal and appends `MGN_TXN_DONE`.

---

## Still open / deferred

- Chain replication on the recipient side (currently followers receive migration entries via standard AppendEntries; chain is an optimization for later).
- Separate-log-space persistence (single AppendEntries channel with type tag is in place; optional split of on-disk files for migration entries is deferred).
- In-memory tombstone for post-FLIP deletes.
- Donor side write-rejection in MIGRATED state.
- Don't-clobber rule in the backpatch apply path.
- Failure / abort / resume paths.

---

## Reference files

- [redis/src/cluster_rdma.c](redis/src/cluster_rdma.c) — existing `migrationWorker` and RDMA primitives (PREP-RPC, RECV-FLIP, DONE-SLOTS, BACKPATCH-STATUS, backpatch worker thread).
- [redisraft/src/migrate.c](redisraft/src/migrate.c) — new commands and donor-side async helpers.
- [redisraft/src/raft.c](redisraft/src/raft.c) — apply cases for the new log types.
- [redisraft/src/redisraft.h](redisraft/src/redisraft.h) — `RAFT_LOGTYPE_MGN_*` constants and function declarations.
- [redisraft/src/util.c](redisraft/src/util.c) — `raftLogTypeName()`.
- [redisraft/src/log.c](redisraft/src/log.c), [redisraft/src/raft.c](redisraft/src/raft.c) — Step 0 observability.
- [redisraft/tests/integration/test_mgn_step1a.py](redisraft/tests/integration/test_mgn_step1a.py), [test_mgn_step1b.py](redisraft/tests/integration/test_mgn_step1b.py), [test_mgn_step1b2.py](redisraft/tests/integration/test_mgn_step1b2.py) — verification.

---

*Last updated: framing corrected from "vanilla migration handler" to "RDMA migration protocol refactor in the aqueduct fork." Steps 0, 1a, 1b complete; 1c integration with `cluster_rdma.c` is the next planning topic.*
