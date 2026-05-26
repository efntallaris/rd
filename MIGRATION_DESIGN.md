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

**Driving model — decision:** option (b). `migrationWorker` in `cluster_rdma.c` keeps driving the migration phases; at each phase boundary it calls `RAFT.MGN-LOG <type> <payload>` (via local hiredis to self) so the redisraft module records the event in the Raft log. Apply handlers on every replica are bookkeeping-only — they do not trigger the next phase. This means our short-lived 1a/1b commands (`RAFT.MIGRATE`, `RAFT.MGN-PREP`, `DonorSendMgnPrep` async send) were obsolete and have been **removed**.

### Step 0 — Observability (COMPLETE)

`raftLogTypeName()` in [util.c](redisraft/src/util.c) + `LOG_NOTICE` at append-time in [log.c](redisraft/src/log.c) `logImplAppend` and at apply-time in [raft.c](redisraft/src/raft.c) `raftApplyLog`. Every Raft log entry on every replica prints `(idx, term, type-name, size)` to the Redis log.

### Step 1 — Protocol log infrastructure (COMPLETE)

- Five `RAFT_LOGTYPE_MGN_*` constants in [redisraft.h](redisraft/src/redisraft.h): `MGN_TXN_START`, `MGN_RECP_TXN_START`, `MGN_INDX_UPD`, `MGN_RECP_TXN_DONE`, `MGN_TXN_DONE`.
- Single `RAFT.MGN-LOG <type> <payload>` command in [migrate.c](redisraft/src/migrate.c), registered in [redisraft.c](redisraft/src/redisraft.c) + [commands.c](redisraft/src/commands.c). Type is a string (`TXN_START` / `RECP_TXN_START` / `INDX_UPD` / `RECP_TXN_DONE` / `TXN_DONE`).
- Apply handlers in [raft.c](redisraft/src/raft.c): all five `MGN_*` cases collapsed into one bookkeeping branch that `LOG_NOTICE`s the payload and frees any req.
- Verification: [test_mgn_log.py](redisraft/tests/integration/test_mgn_log.py) — parametrized over all five types + unknown-type rejection. 6 tests, all pass.

### Step 1c.1 — Worker-thread instrumentation in `cluster_rdma.c` (COMPLETE)

- `rdmaMgnLogSync(type, payload)` helper in [cluster_rdma.c](redis/src/cluster_rdma.c) — synchronous hiredis to `127.0.0.1:server.port`. Safe from worker threads; not safe from main thread (would deadlock).
- **`MGN_TXN_START`** at the top of `migrationWorker` — payload `"sess=<id> slots=<first>-<last> n=<n_slots> recipient=<addr>"`.
- **`MGN_TXN_DONE`** on the success branch of the BACKPATCH-STATUS polling loop in `migrationWorker` — payload `"sess=<id>"`.
- **`MGN_RECP_TXN_START`** in `registerWorkerThread` at the `deliver:` label, before the dial-back, only on the no-error path — payload `"register_id=<id> slots=<first>-<last> n=<n_pairs> donor=<host>:<port>"`.
- Build: clean (redis-server v8.6.2 sha edd66c48).

### Step 1c.2 — Main-thread instrumentation (COMPLETE under no-chain assumption)

- Async hiredis adapter (`mgnRedisAe*` family) + `rdmaMgnLogAsync(type, payload)` helper in [cluster_rdma.c](redis/src/cluster_rdma.c) — lazy-attaches one shared `redisAsyncContext` to `server.el`, fire-and-forget.
- **`MGN_INDX_UPD`** at the `BACKPATCH_DONE` transition in `mergeBackpatchTick` (main thread) — payload `"sess=<src_mig_id> n_slots=<n> applied=<count>"`. Fires once per batch finishing backpatch on the recipient leader.
- **`MGN_RECP_TXN_DONE`** logged immediately after `MGN_INDX_UPD` at the same BACKPATCH_DONE site. Safe in the current no-chain model: `backpatch_batches_by_key` is keyed by `(src_node_id, src_mig_id)` → one batch per session, so per-batch done = per-session done. Once chain replication lands (Phase E), the two diverge: `MGN_INDX_UPD` per-batch chain-majority, `MGN_RECP_TXN_DONE` only after the last batch is both backpatched AND chain-majority-acked. Payload: `"sess=<id> applied=<n> clobber_skipped=<n>"`.

**Coverage: all 5 protocol log events instrumented.**

---

## Chain replication design (post-Step 1c discovery)

### The correctness gap chain replication closes

Investigation of the existing aqueduct showed that recipient-side **followers** never receive the migrated keys during a session. The data flow today:

1. Donor RDMA-WRITEs bulk data into the recipient **leader's** pre-registered landing buffers ([cluster_rdma.c](redis/src/cluster_rdma.c)).
2. Donor sends `DONE-SLOTS` to the recipient leader only (single `(host, port)` target).
3. Recipient leader's `mergeBackpatchTick` installs keys via direct `kvstoreDictAddRaw` ([cluster_rdma.c:1547](redis/src/cluster_rdma.c#L1547)) — bypassing Redis command propagation, bypassing the Raft log.
4. Followers see nothing: no command on the master→replica stream, no Raft entry carrying the keys, no `DONE-SLOTS` RPC. Their `recipientBackpatchThreadStart()` worker thread is running but has no input.

Result: post-migration, recipient followers have stale/missing keys. Only consistency path today is the next periodic snapshot.

**Chain replication is the mechanism the design uses to close this gap.**

### Chosen approach (locked in)

- **Transport:** RDMA-WRITE side-channel between recipient-cluster replicas (matches the donor→recipient leg's existing primitive).
- **Topology:** linear chain — `recipient_leader → F1 → F2 → ... → Fn` — order chosen by recipient leader at session start.
- **AppendEntries:** "wrapped body-less" — AE carries a tiny manifest (mig_id, batch_id, buffer_size, checksum), bulk data delivered via the chain out-of-band. Followers ack matchIndex only after they have both the AE manifest and the chain-delivered body. Avoids `deps/raft/` surgery; same operational outcome as strict body-less.
- **Self-healing:** transport timeout on a chain link → predecessor re-RDMA-WRITEs to the next live successor (skips dead node).

### Architecture overview

```
DONOR LEADER
   │  RDMA-WRITE bulk → recipient leader's landing buffer
   ▼
RECIPIENT LEADER ─── chain RDMA-WRITE ───▶ F1's landing buffer
                                              │
                                              │ chain RDMA-WRITE
                                              ▼
                                          F2's landing buffer
                                              │
                                              ▼
                                          Fn (chain tail)
   ▲
   │  body-less AE manifest (mig_id, batch_id, buffer_size, csum)
   └─────────────────────────▶  F1, F2, ..., Fn
                                 (followers wait for chain body before acking)
```

### Buffer registration (PREP extension)

Today, only the recipient leader registers landing buffers in response to donor's `REGISTER-BLOCK-SLOTS` RPC ([cluster_rdma.c:1680](redis/src/cluster_rdma.c#L1680) `registerWorkerThread`). For chain replication, **every recipient replica** needs an equivalent buffer:

- Recipient leader receives donor's PREP, registers its own landing buffer (as today).
- Recipient leader then issues a **new RPC** (`RDMA CHAIN-PREP`) to each follower, each follower registers its own landing buffer of identical layout, and replies with the buffer rkey + addr.
- Recipient leader assembles the chain (random permutation), computes the order, broadcasts the order via another RPC (`RDMA CHAIN-WIRE`): each follower learns its predecessor (for incoming RDMA-WRITEs) and its successor (for outgoing chain forwards).
- Recipient leader's reply to donor's PREP only completes once all `RDMA CHAIN-PREP` + `RDMA CHAIN-WIRE` round-trips with its followers have succeeded.

This means the existing `MGN_RECP_TXN_START` log entry should be appended **after** the chain is fully established on the recipient side — that's the protocol's notion of "recipient is ready."

### TRANSFER under chain replication

When the recipient leader receives `DONE-SLOTS` for a batch (donor finished its RDMA-WRITE into the leader's buffer):

1. Recipient leader RDMA-WRITEs the buffer contents into F1's landing buffer (same byte layout).
2. F1's "chain receive" handler completes (RDMA completion event) — F1 then RDMA-WRITEs into F2's buffer.
3. ... and so on to Fn.
4. Each link's RDMA completion event triggers a small TCP/hiredis "chain-ack" back to the recipient leader, so the leader knows which followers have the body.
5. Recipient leader appends `MGN_INDX_UPD` to its Raft log **only once chain-majority** (ceil((n+1)/2) replicas including itself) holds the body. The AE for this entry carries only the manifest; followers that don't yet have the chain-delivered body delay their matchIndex ack until both arrive.

### Backpatch on followers

Each follower's already-running `recipientBackpatchThreadStart()` is wired to consume from its own landing buffer (same way the leader's worker does today). The buffer descriptor is delivered to the follower as part of the chain-ack flow (so the follower's worker knows when a batch is ready to process). The existing `mergeBackpatchTick` `kvstoreDictAddRaw` installation logic is unchanged — each replica runs it locally on its own buffer.

### Backpatch ⟂ chain forward — parallelism + buffer lifetime

At **every** replica (leader and followers), once its landing buffer is filled, two operations fire **independently** from the same RDMA buffer-fill completion event:

1. **Local backpatch worker** drains the buffer into this replica's dict.
2. **Chain forwarder** RDMA-WRITEs the buffer to the next link (skipped at the chain tail).

These have no dependency on each other — the leader can finish its backpatch before F1 has received the first chain byte, and vice versa. This is what makes a session's wall-clock time ≈ `max(chain_propagation_time, backpatch_time)`, not their sum.

**Buffer lifetime is governed by a reference count.** Each buffer gets +1 for the local backpatch consumer and +1 for the outgoing chain forward (0 at the chain tail). The buffer's `ibv_reg_mr` registration is held until both decrement to 0; then the buffer can be unmapped / reused for the next batch.

### `MGN_INDX_UPD` and `MGN_RECP_TXN_DONE` semantics under the overlap

- **`MGN_INDX_UPD`** says "the bytes are durable across a chain-majority of replicas." It does **not** imply any replica has applied them. Appended as soon as chain-majority chain-acks arrive — independent of backpatch progress on any replica.

- **`MGN_RECP_TXN_DONE`** is appended on the recipient leader when **both** hold:
  - (a) The recipient leader's own backpatch worker has drained every batch for this session, AND
  - (b) Chain-majority has acked the **last** batch of the session.
  - Followers may still be backpatching when this fires. They're durable (chain-majority ensured) and they catch up on their own schedule. The leader's view of "done" closes the session.

### Self-healing chain

- Each chain link has a fixed timeout (e.g., 5s) on RDMA write completion.
- On timeout, the upstream node marks its downstream as dead, requests the recipient leader to re-issue `RDMA CHAIN-WIRE` with the dead node skipped, and resumes forwarding to the new downstream.
- Recipient leader keeps the live chain order; followers updated on every change.
- Dead node, on recovery, rejoins at chain tail via a fresh `RDMA CHAIN-PREP` + `RDMA CHAIN-WIRE` round-trip — but only between sessions.

### `MGN_RECP_TXN_DONE` under chain replication

Now naturally defined: recipient leader appends `MGN_RECP_TXN_DONE` when (a) its own backpatch worker has drained all queued batches for the session, AND (b) chain-majority of followers have acked done for the session's last batch. This closes the deferred item from Step 1c.2.

### Phase A — detailed plan (RDMA chain transport plumbing)

**Goal:** at the end of Phase A, the recipient leader can issue a test RDMA-WRITE through the chain `leader → F1 → F2 → ... → tail` and verify the same bytes appear at every replica's landing buffer. No actual migration data yet — that's Phase B.

#### A.1 — New file + organization

New file pair, separate from `cluster_rdma.c` so the chain code is auditable independently:
- `redis/src/cluster_rdma_chain.c` — implementation
- `redis/src/cluster_rdma_chain.h` — minimal public surface (one `chainPrepBuildFromPrep()` function for cluster_rdma.c's existing PREP handler to call, plus the new RPC command entry points so they can be wired into the Redis command table)

Reusable building blocks (no new infrastructure needed):
- `rdmamig_client_create` / `rdmamig_client_connect` / `rdmamig_client_post_write` / `rdmamig_client_wait_send` — generic RC QP operations; work for replica↔replica exactly the same way they work for donor↔leader today.
- `rdmamig_server_create` — listening endpoint; followers already have this (one per node, listening on `server.rdma_migration_port`).
- `rdmamig_buffer_create` — single big-MR pool registration; mirror the existing leader-side pattern for follower-side landing buffers.
- `rdmaMgnLogSync` / `rdmaMgnLogAsync` (just added) — for protocol log entries at chain phase boundaries.

#### A.2 — New data structures

```c
/* One per chain link the LEADER tracks (one entry per follower). */
struct rdmaChainPeer {
    sds host;                              /* follower's host */
    int port;                              /* follower's RDMA migration port */
    struct rdmamig_client *client;         /* RDMA QP to this follower */
    struct redisContext *ctrl;             /* hiredis TCP for chain control RPCs */
    struct rdmamig_buffer *peer_pool_buf;  /* what the FOLLOWER registered, peer-side metadata */
    uint64_t peer_pool_addr;               /* follower's landing-pool VA (advertised by follower) */
    uint32_t peer_pool_rkey;               /* follower's pool rkey */
    size_t   peer_pool_bytes;              /* follower's pool size */
    int      chain_position;               /* 0 = leader, 1 = F1, ... */
};

/* Per-session chain state on the LEADER. Lives for the session lifetime. */
struct rdmaLeaderChainState {
    long long src_mig_id;                  /* session id (matches mig->id) */
    int n_peers;                           /* followers in this chain */
    struct rdmaChainPeer peers[CLUSTER_NODES_MAX];   /* indexed by chain position - 1 */
    pthread_mutex_t mu;
};

/* Per-session chain state on a FOLLOWER. */
struct rdmaFollowerChainState {
    long long src_mig_id;
    int chain_position;                    /* my position; 1 .. n */
    int is_tail;                           /* 1 if I'm the last link */
    /* incoming: where my predecessor will RDMA-WRITE INTO */
    void *landing_pool;                    /* mmap'd */
    size_t landing_pool_bytes;
    struct rdmamig_buffer *landing_pool_buf;
    uint32_t landing_pool_rkey;            /* what I advertised to my predecessor */
    /* outgoing: only present if not tail */
    sds successor_host;
    int  successor_port;
    struct rdmamig_client *successor_client;
    struct redisContext *successor_ctrl;
    uint64_t successor_pool_addr;
    uint32_t successor_pool_rkey;
};
```

Both structs live in `cluster_rdma_chain.c` (file-static globals keyed by `src_mig_id`).

#### A.3 — New RPC wire formats (all over hiredis-TCP control plane)

**`RDMA CHAIN-PREP <src_mig_id> <pool_bytes>`** — leader → follower
- Recipient leader sends to each follower at the start of a session.
- Follower registers a landing pool of `pool_bytes` (mmap + `rdmamig_buffer_create`), returns its `(addr, rkey)` for the leader to advertise to the predecessor of the next link.
- Reply: `+CHAIN-PREP-OK <addr> <rkey>` (binary-safe).

**`RDMA CHAIN-WIRE <src_mig_id> <my_position> <n_peers> <pred_host> <pred_port> <succ_host> <succ_port> <succ_addr> <succ_rkey>`** — leader → follower
- Tells follower its chain position, total length, predecessor (informational), and successor.
- `<succ_host>=- <succ_port>=0 <succ_addr>=0 <succ_rkey>=0` if this follower is the chain tail.
- Follower establishes the outgoing RDMA QP to its successor (rdmamig_client_create + connect) if not tail.
- Reply: `+CHAIN-WIRE-OK`.

**`RDMA CHAIN-PING <src_mig_id> <expected_bytes>`** — leader → follower (Phase A verification only)
- Used **only** in Phase A end-to-end test: leader RDMA-WRITEs a known pattern through the chain; each follower verifies its landing buffer contains the expected pattern and replies `+CHAIN-PING-OK` or `-ERR mismatch`. Removed (or repurposed) in Phase B once real TRANSFER data flows.

#### A.4 — New handler functions

| Function | Where | Thread context |
|---|---|---|
| `rdmaChainPrepCommand` | `cluster_rdma_chain.c`, wired into Redis command table | Main thread |
| `rdmaChainPrepWorkerThread` | `cluster_rdma_chain.c` | Detached pthread (mirrors `registerWorkerThread` pattern — buffer registration off main thread) |
| `rdmaChainWireCommand` | `cluster_rdma_chain.c`, command table | Main thread |
| `rdmaChainWireWorkerThread` | `cluster_rdma_chain.c` | Detached pthread (RDMA QP setup is slow) |
| `rdmaChainPingCommand` | `cluster_rdma_chain.c`, command table | Main thread (verification only) |
| `leaderEstablishChain(rdmaMigration *mig)` | `cluster_rdma_chain.c`, called from inside `rdmaMigratePrepHelper` after the donor↔leader PREP succeeds | Worker pthread (migrationWorker, same as caller) |

`leaderEstablishChain` is the orchestration entry point. It:
1. Picks a random permutation of the local cluster's follower nodes (using `clusterNodes` enumeration).
2. For each follower in order: sends `CHAIN-PREP`, waits for `(addr, rkey)` in reply.
3. For each follower in order: sends `CHAIN-WIRE` with that follower's predecessor + successor info.
4. Stores the assembled `rdmaLeaderChainState` keyed by `src_mig_id`.
5. Returns success/failure to the donor↔leader PREP handler.

#### A.5 — Where to insert calls

- **`registerWorkerThread`** (recipient-side donor PREP handler, [cluster_rdma.c:1751](redis/src/cluster_rdma.c#L1751) deliver: label): **just before the dial-back** to donor, call `leaderEstablishChain(... )`. Only on the no-error path. If chain establishment fails, the dial-back reports the error to donor and the whole migration aborts before TRANSFER.
- The existing `MGN_RECP_TXN_START` log entry we added in 1c.1 then accurately reflects "recipient (and its chain) is ready" rather than only "leader's buffer is ready."

#### A.6 — Buffer sizing

For Phase A, the chain landing pool on every follower is **identical in size and layout to the leader's landing pool**. Same r_allocator strides, same number of slots. Followers compute the size from the `pool_bytes` arg in `CHAIN-PREP`. Phase B will rely on this to do byte-for-byte RDMA-WRITEs into matching offsets.

#### A.7 — Cluster topology lookup

The recipient leader needs to enumerate its own cluster's followers (to assemble the chain) — uses the existing `cluster.h` `clusterNodes` dict, filtering for nodes that are replicas of `myself` (`CLUSTER_NODE_SLAVE` whose `slaveof == myself`).

#### A.8 — Verification approach for Phase A

Two complementary checks:

1. **TCP control RPCs verifiable without RDMA.** A pytest test can:
   - Bring up a 3-node Redis cluster with redisraft loaded.
   - Manually issue `RDMA CHAIN-PREP <session_id> 1024` to two of the nodes (acting as followers) via redis-cli or the pytest harness.
   - Assert each replies `+CHAIN-PREP-OK <addr> <rkey>`.
   - Manually issue `RDMA CHAIN-WIRE ...` and assert OK.
   - Doesn't exercise actual RDMA but proves the control-plane flow + buffer registration code path. Catches arity / parse / state-machine bugs cheaply.

2. **End-to-end RDMA verification (needs hardware).** The `RDMA CHAIN-PING` command exists for this: leader writes a known pattern to its own landing buffer, RDMA-WRITEs through the chain, then issues `CHAIN-PING` to every follower; each follower checks its landing buffer against the expected pattern. You run this on your benchmark setup; I can't verify it without hardware.

#### A.9 — Phase A explicit non-goals

- No backpatch consumption of chain-delivered data (Phase C).
- No integration with `mergeBackpatchTick` (Phase C).
- No body-less AE manifest (Phase D).
- No self-healing on link failure — if a `CHAIN-PREP` or `CHAIN-WIRE` RPC fails, the whole migration aborts and the donor retries (Phase F adds repair).
- No reuse of existing `rdmaCachedConnection` — chain state is its own.

#### A.10 — Phase A open questions

- **rkey lifetime across multiple sessions:** can a follower's pool be reused for a second migration session, or do we re-register every time? For Phase A: re-register every time (simplest); optimize later if profiling shows overhead.
- **Inflight `CHAIN-PREP` count:** can the leader send to all followers concurrently or strictly serial? Strictly serial for Phase A (one less moving part); parallelize in Phase F if needed.
- **Failure semantics:** what if 2 of 3 followers reply OK and the 3rd times out? Phase A: whole-session abort (treat any error as fatal). Phase F adds tolerance.

#### A.11 — Phase A code-size estimate

~600–900 lines in `cluster_rdma_chain.c` (struct defs ~100, RPC handlers + workers ~300, `leaderEstablishChain` orchestrator ~150, cleanup paths ~100, debugging/logging ~100). ~50 lines insertion in `cluster_rdma.c` to call `leaderEstablishChain`. Roughly 2–4 days of focused implementation + however long RDMA debugging takes on the benchmark setup.

---

### Phase A.full discovery — rdmamig API ordering constraint

Implementing Phase A.full surfaced a **hard ordering constraint** in the `rdmamig` library that the original Phase A design didn't account for. The constraint shapes everything downstream.

#### The constraint

From [redis/src/rdma_migration/include/rdma_migration.h](redis/src/rdma_migration/include/rdma_migration.h):

```c
rdmamig_buffer *rdmamig_buffer_create(struct rdma_cm_id *id, char *buffer, size_t size, int access);
struct rdma_cm_id *rdmamig_server_cm_id(rdmamig_server *s);  /* NULL until a peer connects */
```

A buffer cannot be registered until a `cm_id` exists. The recipient's `rdmamig_server_cm_id()` returns `NULL` until a peer has actually connected via `rdmamig_client_connect()` on the other end. **Registration cannot happen before connection.**

The existing donor↔leader flow works because the donor connects first (`rdmamig_client_connect` during `rdmaMigratePrepHelper`), and *then* sends `REGISTER-BLOCK-SLOTS` over TCP — by which time the recipient's `server_cm_id` is set.

Our current Phase A skeleton's protocol does the opposite order:

```
SKELETON PROTOCOL (works without RDMA — fails on RDMA hardware):
  CHAIN-PREP  → follower mmaps pool, returns placeholder rkey=0  ← FAILS on real ibv_reg_mr
  CHAIN-WIRE  → follower stores succ info, no QP opened          ← never connects
```

#### Revised protocol (connect-then-register)

```
RDMA-CORRECT PROTOCOL:

  Step 1: Leader-to-follower QPs (cm_id bootstrap)
    For each follower:
      Leader calls rdmamig_client_create + rdmamig_client_connect to that follower.
      Follower's accept thread sets server_cm_id on first incoming connection.

  Step 2: CHAIN-PREP (now safe to register)
    Leader sends CHAIN-PREP over TCP. Follower can now call
    rdmamig_buffer_create(rdmamig_server_cm_id(local_server), pool, bytes, 0)
    → returns real rkey. Follower replies with (addr, rkey).

  Step 3: Follower-to-follower QPs
    For each non-tail follower:
      Leader sends CHAIN-WIRE telling that follower "your successor is at
      (host, rdma_port)". Follower calls rdmamig_client_connect to successor.
      Successor's server_cm_id is now set.

  Step 4: CHAIN-PREP-PEER (re-register on successor's PD)
    Open question: when a follower's pool needs to receive RDMA-WRITEs
    from a DIFFERENT predecessor than the leader (the previous follower in
    the chain), it likely needs the pool registered against THAT
    connection's cm_id too — different cm_ids generally use different PDs.
    Two ways to handle this:
      (a) Re-register the pool against the new cm_id (cheap-ish if same
          ibv_context).
      (b) Bypass rdmamig_buffer_create and use raw ibv_alloc_pd +
          ibv_reg_mr against a single process-wide PD shared across all
          chain QPs. Cleaner architecturally; adds ~300 lines duplicating
          what rdmamig already does for the donor-leader path.
    Decision deferred until first attempt at real hardware run.
```

#### Implications for protocol design

- The chain protocol becomes **multi-round-trip with strict ordering**: connect → prep → wire → connect-to-successor → re-prep. Not the single PREP+WIRE handshake the original design envisioned.
- The leader becomes the orchestrator of all these round-trips. Each step is sequential per follower (can be parallelized later).
- The `rdmamig_server` on each follower must be bootstrapped early (e.g., at process startup) so its listening endpoint is ready when CHAIN-INIT-QP arrives. Today the rdmamig_server is only started lazily on the recipient leader when `RDMA INIT-SERVER` arrives — for followers, we'd need an eager bootstrap path or an analogous RPC.

#### Why this isn't a small "write 500 lines" fix

The original Phase A plan assumed I could write the RDMA calls cleanly because the structural shape matched the library. With the constraint surfaced, what's required is a **protocol redesign** + RDMA-side debugging that can only happen on real hardware. Without an RDMA-capable dev environment, each iteration is "I write code, you run it on the benchmark, capture failures, send back." That loop is realistic for the eventual implementation but not for the design-discovery phase where we'd be iterating on PD-sharing and ordering edge cases.

#### Phase A.full status — RESUMED, path (a)

Decision: redesign the protocol around the rdmamig connect-then-register constraint.

**Revised Phase A.full sequence (connect-then-register):**

1. **CHAIN-INIT-QP** (NEW RPC) — leader → each follower over hiredis-TCP:
   - Each follower lazy-starts its `rdmamig_server` (binds on `server.rdma_migration_port` if not already up).
   - Replies with its RDMA port.

2. **Leader-to-follower QP setup** — leader does `rdmamig_client_create(follower_host, rdma_port)` + `rdmamig_client_connect()` for each follower. After successful connect, the follower's `server_cm_id` is non-NULL.

3. **CHAIN-PREP** (now register-capable) — leader → each follower:
   - Follower `mmap`s the landing pool (as before).
   - Follower **calls `rdmamig_buffer_create(rdmamig_server_cm_id(local_server), pool, bytes, 0)`** — now safe because the leader has connected.
   - Replies with `(addr, rkey)` — real rkey, not placeholder 0.

4. **CHAIN-WIRE** — leader → each follower with successor `(host, rdma_port, addr, rkey)`:
   - Each non-tail follower does `rdmamig_client_create(succ_host, succ_rdma_port)` + `connect` to its successor.
   - Once connected, follower's pool is reachable via the leader's WRITE path. Successor's pool was already registered in step 3 (against its own `server_cm_id` from when leader connected to it in step 2). For chain forwarding, the upstream follower writes to the successor's pool using `(succ_addr, succ_rkey)` from CHAIN-WIRE.

**Open question about cm_id and PD sharing:** when multiple peers connect to the same `rdmamig_server`, does `rdmamig_server_cm_id()` return the most recent connection (overwriting) or accumulate? If overwriting, a pool registered after the leader connects may not be accessible to the F1→F2 QP when F1 connects later. Verification: actual hardware iteration required. If problematic, fallback is path (b) (raw `ibv_alloc_pd` once at process startup, shared across all chain QPs, `ibv_reg_mr` against that PD bypassing `rdmamig_buffer_create`). Path (b) is logged as a contingency, not the first attempt.

**Verification model:** end-to-end requires RDMA hardware. The code compiles clean locally; you iterate on hardware, capture failures, send back. Multi-iteration loop expected.

#### Phase A.full hardware test — outcome (this session)

**Wins verified on real RDMA hardware:**
1. Connect-then-register order works. Leader RDMA-connects to both followers (`rdmamig_client_create + connect`); follower's `server_cm_id` is non-NULL by the time CHAIN-PREP arrives; `rdmamig_buffer_create` succeeds and real rkeys propagate (e.g., `rkey=0x2493a`, `rkey=0xafced`).
2. Per-process chain worker thread (added per user request "keep only one thread for the replication") isolates `rdmamig_client_connect` from the main Redis thread. CHAIN-WIRE handler pushes work to a queue and returns immediately; worker drains FIFO.
3. Without the worker fix, `rdmamig_client_connect` on the main thread starved Raft heartbeats and triggered election storms across the cluster — fully resolved by the worker.
4. `redis-cli RDMA DEBUG-CHAIN-ESTABLISH` returns `OK` within ~100ms (vs hung forever before the fix).

**Remaining blocker — `rdmamig_server` is single-accept:**
The library's accept thread does one `rdma_accept` and returns. The existing aqueduct design has a single peer per session (donor → recipient), so one accept is sufficient. The chain topology breaks this: F2's server needs to accept both the leader (for PREP-time cm_id) AND F1 (for chain forwarding RDMA-WRITEs).

Three forward paths (deferred decision):
- (i) Modify rdmamig's accept thread to loop. Library-level change; single accept becomes accept-loop. ~30 lines in the library + careful concurrency review.
- (ii) Per-incoming-peer `rdmamig_server` — each node runs N servers on different ports. Avoids library change; adds port-management complexity (port allocation, advertisement in CHAIN-INIT-QP replies).
- (iii) Bypass rdmamig for chain entirely — process-wide `ibv_alloc_pd` once at startup; `ibv_reg_mr` against that PD; raw `ibv_post_send` for chain WRITEs. The shared PD means landing buffers are accessible from any QP regardless of which peer connected. Sidesteps both single-accept and cm_id-overwriting. Heaviest in lines but cleanest architecturally.

**Status:** Phase A.full transport plumbing is in. Phase B (chain RDMA-WRITE on DONE-SLOTS) blocked on resolution of the single-accept issue.

---

### Implementation phasing

| Phase | Scope |
|---|---|
| **A** | RDMA chain transport plumbing: QP setup between adjacent replicas, `RDMA CHAIN-PREP` + `RDMA CHAIN-WIRE` RPCs, buffer registration on followers. No batches yet — just verify a buffer can be RDMA-WRITTEN from leader through to chain tail. |
| **B** | Wire TRANSFER to the chain: recipient leader, on receiving `DONE-SLOTS`, forwards the buffer down the chain. Chain-ack TCP control flow back to leader. |
| **C** | Follower-side backpatch consumer: each follower's worker drains its own buffer; uses existing `mergeBackpatchTick` install path. Verify all 3 replicas have keys post-session. |
| **D** | Wrapped body-less AE for `MGN_INDX_UPD`: manifest body only; followers gate matchIndex ack on chain delivery. Implements true chain-majority `MGN_INDX_UPD` semantics. |
| **E** | Session-end tracking + `MGN_RECP_TXN_DONE` on recipient leader at chain-majority done-of-last-batch. Closes the deferred Step 1c.2 item. |
| **F** | Self-healing on chain link failures (timeout → re-wire). |

Each phase is independently verifiable and can be landed separately.

### Open questions for chain replication

- Chain depth: does `O(log N)` matter at typical N=3-5 replica counts? Probably not (1-2 extra hops). Worth re-evaluating if N grows.
- TCP control vs RDMA control for chain-ack: hiredis-over-TCP is simpler; matches the existing aqueduct's control-plane style. Use TCP for chain-ack.
- Interaction with leader changeover mid-migration: out of scope for first cut; we abort the session and the donor retries.
- Where the manifest's `checksum` is computed: at the leader (just before chain forward) or at the donor (so we get end-to-end integrity)? Donor-side checksum is more thorough; defer the choice to Phase D.

---

## Discovery: aqueduct RDMA migration + RedisRaft don't compose today

A benchmark experiment combining the existing aqueduct RDMA migration path (RDMA MIGRATE-ALL → migrationWorker → mergeBackpatchTick) with `redisraft.so` loaded was attempted in this session ([ansible/experiments/custom_reshard_v2_orch_raft/](ansible/experiments/custom_reshard_v2_orch_raft/)). What it surfaced:

1. **Whitelisting RDMA subcommands in redisraft works.** Added entries for `rdma|migrate`, `migrate-all`, `migrate-prep`, `migrate-status`, `migrate-complete`, `migrate-all-status`, `init-server`, `init-client`, `register-block-slots`, `register-result`, `reshard`, `reshard-flip`, `reshard-recv-flip`, `reshard-transfer`, `transfer-slots`, `done-slots`, `backpatch-status` in [redisraft/src/commands.c](redisraft/src/commands.c). Verified: `RDMA MIGRATE-ALL` now reaches `rdmaMigrateAllCommand` instead of being intercepted/MOVED.

2. **But the aqueduct command returns `ERR cluster mode not enabled on this node`.** Six call sites in [cluster_rdma.c](redis/src/cluster_rdma.c) (around lines 2362, 2576, 2693, 2878, 3918, 4224) test `server.cluster == NULL || server.cluster->myself == NULL` and reject. Under RedisRaft, `server.cluster` (the Redis Cluster runtime struct) is uninitialized — redisraft is started with `cluster-enabled no` and only *fakes* cluster behavior at the module layer (INFO override, slot routing via `ShardingInfo`).

3. **Three identity layers need reconciliation** to bridge:
   - **Slot ownership**: `server.cluster->slots[]` (Redis Cluster, uninitialized under redisraft) vs `ShardingInfo` (redisraft's view) vs `slot_mig_state` (aqueduct migration metadata).
   - **Node identity**: `server.cluster->myself` (NULL under redisraft) vs redisraft's `clusterNodes` dict.
   - **Command routing**: native `getNodeByQuery` in cluster.c vs redisraft's command interception.

4. **The 6 sites need to either** (a) be rewritten to consult redisraft's `ShardingInfo` for node identity + slot ownership, or (b) have redisraft populate `server.cluster->myself` and the slot map as part of its init, or (c) RedisRaft startup check is relaxed to allow `cluster-enabled yes` and the two layers coexist with a clear ownership rule. Each path is real design work.

**Status: paused alongside chain replication.** Both items require the same kind of design discussion — they're not "iterate to fix" bugs.

---

## Operating assumption (intentional defer)

**No leader failover during or shortly after a migration.** The recipient-follower correctness gap (see chain-replication section above) is invisible under happy-path operation: leader has all migrated keys, clients read from leader, no failover means no exposed follower with missing keys. The gap matters only on recipient-side leader change between TRANSFER and the next periodic snapshot. We are explicitly choosing to live with this gap and run the migration assuming "everything runs smoothly." Chain replication remains the proper fix and is documented in the section above; implementation is deferred until the failover scenario becomes a concrete need.

## Still open / deferred

- **Chain replication** — design locked in (see section above); implementation phasing A–F. Closes the recipient-follower correctness gap. **Implementation deferred per operating assumption above; revisit when failover-during-migration becomes a concrete need.**
- ~~**Recipient-side `MGN_RECP_TXN_DONE`**~~ — **implemented under the no-chain assumption.** Logged in [cluster_rdma.c](redis/src/cluster_rdma.c) at the `BACKPATCH_DONE` site, immediately after `MGN_INDX_UPD`. Justification: `backpatch_batches_by_key` is keyed by `(src_node_id, src_mig_id)` → one batch per session, so the per-batch BACKPATCH_DONE event = per-session done. Under chain replication (Phase E), the two log entries will diverge: `MGN_INDX_UPD` per-batch on chain-majority durability, `MGN_RECP_TXN_DONE` only after the last batch is both backpatched and chain-majority-acked. Payload: `"sess=<id> applied=<n> clobber_skipped=<n>"`.
- ~~**Don't-clobber rule** in the backpatch apply path~~ — **already implemented** in both install paths; observability added. [cluster_rdma.c:1547](redis/src/cluster_rdma.c#L1547) uses `kvstoreDictAddRaw` (skip-on-existing); the legacy [cluster_rdma.c:848-855](redis/src/cluster_rdma.c#L848-L855) path uses `lookupKeyWrite + dbAdd` (skip-on-existing). Counter added to both paths: `backpatchMergeWork.skipped_existing` aggregated into `backpatchBatch.clobber_skipped`, logged at `BACKPATCH_DONE` as `clobber_skipped=N`; legacy path stores into `pendingBackpatch.clobber_skipped` and logs at end-of-chunk.
- **In-memory tombstone** for post-FLIP DELETEs (prevent backpatch from resurrecting deleted keys).
- ~~**Donor side write-rejection in MIGRATED state**~~ — **already enforced** by FLIP ordering. `rdmaReshardFlipHelper` at [cluster_rdma.c:3736](redis/src/cluster_rdma.c#L3736) calls `clusterDelSlot(slot) + clusterAddSlot(recipient_node, slot)` under the topology write lock **before** `slot_mig_state` transitions to MIGRATED at [cluster_rdma.c:3747](redis/src/cluster_rdma.c#L3747). By the time MIGRATED is set, `server.cluster->slots[slot]` already points to the recipient → standard routing returns `-MOVED <slot> <recipient>`. Intent documented at [cluster.c:1315-1331](redis/src/cluster.c#L1315-L1331). **CI verification deferred to the fake-harness item below** — RedisRaft fakes Redis Cluster mode (`config.c:736-738`), so the standard routing path doesn't run; faking it across all three layers (`server.cluster->slots[]` + `ShardingInfo` + `slot_mig_state`) for a one-off test is more brittle than building it into the fake-harness once.
- **Failure / abort / resume paths** — donor/recipient leader changeover mid-migration, chain segment that can't heal across sessions, etc.
- **Separate-log-space persistence** — single AppendEntries channel with type tag is in place; optional split of on-disk files for migration entries is deferred.
- **Fake-harness for non-RDMA verification** (Step 1c.3) — a `DEBUG MGN-SIMULATE` style command so the protocol-logging paths can be regression-tested in CI without real RDMA hardware.

---

## Reference files

- [redis/src/cluster_rdma.c](redis/src/cluster_rdma.c) — `migrationWorker` + RDMA primitives + (NEW) `rdmaMgnLogSync` / `rdmaMgnLogAsync` helpers + async hiredis adapter (`mgnRedisAe*`).
- [redisraft/src/migrate.c](redisraft/src/migrate.c) — `cmdRaftMgnLog`.
- [redisraft/src/raft.c](redisraft/src/raft.c) — apply handler bookkeeping for the five `MGN_*` types + Step 0 apply-time observability.
- [redisraft/src/log.c](redisraft/src/log.c) — Step 0 append-time observability.
- [redisraft/src/redisraft.h](redisraft/src/redisraft.h) — `RAFT_LOGTYPE_MGN_*` constants + function declarations.
- [redisraft/src/util.c](redisraft/src/util.c) — `raftLogTypeName()`.
- [redisraft/src/redisraft.c](redisraft/src/redisraft.c), [redisraft/src/commands.c](redisraft/src/commands.c) — `raft.mgn-log` registration.
- [redisraft/tests/integration/test_mgn_log.py](redisraft/tests/integration/test_mgn_log.py) — verification (6 tests, parametrized over all five types + unknown-type rejection).

---

*Last updated: Step 1c.1 (worker-thread instrumentation) + 1c.2 (`MGN_INDX_UPD` main-thread instrumentation) complete; recipient-follower correctness gap identified and chain replication design locked in. Phase A (RDMA chain transport plumbing) is the next implementation step.*
