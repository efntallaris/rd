# AqRaft 30M BGMERGE — Crash / Fault-Injection Scenarios

> Scope: fault-injection + crash-recovery scenarios layered on top of the validated
> 30M background-merge reshard (`HOWTO_30M_BGMERGE.md`). Four scenarios below.
> Baseline run command, topology, and expected clean results all come from that HOWTO.
> **Proposed final home in repo:** `ansible/experiments/custom_reshard_v2_orch_raft_chunked/CRASH_SCENARIOS.md`

## Shared context (applies to every scenario)

**Topology** (`inventory.ini`, `group_vars/all.yml`):
- Donors (3-way replicated across redis0/1/2): sg1 leader `redis0:8000` (followers redis1,redis2);
  sg2 leader `redis1:8001` (followers redis2,redis0); sg3 leader `redis2:8002` (followers redis0,redis1).
- Recipient sg4: **leader `redis3:8000`**, followers **redis4, redis5** (chain: leader→F1→F2).
- Recipient carries `--raft.snapshot-disable yes`.

**Kill handles** — pidfile `/tmp/redis_<host>_<sg>.pid`, log `/tmp/redis_logs/<host>_<sg>.log`.
There is **no supervisor / no auto-restart / no targeted-kill task** in the repo — a kill needs a
new backgrounded killer (`kill -9 $(cat <pidfile>)`) armed off a log marker.

**Timing markers to arm an injection off of** (all in the recipient/donor logs):
- `DONE-SLOTS-CHUNK seq=N n_slots=N` — one per chunk (~252ms apart, 12 total). Best mid-transfer fire.
- `RDMA MIGRATE worker: ... state=FLIPPING` — first donor ownership flip (window START).
- `forward FIRST-POST` / `pipelined forward complete` — chain-forward start/end (3 per run).
- `AqRaft bg-merge: ... moved=... skipped=...` — merge progress; `merge_done` on last slot.
- `RDMA MIGRATE-ALL-STATUS <oid>` → `DONE|FAILED`; `INFO cluster` field
  `rdma_recipient_backpatch_in_progress:N` (nonzero = merges still draining, pre-NARROW).

**The governing fact (no in-process resume):** all recipient migration state is volatile
(never in RDB/AOF; `dictCreate`'d fresh at boot). A restarted recipient cannot rejoin a session —
`DONE-SLOTS-CHUNK` for an unknown `mig_id` hard-errors, donor retries to its **60s timeout →
`RDMA_MIG_FAILED`**. Recovery is designed to come from **Raft leader-election promoting a follower**
that already holds chain-replicated blocks, not from the crashed process resuming.

### Leader election / failover (redisraft — willemt/raft, `redisraft/deps/raft`)
- **Active & real.** `election_timeout=1000ms`, heartbeat `request_timeout=200ms`
  (`deps/raft/src/raft_server.c:108-109`; defaults not overridden in ansible). New leader in ~1–2s.
- **Quorum:** every group is 3 nodes → losing **1 of 3 keeps quorum**; check-quorum step-down
  implemented (`raft_periodic`, `raft_server.c:619`). Applies to donors sg1/2/3 AND recipient sg4.
- **BUT the RDMA reshard control plane BYPASSES Raft** (`CMD_SPEC_DONT_INTERCEPT`,
  `redisraft/src/commands.c:116-131`). Only `mgn-log` metadata markers
  (TXN_START/INDX_UPD/RECP_TXN_DONE/TXN_DONE, `migrate.c:416`) + normal GET/SET are Raft-replicated.
  `cluster_rdma.c` has **no become-leader hook** → a newly-elected leader does NOT resume/abort the
  migration. Committed client data survives failover; the in-flight migration dies (donor 60s → FAILED).
  **This gap is what every scenario below turns on.**

### Client failure handling (`ycsb_client/.../RedisClient.java`, stock Jedis 2.9 + custom routing)
- **UPDATE/DELETE** (`execForSlot`, L679): 1500-attempt / **~15s retry loop**; evicts dead conn,
  `refreshSlotOwner` via `CLUSTER SLOTS` re-resolves to the **new leader**; handles
  NOTLEADER/LEADERSHIP/CLUSTERDOWN/TRYAGAIN. Never kills the thread; exhaustion = one `Return=ERROR`.
  → absorbs a ~1–2s leader election inside its budget.
- **READ** (double-read, L867): best-effort; degrades to peer/donor leg; double-miss = one ERROR (noise).
- **slotpoll** (`ycsb_slotpoll_ms=100`) + **no-downgrade guard** (`curIsRecipient`, L184-190) keep writes
  pinned to the correct new leader (prevents the `writeRedirect` storm). Socket timeout 10s.
- **Open question per scenario:** behavior of ops on **in-flight migration slots** while the new leader
  holds none of the volatile backpatch state.

---

## Scenario 1 — Recipient LEADER crash

- **Target:** `redis3:8000` (sg4 leader). Handle: `/tmp/redis_redis3_sg4.pid`, log `redis3_sg4.log`.
- **Injection point (proposed):** mid-transfer, armed off `DONE-SLOTS-CHUNK seq=6` (≈halfway through 12 chunks).
- **Current behavior (from code):** donor's backpatch poll gets errors → retries to 60s timeout →
  donor migration `RDMA_MIG_FAILED`; orchestration `RDMA_ORCH_FAILED`. Recovery depends entirely on
  Raft electing redis4/redis5 (which hold chain-replicated bytes). No in-process resume.

### What should happen (DEFINING — in progress)
On recipient leader election, the new leader:
1. checks whether there is an **active migration** (`TXN_START` seen, no `TXN_DONE`);
2. if active, checks whether there is an **`INDX_UPD`** on the log for it. If yes:
   - first check whether the **RDMA buffers are received** so the index-update can be executed;
   - if not, it is **guaranteed a majority holds the RDMA buffers** (that is exactly what a committed
     `INDX_UPD` means), so some nodes have them → **transfer those** (pull from a peer that has them);
   - **check the chain status** to locate/repair.

Roll-forward, never roll back.

### Chain-status check + buffer recovery — PROPOSED (reuses #4's primitives)
Load-bearing fact (confirmed): chain followers **already apply raw blocks into their own keyspace** —
`CHAIN-FORWARDED` handler does per-slot `rdmaApplySlotBlock` + `rdmaFollowerEnqueueSlotMerge`
(`cluster_rdma_chain.c:1025,1039`, guarded by the `applied` flag). So a promoted follower's keyspace
already reflects everything it received pre-crash → recovery = close a **gap**, not rebuild.

Framing: **recipient-leader crash = "the chain HEAD died."** #4 (follower crash) = middle/tail died,
alive leader re-wires around it. #1 = head died, a follower is promoted and must (a) recover missing
blocks from a peer, then (b) re-form the chain as the new head. **#1 = #4's re-form primitive + a
buffer-gap pull + the `mgn_executed_idx` execution step.**

New leader's reconciliation on promotion, per active session with committed-but-not-executed `INDX_UPD`:
- **A. Chain-status query (new RPC `RDMA CHAIN-STATUS <sess>`, shared with #4):** ask every surviving
  sg4 peer for its **per-slot** state (which slots' raw blocks it holds / applied). Build
  `slot → {holders}`. Committed `INDX_UPD` ⇒ a majority holds the block → every needed slot covered by ≥1 peer.
- **B. Gap recovery (pull):** for each slot the new leader lacks, **reuse the chain-forward path pointed
  inward** — wire a transient recovery link holder→new-leader, holder `CHAIN-FORWARDED`s the slots, new
  leader installs via `rdmaApplySlotBlock`. No new transfer primitive.
- **C. Execute pending index-updates:** once all blocks present, run merge (`rdmaFollowerEnqueueSlotMerge`
  path) and **advance `mgn_executed_idx`**.
- **D. Re-form chain (literally #4) + restore durability:** `rdmaLeaderChainEstablish` over survivors,
  excluding dead old-leader; re-forward any slot lacking a chain-majority of holders before firing
  `RECP_TXN_DONE` (same 3-flag DONE, now on the new leader).
- **E. Donor hand-off (= #2):** donor's remaining chunks past the last `INDX_UPD` must land on the NEW
  leader — donor backpatch-status poll re-resolves to the new endpoint.

**The clean boundary** — recover-vs-retransmit splits exactly at the `INDX_UPD` commit point:
| slot state | source of truth | recovery |
|---|---|---|
| `INDX_UPD` committed | majority of sg4 holds the block (durable) | **pull from a surviving peer** (B) |
| `INDX_UPD` not committed | not guaranteed durable on sg4 | **re-transfer from donor** (E, = #2) |

Shared machinery with #4: `CHAIN-STATUS` RPC + `rdmaLeaderChainEstablish` re-form. #1 adds gap-pull +
executed-watermark execution. Optional optimization: post-election leadership-transfer toward the chain
**tail** (most-complete via tail-commit) to minimize gap recovery — not required.

### KEY MECHANISM — a third Raft watermark ("index-update executed")  ✅ CONFIRMED NEEDED
Raft has exactly two pointers: `commit_idx` and `last_applied_idx`
(`redisraft/deps/raft/src/raft_server.c:170-171`). BUT applying an `MGN_INDX_UPD` entry is
**bookkeeping-only** today (`redisraft/src/raft.c:1117-1134` — it just `LOG_NOTICE`s; the actual merge
runs in the *volatile* `migrationWorker`/`mergeBackpatchTick` on the old leader, which dies with it).
So `last_applied_idx` past an `INDX_UPD` means "marker recorded", NOT "merge executed against live keyspace".

→ Add a third watermark **`mgn_executed_idx`** (**per-session**): "the index-update this `INDX_UPD`
represents is actually applied to *my* live keyspace." On promotion the new leader executes every
committed-but-not-executed `INDX_UPD` in `(mgn_executed_idx, commit_idx]`, sourcing the RDMA buffers it
already holds (was a chain follower) or pulling from a peer. `snapshot-disable yes` on sg4 means the full
log is retained → every `INDX_UPD` marker is always inspectable (no snapshot gap); if snapshots were ever
enabled this watermark must be snapshot-persisted.

**`mgn_executed_idx` semantics (DEFINED): advances only when the merge is FULLY APPLIED** to the live
keyspace (not merely enqueued). On promotion the new leader re-executes any session whose merge wasn't
fully applied; the don't-clobber merge is idempotent so re-applying a partial merge is safe.

### CORE INVARIANT (user-defined) — resolves the multi-majority / double-fault holes
> **`INDX_UPD` committed ⟺ the session's migrated data is durably present in the recipient replica group**
> (a majority of sg4 physically holds the raw blocks).

Consequences:
- **"Two majorities" hole dissolves:** by definition a committed `INDX_UPD` ⇒ a chain-majority physically
  holds the blocks. A single leader crash is a *minority* failure → ≥1 survivor is in that majority →
  `CHAIN-STATUS` always finds a holder → Step-B pull always succeeds. (Standard Raft durability.)
- **"Both followers lag / double fault" hole dissolves:** committed slot → a survivor has it (ask via
  `CHAIN-STATUS`); uncommitted slot → re-transfer from donor (Step E). Genuine double fault (2 of 3) = lost
  majority → Raft can't progress → migration stalls until a node returns (out of scope, = Scenario 3).
- **⚠️ DECISION — never fire `INDX_UPD` on timeout; wait for a real majority, always (today VIOLATED):**
  `chainPendingTick` currently fires `INDX_UPD` on the **5s timeout "anyway"** (`cluster_rdma.c:3042-3050`,
  `CHAIN_PENDING_TIMEOUT_MS`=5000 @ `L1663`) even with a follower missing bytes — faking durability when
  possibly only the dying leader holds the blocks. Under the invariant this is ILLEGAL and must be removed.
  - **Delete the `|| timed_out → fire anyway` behavior.** `INDX_UPD` fires ONLY on a genuine ack.
  - **The 5s timer's job flips:** from *"give up and fire anyway"* → *"stall detected → trigger #4 chain
    re-form"* so a live majority CAN ack. Waiting becomes productive repair, not a passive hang.
  - **"Majority", not the full chain.** Today the leader waits for the **tail** ack = waits for *all*
    followers (tail-commit) = stronger than majority; a slow/dead tail blocks even when a majority already
    holds the data. sg4 = 3 nodes, majority = 2, and the leader always holds the bytes (head) → **L + one
    follower = majority.** Re-form keeps the chain spanning a **live** majority, so the tail ack of the
    re-formed chain = genuine majority durability. (Tail F2 dead → re-form L→F1, F1 acks = L+F1 majority.
    Middle F1 dead → re-form L→F2, F2 acks = L+F2 majority.)
  - **Only true blocker:** a majority of sg4 down (2 of 3) → Raft has no quorum anyway → migration
    correctly stalls until a node returns (= Scenario 3 out-of-scope). No liveness lost in single-fault.
  - → **#4's chain re-form is the PREREQUISITE that makes #1's recovery sound**; the two are coupled.

  **⚠️ EMPIRICAL (VALIDATED on real runs) — the invariant is ALREADY VIOLATED in the CLEAN baseline:**
  Grepping the recipient log of the *validated no-crash* run `bgm_30m_4ck` (and `_capfix`):
  - `chain-ack observed` = **0**; `pending CHAIN-ACK timeout` = **3**; `firing MGN_INDX_UPD anyway` = **3**
    (ALL 3 sessions degrade to Raft-only on the 5s timeout — with every follower healthy, no crash).
  - BUT `CHAIN-ACK: sess=1 ... count=3..6` DOES appear → the followers **do** receive + ack the bytes,
    just **after** the 5s deadline (the follower-side merge of the 2.86 GB pool takes > 5s).
  Root cause: **`CHAIN_PENDING_TIMEOUT_MS`=5000 is mistuned** — shorter than the follower merge. So the
  leader fires `INDX_UPD` *before* the ack, every time. Happy path survives (data becomes durable late),
  but there is a real **vulnerability window** [INDX_UPD fired → real ack]. A leader crash in that window
  is exactly the S1 gap. S4's *immediate* degrade (dead F1 → forward fails) and the baseline *timeout*
  degrade are two flavors of one root cause: **INDX_UPD fires before durability is confirmed.**
  → #4 fix is very achievable: the ack mechanism WORKS (acks arrive), so "wait for the real majority ack,
  no matter what" just means **don't give up at 5s** (wait for the ack; re-form only if a member is
  actually dead). This is a **baseline correctness fix**, not only a crash-recovery feature.

### Concurrent multi-donor sessions (assume all 3 donors migrate at once)
Each donor session has its own `INDX_UPD` stream; **per-session `mgn_executed_idx`** lets the new leader
reconcile each `sess` independently (per-session `CHAIN-STATUS` → gap-pull → execute → advance that
session's watermark). Independent by `sess`/mig_id → parallelism is a throughput concern, not correctness.
Per-session watermark is sufficient. ✓

---

## Scenario 2 — Donor LEADER crash

- **Target:** a donor leader, e.g. sg1 `redis0:8000` (or sg2 `redis1:8001` / sg3 `redis2:8002`).
  Handle: `/tmp/redis_redis0_sg1.pid`, log `redis0_sg1.log`.
- **Injection point (proposed):** after that donor's `WRITE_FLIP` but before its chunks reach DONE
  (arm off that donor's `state=FLIPPING`, then fixed delay; or off its first `DONE-SLOTS-CHUNK`).
- **Current behavior (from code):** donor failure funnels through `migFail` → `RDMA_MIG_FAILED` +
  orchestrator `n_failed++`. **Gap:** recipient batch for that session stays `RUNNING` with
  `remaining>0` forever (no recipient-side timeout watching a dead donor). No donor re-assign/retry.

### What should happen (DEFINED):
On donor leader election, the new leader **first checks whether a migration is active**. If so, the new
donor leader **sends an RPC to the recipient leader** to query the migration status and **continues from
there**. The migration is **roll-forward — never roll back**.

### Feasibility (grounded in code) — VIABLE, but requires source changes (a feature, not just a test)
Enabling facts already true:
- `TXN_START` is Raft-replicated with `sess=<mig->id> slots=<first>-<last> n=<n> recipient=<addr>`
  (`cluster_rdma.c:6285`) → new leader detects an in-flight session (`TXN_START` seen, no `TXN_DONE`)
  and knows *what* to migrate and *to whom*, straight from its own Raft log.
- WRITE_FLIP / write-redirect is applied on the `MGN_TXN_START` apply on **every replica**
  (`cluster_rdma.c:230,6292`) → new leader already redirects those slots to sg4; ownership consistent.
- Recipient merge is **don't-clobber / idempotent** (`mergeBackpatchTick`) → re-shipping slots is safe.
  Roll-forward is compatible; no rollback machinery needed.
- Recipient leader (redis3) is alive in this scenario → it can answer the status RPC.

Snag — session identity: recipient keys its batch by `(src_node_id, src_mig_id)` (`cluster_rdma.c:1776`),
and `src_node_id` is the OLD leader's 40-char node id; the new leader has a different node id → won't match
the stuck batch. Resolve by keying resume on the replicated **`sess`** (`mig->id`), not the node id.

Missing pieces to build:
1. **Donor become-leader hook** — on `RAFT_STATE_LEADER`, scan applied mgn-log for `TXN_START` w/o
   `TXN_DONE` → trigger resume. No hook in `cluster_rdma.c` today; wire from redisraft state-change
   callback (`redisraft/src/raft.c:1313`).
2. **Status-query RPC** (new donor leader → recipient) — "for `sess=K`, which slots received+merged,
   which pending?" Today only aggregate `BACKPATCH-STATUS` keyed by node_id+mig_id; need session-keyed,
   per-slot query.
3. **Recipient session-key index + batch adoption** — find the stuck batch by replicated `sess` and let
   the resumed session continue it. *Also closes the "batch stuck RUNNING" gap — resume, not abort.*
4. **Donor resume-transfer path** — `migrationWorker` entry starting at **TRANSFER** for the *remaining*
   slots: re-establish QP + `reg_mr`, ship remaining blocks → BACKPATCH → DONE → TXN_DONE.
5. **Backstop abort** — if no new leader ever resumes (whole donor shardgroup lost), recipient stuck
   batch needs an eventual timeout. Roll-forward covers the common case; backstop covers truly-abandoned.

Open design decisions:
- (a) Keying: reuse replicated `sess` as the stable session key (add recipient session-key index) —
  recommended, matches "continue the same migration."
- (b) Payload: `sess`+range+recipient already replicated; may also carry old node_id / chunk plan for
  precise resume (minor enrichment of the `TXN_START` payload).

---

## Scenario 3 — Donor FOLLOWER crash

- **Target:** a follower of a donor shardgroup, e.g. sg1 follower `redis1` (redis1 also hosts sg2 leader —
  pick the sg1 *instance* on redis1). Handle/log per the sg instance on that host.
- **Injection point (proposed):** mid-transfer of the affected donor (off `DONE-SLOTS-CHUNK`).
- **Current behavior (from code):** donor→recipient RDMA transfer is driven by the donor **leader**;
  the donor follower is not in the transfer path. Each donor sg is 3 nodes, so losing 1 follower keeps
  Raft quorum (2/3). Likely benign for the migration; tests donor-side Raft resilience under load.

### What should happen (DEFINED):
**As long as the donor shardgroup keeps a majority (2 of 3), the system continues working** — the
migration completes unaffected and clients see no impact.

### Feasibility — ALREADY DELIVERED by current code (validation scenario, no source changes)
- Losing 1 donor follower = **2/3 majority retained** → Raft keeps committing (mgn-log entries commit on
  2 acks; the dead follower's append-entries just retry harmlessly). Donor **leader stays leader** (no
  quorum loss → no check-quorum step-down).
- Migration is driven by the donor **leader** (alive); transfer path never touches the follower. Donor
  followers are **not** in the chain (chain is recipient-side: recipient leader → redis4/redis5).
- **This is a validation test**, not a feature: kill a donor follower mid-transfer, assert migration
  completes and integrity holds. Needs only the shared injection harness + verdict checks.

Acceptance criteria: migration `MIGRATE-ALL-STATUS = DONE` (not FAILED); crash signatures = 0 on all
hosts; recipient DBSIZE == migrated keyspace (integrity intact); YCSB UPDATE errors ≈ 0; migration window
not materially widened; donor leader unchanged (no spurious election). Optional: restart the killed
follower and confirm it rejoins + catches up via Raft (donors are not `snapshot-disable`, so catch-up works).

Out of scope (negative case): killing 2 of 3 (or leader+follower) **loses majority** → donor cannot
commit → migration stalls/fails. That is a double-fault / leader-crash case, not this scenario.

---

## Scenario 4 — Recipient FOLLOWER crash

- **Target:** `redis4` or `redis5` (sg4 chain follower). Handle: `/tmp/redis_redis4_sg4.pid`, log `redis4_sg4.log`.
  (`verify_follower_crash_loop.sh` already scans sg4 followers for crash signatures.)
- **Injection point (proposed):** mid-chain, armed off `forward FIRST-POST` (or a `DONE-SLOTS-CHUNK`).
- **Current behavior (from code + VALIDATED baseline run `crash_s4`):** there are **TWO** degrade paths,
  and killing F1 (redis4) triggers the **synchronous** one, not the timeout:
  1. **Synchronous forward-failure (the one that fires on a clean kill):** the leader's *pipelined*
     forward to F1 detects the dead peer immediately — `CHAIN: sess=N pipelined forward failed
     (CHAIN-FORWARDED to F1 failed: Connection reset by peer / poll_send ... reaped) - firing
     MGN_INDX_UPD immediately as fallback`. All 3 sessions degraded this way (observed: 3 immediate, 0
     timeout). The leader ALREADY detects F1 death synchronously — good for #4 (no 5s wait needed).
  2. **5s timeout (`chainPendingTick`):** `firing MGN_INDX_UPD anyway`. Secondary path.
  Both **degrade to Raft-only durability** with the killed follower (and downstream F2) missing bytes.
- **Baseline result (`crash_s4`, redis4 killed at 1st `forward FIRST-POST`):** client-invisible —
  DBSIZE exact 7,492,753, bg-merge moved=7,448,411/skipped=44,341, YCSB ~81k/client, 0 UPDATE errors.
  But durability degraded (3 immediate degrades). Gantt signature: migration 3.45s→**6.08s**,
  4,098→**2,731 blocks**, **ALL-REPLICAS (tail hop F1→F2) row empty**, session 3.1 INDEX-UPDATE
  751ms→**3.66s**. Figures: `figures/crash_s4_gantt.png`, `figures/crash_s4_ycsb.png`.
  → **#4 must intercept BOTH degrade paths** (esp. the synchronous "immediately as fallback"), replacing
  the degrade with re-form-until-live-majority.

### What should happen (DEFINED): leader re-forms the chain
The predecessor of the failed node **detects** the death (see below), **notifies the leader**, and the
**leader re-forms the chain** excluding the dead member, then re-forwards the affected blocks down the
repaired chain (roll-forward, idempotent). Chosen over node-local skip because the leader is the single
source of truth for topology and already owns the establish/wire path.

### Feasibility (grounded in code) — VIABLE, reuses existing primitives
Enabling facts:
- **Detection already works, precisely.** In `chainWorkerHandleForward` (`cluster_rdma_chain.c:387`) the
  predecessor hits the dead successor at 3 points: RDMA completion `rdmamig_client_wait_send<0` (L440),
  TCP `redisConnect` refused (L454), or `CHAIN-FORWARDED` RPC NULL/error (L485). Today it only logs+returns.
- **Leader owns full topology + wire machinery:** `rdmaLeaderChainEstablish` + `CHAIN-INIT-QP`/`CHAIN-PREP`/
  `CHAIN-WIRE`. Each node stores only its *immediate* successor (`rdmaFollowerChainState`,
  `cluster_rdma_chain.c:115` — no full-chain view), so re-form MUST be leader-driven.
- **Recipient merge is idempotent** (don't-clobber) → re-forwarding blocks is safe.
- **Position matters:** sg4 has 2 followers (L→F1→F2). If the **tail F2** dies, bytes are already on L+F1
  = 2/3 majority → re-form is cheap (F1 becomes tail, sends `CHAIN-ACK`). If the **middle F1** dies, only
  L has the bytes (1/3) → leader must re-wire L→F2 directly and re-forward → restores 2/3.

Missing pieces to build:
1. **Predecessor→leader down-notification RPC** — e.g. `RDMA CHAIN-MEMBER-DOWN <sess> <dead_succ_id>
   <reporter_pos>`, replacing the silent log+return at the 3 detection sites in `chainWorkerHandleForward`.
2. **Leader re-form handler** — drop the dead node from the session's ordered membership, recompute
   ordering + `is_tail`, and re-run establish/wire to splice predecessor→(next-after-dead), or make the
   predecessor the new tail if the dead node was the tail.
3. **Re-forward affected blocks** down the repaired chain (roll-forward). Simplest: leader replays all
   session slots from its own landing copy (idempotent merge makes it safe); optimization: track the
   un-acked subset and replay only those.
4. **New-tail `CHAIN-ACK`** — the recomputed tail acks; `chainPendingTick` then completes normally
   instead of the 5s degrade.
5. **Keep the 5s `chainPendingTick` timeout as a BACKSTOP** — if re-form also fails (e.g. both followers
   dead → below majority), fall back to today's Raft-only degrade / abort.

Reuse: `rdmaLeaderChainEstablish` + `CHAIN-*` wire RPCs for the re-wire; idempotent merge for safe
re-forward; existing `chainPendingTick` as the backstop. The crashed node is still a Raft member of sg4
(quorum holds with 2/3); chain re-form is the data-overlay repair, independent of Raft membership.

Acceptance criteria: kill a chain member mid-forward → leader logs re-form → new chain wired excluding the
dead node → `CHAIN-ACK` received (no 5s-timeout degrade line) → all surviving followers hold the session
bytes (integrity) → crash-sig = 0 → YCSB UPDATE errors ≈ 0. Optional: restart the killed follower, confirm
it rejoins Raft and participates in *subsequent* sessions' chains.

---

## EXECUTION ORDER

**Step 0 — commit the current working tree FIRST (branch `aqueduct_broken`).**
`git add -A` everything (bgmerge source, HOWTOs, workloads, all ~18 plot PNGs, AND the
`.claude/settings.json` deletion — per user), then commit with a message describing the validated
background-merge work. This cleanly separates the existing bgmerge work from the new crash-scenario work.
Then proceed to Part 2.

---

## PART 2 — Experimental harness + HOWTO (runnable for ALL 4 scenarios)

### Approach: baseline-first
Build the harness to run against the **current binary first** → captures the "before" (documents today's
failure modes). **S3 should PASS today (the control** — validates the harness). S1/S2/S4 reproduce the gaps.
The SAME harness re-runs after each C fix to prove roll-forward. The migration playbook is unchanged — the
killer is a **backgrounded side-car** armed off a log marker (the migration bash loop is one Ansible task,
so injection must be out-of-band).

### New files (proposed, under `ansible/crash/`)
1. `crash_inject.sh` — shared backgrounded killer. Separates the **arm source** (log to watch) from the
   **kill target** (pidfile) — they differ for S3/S4. Env: `ARM_HOST ARM_LOG ARM_MARKER TARGET_HOST
   PIDFILE [POST_KILL_DELAY] [RESTART=no]`. Flow: `ssh ARM_HOST "tail -Fn0 ARM_LOG" | grep -m1 -E
   ARM_MARKER` (— `-n0` = only NEW lines, avoids a stale prior-run marker firing instantly) → record
   `t_arm` → `ssh TARGET_HOST "kill -9 \$(cat PIDFILE)"` → record `t_kill` to a result file.
   `RESTART=yes` (v2/recovery) re-launches the instance after the delay; baseline uses `no`.
2. `scenarios.env` — the per-scenario config table below.
3. `run_crash_scenario.sh <S1|S2|S3|S4>` — wrapper: background `crash_inject.sh` with the scenario config,
   run the base HOWTO_30M_BGMERGE §1 command with `experiment_name=crash_<scenario>`, then `verdict.sh`.
4. `verdict.sh <scenario>` — observation/verdict collector (reuses `migration_window.py`,
   `verify_follower_crash_loop.sh` grep patterns).
5. `HOWTO_CRASH.md` — the run doc (mirrors HOWTO_30M_BGMERGE structure: run / expected / verify).

### Per-scenario config (arm ≠ target where noted)
| scenario | ARM (watch log) | ARM_MARKER | KILL target (pidfile) |
|---|---|---|---|
| **S1** recipient leader | redis3 `redis3_sg4.log` | `DONE-SLOTS-CHUNK.*seq=6` (~chunk 6/12) | redis3 `/tmp/redis_redis3_sg4.pid` |
| **S2** donor leader | redis0 `redis0_sg1.log` | `state=FLIPPING` +delay, or 1st `DONE-SLOTS-CHUNK` | redis0 `/tmp/redis_redis0_sg1.pid` |
| **S3** donor follower | redis0 `redis0_sg1.log` (leader drives markers) | `DONE-SLOTS-CHUNK` (mid sg1) | **redis1** `/tmp/redis_redis1_sg1.pid` (sg1 replica) |
| **S4** recipient follower | redis3 `redis3_sg4.log` (leader-side chain marker) | `forward FIRST-POST` | **redis4** `/tmp/redis_redis4_sg4.pid` |
> S3/S4 arm on a *different* host's log than the one killed (donor follower / chain follower don't emit
> the timing marker themselves). New-leader detection for verdicts: grep sg4 follower logs for
> `State change: Node is now a leader` (`redisraft/src/raft.c:1314`).

### Verdict per scenario — baseline (current binary) vs post-fix
Common checks: crash sigs (`ASSERTION FAILED|REDIS BUG|Crashed by signal|SIGSEGV`) = 0; restarts; recipient
DBSIZE == migrated keyspace; YCSB UPDATE/READ errors + throughput; `MIGRATE-ALL-STATUS` DONE/FAILED.
- **S1** baseline: recipient down, migration not completing, a follower logs "now a leader" but migration
  NOT resumed → UPDATE errors + short DBSIZE. Post-fix: roll-forward, integrity exact, UPDATE≈0.
- **S2** baseline: donor `RDMA_MIG_FAILED`, recipient batch stuck `RUNNING`, orchestration FAILED.
  Post-fix: new donor leader resumes, migration DONE, integrity exact.
- **S3** baseline: **PASS** — migration DONE, integrity intact, no crash, donor leader unchanged (control).
- **S4** baseline: migration completes but log shows `firing MGN_INDX_UPD anyway` (degraded), killed
  follower missing bytes. Post-fix: no "anyway" line, chain re-formed, real `CHAIN-ACK`, all survivors hold bytes.

### Build order
- **Phase A (now, NO C changes):** harness + `HOWTO_CRASH.md` + baseline runs for all 4. Confirms S3
  passes and S1/S2/S4 reproduce the documented gaps — the "before".
- **Phase B (C changes, ordered):** #4 durability gate → #1 recipient recovery → #2 donor resume.
  Re-run the harness after each to show the "after".

### Consolidated source-change list (Phase B, from the scenario sections above)
- Third watermark `mgn_executed_idx` (per-session, merge-fully-applied). [#1]
- Remove `|| timed_out` fire in `chainPendingTick`; repurpose the 5s timer → trigger re-form. [#4→#1]
- `CHAIN-STATUS` RPC (per-slot, session-keyed) — shared. [#1/#4]
- Leader chain re-form handler (reuse `rdmaLeaderChainEstablish`). [#4/#1]
- Predecessor→leader chain-member-down notify RPC. [#4]
- Recipient become-leader hook: execute pending `INDX_UPD` + gap-pull. [#1]
- Donor become-leader hook + session-keyed status RPC + batch adoption + resume-from-TRANSFER + backstop. [#2]
- Wire redisraft state-change callback (`redisraft/src/raft.c:1313`) → `cluster_rdma` notifications. [#1/#2]

### End-to-end verification
Each scenario is verified by `run_crash_scenario.sh <S>` → the base 30M reshard runs with the kill injected
mid-migration → `verdict.sh` prints PASS/FAIL against the table above. Phase-A success = S3 PASS +
S1/S2/S4 reproduce baseline gaps; Phase-B success = all 4 PASS with exact key integrity and UPDATE≈0.
