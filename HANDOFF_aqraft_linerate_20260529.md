# Handoff: push AqRaft migration aggregate throughput → NIC line rate

Date: 2026-05-29
Branch: `aqueduct_broken` (uncommitted WIP on top of `d8e5ffea`)

## State at end of today

| | |
|---|---|
| Per-donor RDMA transfer (pipelined) | **~12 Gbps** (1.43 GB in ~0.95 s) |
| Aggregate migration (8.58 GB) | **~3.0 Gbps** (23 s wall) |
| Per-donor cycle | ~4.45 s = transfer 1 + backpatch 1.5 + chain 0.7 + INDX_UPD wait 1.3 |
| Durability invariant | `BACKPATCH-STATUS=done` waits for **merge_done && chain_acked && indx_applied** (all 3 flags) |
| Crash status | 0 across redis3/redis4/ycsb1 |
| YCSB OVERALL @ 200t | ~141K ops/sec |

What's already landed (uncommitted in working tree):
- RDMA pipelining ([`rdma_client.c` `poll_send`](redis/src/rdma_migration/rdma_client.c) + [transfer loop](redis/src/cluster_rdma.c#L4522))
- Completion-driven sequencer (`orchSequencerMain` in `cluster_rdma.c`; opt-in via `rdma_migration_peer_stagger_ms=0`)
- 3-flag DONE invariant on `backpatchBatch`: `merge_done`, `chain_acked`, `indx_applied`
- Parallel chain spawn from pool worker on last snapshot capture
- Chain-fallback: `chain_acked=1` when chain not configured or forward fails

## Why aggregate is ~3 Gbps not ~12 Gbps

The per-donor cycle is dominated by backpatch + chain + INDX_UPD wait (~3.5 s) on top of the 1 s actual RDMA write. Sequential donors with the durability invariant means we serialize on `max(merge, chain) + INDX_commit` per donor → ~4.5 s/donor × 3 donors × 2 rounds + setup ≈ ~23 s.

## Levers to pursue tomorrow (ranked by expected payoff)

### 1. Per-chunk chain forward (chain wholly hidden) — **likely biggest unlock**
- **Today:** `chainForwardWorker` forwards ALL 682 slots' bytes in one shot (~0.7 s) AFTER backpatch finishes. Even with the new "spawn on last snapshot" parallel hook, the chain finishes ~0.7 s after BP_DONE in the trace.
- **Change:** forward each `DONE-SLOTS-CHUNK`'s bytes as it arrives (K=32 → ~64 MB → ~ms per chunk on RDMA). Chain becomes ~fully overlapped with transfer + backpatch.
- **Expected:** per-donor cycle 4.45 → ~3 s; aggregate ~3 → ~4.5 Gbps.
- **Risk:** medium-high. Chain-ack accounting becomes per-chunk; recipient `chainPendingTick` needs to track multiple in-flight chain forwards per session.
- **Files:** `cluster_rdma.c` `chainForwardWorker` (~2221), `rdmaDoneSlotsChunkCommand` (~1859), `rdmaLeaderChainForwardPerSlot` (called from chainForwardWorker).

### 2. Sequencer signal on `chain_acked` instead of all 3 — **safe quick win**
- **Today:** `orchSequencerMain` waits for `donors[i].terminal` which reflects donor worker reaching `RDMA_MIG_DONE` → which requires `BACKPATCH-STATUS=done` → all 3 flags.
- **Observation:** `indx_applied` follows `chain_acked` by ~tens of ms (raft commit on a 3-node group is fast). The next donor's PREP+TRANSFER easily covers that gap.
- **Change:** add a second per-donor signal — `chain_durable` set when `chain_acked` is set, dispatched to the orchestrator BEFORE `indx_applied`. Sequencer waits on `chain_durable` not `terminal`.
- **Expected:** shave ~0.5–1 s per donor; aggregate ~3 → ~3.5 Gbps.
- **Risk:** low. `chain_acked` already means majority has the data; INDX_UPD is metadata-only durability.
- **Files:** `cluster_rdma.c` `orchSequencerMain` (~5410), `migNotifyOrchestratorIfAny` (~2475), `rdmaMigrateCompleteCommand`.

### 3. Eliminate donor staging memcpy (register donor blocks directly) — **CPU win**
- **Today:** [line 4542](redis/src/cluster_rdma.c#L4542) `memcpy(staging, donor_blocks[0], RDMAMIG_BLOCK_SIZE_BYTES)` then RDMA-WRITE from staging. ~1.36 GB/donor of donor-CPU memcpy serialized in the transfer loop.
- **Change:** register the donor `r_allocator` block directly as the source MR, post WRITE from `donor_blocks[0]` directly. No memcpy.
- **Expected:** per-donor transfer ~1 s → ~0.5 s; aggregate ~3 → ~3.5 Gbps. Bigger win once (1) lands.
- **Risk:** medium. Lifetime of donor `r_allocator` blocks during in-flight WRs — need to defer block free until completion reaped.
- **Files:** `cluster_rdma.c` `rdmaReshardTransferHelper` (~4522), `rdmaReshardRegisterHelper` (~4290), `r_allocator` API.

### 4. Fix R2 chain "not established" — **correctness, currently hidden by fallback**
- **Today:** R2 chain forward fails with `chain not established for sess=2` on all 3 R2 donors. My chain-fallback patch makes the system NOT hang (chain_acked=1, fall through to INDX_UPD raft replication) — but R2 followers don't receive the data via chain.
- **Investigation:** trace where the chain connection is set up per session and why it's torn down between R1 NARROW and R2 dispatch. Grep for "chain not established" emitter.
- **Risk:** unknown until root-caused.
- **Files:** `cluster_rdma.c` chain establishment path; `rdmaLeaderChainForwardPerSlot` (called from `chainForwardWorker`).

### 5. Compaction (used-extent shipping) — **biggest theoretical, but user said no**
- **User decision today:** "ship the whole blocks, it's not a problem" — keeping full-block. Listed for completeness.
- Would cut transfer ~100× (~8.58 GB → ~16 MB real key data). Per-donor cycle then dominated by backpatch+INDX_UPD.
- **Files:** `cluster_rdma.c:4542` + new `r_allocator_block_used_extent()` in `allocator.c`.

## How to verify any change

1. Build: `make -j -C redis/src redis-server`
2. Deploy: `cd ansible && sudo ansible-playbook -i inventory.ini tasks/build/deploy_redis_binary.yml`
3. Run: `cd ansible && sudo ansible-playbook -i inventory.ini experiments/custom_reshard_v2_orch_raft_chunked/workload.yml -e redis_variant=custom -e pre_reshard_pause=140 -e rdma_migration_peer_stagger_ms=0 -e experiment_name=<name> -e redis_workload=workloada_scaletest -e ycsb_threads_run=200`
4. Check:
   - `grep FINAL_STATE= /tmp/<run>.log` — both rounds DONE
   - `grep -cE "Crashed|BUG REPORT" /tmp/experiments/<name>/logs/redis3/tmp/redis_logs/redis3_sg4.log` — must be 0
   - per-donor timeline: parse `REGISTER-BLOCK-SLOTS: src=`, `backpatch-merge: batch DONE`, `MGN_INDX_UPD applied` from the recipient-leader log
   - YCSB: `grep ^\\[OVERALL\\] /tmp/experiments/<name>/ycsb/ycsb0/tmp/ycsb_output_ycsb0`

5. Plot scripts that already work:
   - `plot_full_run.py <expdir> <out.png>` — CPU/network/throughput/latency 4-panel
   - inline pythons in chat for Gantt + migration-window overlay (see today's `figures/aqraft_3flag_invariant.png` and `aqraft_3flag_invariant_overlay.png`)

## Reference data from today

| Run | Path |
|---|---|
| Default 30s stagger | `/tmp/experiments/scaletest_3v4_200t/` |
| Completion-driven (seq@BP_DONE, unsound) | `/tmp/experiments/completion_driven_200t/` |
| 3-flag DONE (durable, latest) | `/tmp/experiments/invariant_3flag_v2_200t/` |
| Plots | `/users/entall/rd/figures/aqraft_3flag_invariant*.png` |

## Open question

- Should the chain-fallback (`chain_acked=1` on no-chain / forward-failed) keep the data-loss window flagged, or is the MGN_INDX_UPD raft replication considered sufficient for durability in those paths? The R2 failure right now silently falls back; need a decision on whether to surface it as a migration error.
