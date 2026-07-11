/*
 * Copyright Redis Ltd. 2026 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 *
 * Chain replication for the recipient-side of RDMA migration. See
 * /users/entall/rd/MIGRATION_DESIGN.md "Chain replication design" for the
 * full design; this header exposes only the minimal surface that the
 * existing cluster_rdma.c and the Redis command dispatch need.
 */

#ifndef __CLUSTER_RDMA_CHAIN_H
#define __CLUSTER_RDMA_CHAIN_H

#include "server.h"

/* RPC handlers wired into the Redis command table via commands.def.
 * All three run on the main Redis thread; heavy work (RDMA QP setup,
 * memory-region registration) is dispatched onto detached pthreads from
 * within these handlers. */
void rdmaChainInitQpCommand(client *c);
void rdmaChainPrepCommand(client *c);
void rdmaChainWireCommand(client *c);
void rdmaChainForwardedCommand(client *c);
void rdmaChainAckCommand(client *c);
void rdmaChainPingCommand(client *c);
void rdmaDebugChainEstablishCommand(client *c);
void rdmaDebugChainForwardCommand(client *c);
void rdmaDebugChainStatusCommand(client *c);
void rdmaDebugChainApplySlotCommand(client *c);

/* Leader-side library entry points, callable from cluster_rdma.c to wire
 * chain replication into the real migration data path (Phase B.5+).
 *
 * rdmaLeaderChainEstablish: orchestrate the full INIT-QP / RDMA-connect /
 *   PREP / WIRE handshake to a list of follower (host, port) pairs for
 *   the given session. Returns C_OK on success; on failure writes a
 *   message into errbuf and returns C_ERR. */
int rdmaLeaderChainEstablish(long long src_mig_id, long long pool_bytes,
                             int n_followers,
                             const char **hosts, int *ports,
                             char *errbuf, size_t errbuf_len);

/* AqRaft #4 Part B: drop a dead chain head (peers[0]) and promote the next
 * follower into peers[0] so a re-invoked forward routes straight to the
 * surviving follower (chain re-form after a recipient-follower crash).
 * Returns C_OK if a live follower is now at peers[0], C_ERR if none remains. */
int rdmaLeaderChainDropDeadHead(long long src_mig_id,
                                char *errbuf, size_t errbuf_len);

/* AqRaft local slot inventory (bookkeeping WITHOUT Raft): node-local,
 * in-memory, per-session record of received (raw block held) / merged
 * (applied to live keyspace) slots + executed flag. RULE: mark only AFTER
 * the install/apply succeeded (apply-then-mark). See the module comment in
 * cluster_rdma_chain.c. */
void rdmaInvMarkReceived(long long sess, int slot);
void rdmaInvMarkMerged(long long sess, int slot);
void rdmaInvMarkMergedBySlot(int slot);   /* follower path: no sess in the work item */
void rdmaInvMarkExecuted(long long sess);
int  rdmaInvSlotsInRange(long long sess, int kind /*0=received,1=merged*/,
                         int lo, int hi, long long *out, int cap);
int  rdmaInvSummary(long long sess, long long *n_received, long long *n_merged,
                    int *executed);

/* rdmaLeaderChainForwardPerSlot: pass-through chain forward.
 *
 * Caller supplies `snapshot_pool` — a snapshot of the donor's raw 2 MiB
 * per-slot blocks captured BEFORE the recipient's r_allocator corrupts
 * them with kvobj segments (see cluster_rdma.c backpatchBatch::donor_snapshot_pool).
 * Slot at slots[i] sits at offset i * RDMAMIG_BLOCK_SIZE_BYTES within
 * the snapshot pool. The function memcpys snapshot_pool into the chain
 * src_pool (registered for RDMA), issues ONE RDMA-WRITE of
 * n_slots * 2 MiB to F1's landing pool, then sends
 * `RDMA CHAIN-FORWARDED <sess> <n_slots> <slot_0> ... <slot_n>` to F1.
 * F1's chain worker cascades the same wire format to F2.
 *
 * Replaces the legacy rdmaLeaderChainForward + rdmaChainEncodeBatch combo.
 * No dense re-encoding: each per-slot block keeps its on-the-wire format
 * (uint32_t n_entries + (klen, key, vlen, value) tuples). Followers decode
 * per slot via rdmaApplySlotBlock (in cluster_rdma.c).
 *
 * Returns C_OK on success; errbuf populated on C_ERR. */
/* AqRaft zero-copy chain forward: instead of a snapshot copy, the forwarder
 * RDMA-WRITEs the donor's bytes straight out of the recipient's landing ring
 * buffer. `landing_va[i]` is the donor block VA for slots[i] (captured by the
 * backpatch pool worker while still pristine); `landing_buf` is the ring
 * rdmamig_buffer* the donor wrote into — the forwarder registers its F1-PD twin
 * via rdmaLandingFwdBufFor and posts from there. No memcpy, no src_pool. */
int rdmaLeaderChainForwardPerSlot(long long src_mig_id,
                                  const int *slots, int n_slots,
                                  void *const *landing_va,
                                  void *landing_buf,
                                  char *errbuf, size_t errbuf_len);

/* rdmaLeaderChainForwardPipelined: like ForwardPerSlot but RDMA-forwards each
 * 2 MiB block as soon as snapshot_ready[idx] is set by the backpatch worker,
 * overlapping the recipient->F1 write with the ongoing transfer + merge
 * (rdma-chain-pipeline). Single-threaded; sole forwarder for the session. */
int rdmaLeaderChainForwardPipelined(long long src_mig_id,
                                    const int *slots, int n_slots,
                                    void *const *landing_va,
                                    void *landing_buf,
                                    const _Atomic unsigned char *snapshot_ready,
                                    const int *chunk_slots, _Atomic uint64_t *ch_chunk_logged,
                                    char *errbuf, size_t errbuf_len);

/* AqRaft zero-copy chain forward (implemented in cluster_rdma.c): register every
 * landing-ring buffer's F1-PD twin (off-main), and resolve the twin MR for a
 * given ring buffer + F1 cm_id. struct rdma_cm_id is opaque here. */
struct rdma_cm_id;
void  rdmaEnsureLandingFwdReg(struct rdma_cm_id *f1_cm);
void *rdmaLandingFwdBufFor(void *landing_buf, struct rdma_cm_id *f1_cm);

/* rdmaLeaderChainAckCount: number of CHAIN-ACK messages received from the
 * tail for this session. Returns -1 if no chain state for sess. */
long long rdmaLeaderChainAckCount(long long src_mig_id);

/* AqRaft Patch 15: pre-register the leader's chain source pool so the first
 * rdmaLeaderChainForwardPerSlot call doesn't block the main thread on a 2.86
 * GB ibv_reg_mr. Idempotent. Returns C_OK on success / already-registered;
 * C_ERR otherwise with errbuf populated. Designed to be called from
 * chainEstablishThread immediately after rdmaLeaderChainEstablish succeeds,
 * so the heavy ibv_reg_mr happens off the main event loop. */
int rdmaLeaderChainEnsureSrcPool(long long src_mig_id,
                                 char *errbuf, size_t errbuf_len);

/* Apply one slot's 2 MiB block (uint32_t n_entries header + entry tuples)
 * into this replica's kvstore. Installs through r_allocator +
 * kvstoreDictAddRaw with don't-clobber semantics (existing keys win).
 * Implemented in cluster_rdma.c (needs r_allocator / kvstore access).
 * Returns number of keys installed. */
int rdmaApplySlotBlock(redisDb *db, int slot, const char *buf, size_t buf_size);

/* AqRaft Patch 29: recipient-follower per-slot merge enqueue. Builds the
 * shadow dict off-main from the slot's r_allocator-registered blocks and
 * hands it to the main-thread merge queue (mergeBackpatchTick) with a NULL
 * batch, so the live-keyspace insert runs serialized on the event loop —
 * same as the leader. Precondition: the slot's landing-pool slice must
 * already be registered via r_allocator_register_existing_block.
 * Implemented in cluster_rdma.c. Returns staged key count. */
int rdmaFollowerEnqueueSlotMerge(redisDb *db, int slot);

#endif /* __CLUSTER_RDMA_CHAIN_H */
