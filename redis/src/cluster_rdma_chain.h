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

/* rdmaLeaderChainForward: RDMA-WRITE the leader's src_pool (lazy-allocated
 * on first call) to F1 and send CHAIN-FORWARDED so the chain cascades.
 * fill_pattern: when nonzero, fill the first <length> bytes with
 * byte[i]=i%256 before the WRITE (for DEBUG-only / CHAIN-PING verification).
 * Returns C_OK on success; errbuf populated on C_ERR. */
int rdmaLeaderChainForward(long long src_mig_id, size_t length,
                           int fill_pattern,
                           char *errbuf, size_t errbuf_len);

/* rdmaLeaderChainAckCount: number of CHAIN-ACK messages received from the
 * tail for this session. Returns -1 if no chain state for sess. */
long long rdmaLeaderChainAckCount(long long src_mig_id);

/* Phase C: caller fills the chain src_pool with real migration data
 * (packed format produced by rdmaChainEncodeBatch in cluster_rdma.c),
 * then calls rdmaLeaderChainForward to RDMA-WRITE it down the chain.
 *
 * rdmaLeaderChainGetSrcPool returns a writable pointer + size for the
 * leader's chain src_pool. Lazy-allocates on first call. Returns NULL
 * if the chain isn't established for sess. */
void *rdmaLeaderChainGetSrcPool(long long src_mig_id, size_t *out_bytes);

/* Phase C: follower-side hook. Called from rdmaChainForwardedCommand once
 * the chain has delivered `length` bytes into our landing pool. Decodes
 * the packed format and installs entries via dbAdd-style semantics. Pure
 * forward-decl — implemented in cluster_rdma.c so it has access to db /
 * kvstore APIs. */
void rdmaApplyChainBatch(redisDb *db, const char *buf, size_t length);

#endif /* __CLUSTER_RDMA_CHAIN_H */
