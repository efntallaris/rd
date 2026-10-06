/*
 * Copyright Redis Ltd. 2026 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 *
 * Chain replication for the recipient side of RDMA migration.
 *
 * The donor RDMA-WRITEs bulk migration data into the recipient leader's
 * landing pool. Without chain replication, recipient followers would be left
 * with stale / missing keys (see MIGRATION_DESIGN.md "The correctness gap
 * chain replication closes"). This module is responsible for:
 *
 *   - Establishing replica↔replica RDMA QPs along a linear chain
 *     (leader → F1 → F2 → ... → tail) at session start.
 *   - Registering identical landing pools on every replica.
 *   - Forwarding each batch's buffer along the chain so all replicas end
 *     up with byte-identical landing pools.
 *
 * This file currently implements only the Phase A skeleton: RPC handlers
 * parse + reply with placeholder data, and the data structures + life-cycle
 * dictionaries are in place. Phase A.full will wire up actual rdmamig_*
 * QP creation and ibv_reg_mr; Phases B+ will handle TRANSFER forwarding,
 * follower-side backpatch consumption, and self-healing.
 */

#include "server.h"
#include "cluster.h"
#include "cluster_rdma_chain.h"
#include "hiredis.h"
#include "rdma_migration/include/rdma_migration.h"
#include "rdma_migration/allocator.h"   /* r_allocator_get_block_buffers_for_slot */

#include <pthread.h>
#include <sys/mman.h>
#include <string.h>
#include <stdlib.h>
#include <inttypes.h>

/* ====================================================================== *
 *  Lazy bootstrap of the local rdmamig_server                           *
 * ====================================================================== *
 *
 * Required because Phase A.full's redesigned protocol order is
 * connect-then-register: the leader (and upstream followers) must connect
 * to the local rdmamig_server BEFORE we can call rdmamig_buffer_create
 * (which needs a non-NULL cm_id). The existing aqueduct INIT-SERVER
 * RPC starts the server when the donor calls it; for chain replication we
 * need it up before CHAIN-PREP, which means each follower must bootstrap
 * its own.
 *
 * server.rdma_server is a global slot used by both paths (existing
 * INIT-SERVER + this chain code). If already populated we reuse it. */
void rdmaMgnReceivedAsync(long long sess, long long len, int position);   /* cluster_rdma.c */
static pthread_mutex_t g_rdma_server_bootstrap_mu = PTHREAD_MUTEX_INITIALIZER;

/* AqRaft S1: chain followers listen on their own port range. The recipient
 * opens one listener per donor on that donor's rdma-migration-port (17777 +
 * donor index), and a follower used to bind its chain listener on its own
 * rdma-migration-port (also 17777). Once that follower was promoted to leader,
 * the first donor's INIT-SERVER could not bind ("rdmamig_server_create failed")
 * and that donor could never be re-homed. The leader learns this port from the
 * CHAIN-INIT-QP reply, so moving it needs no other change. */
#define CHAIN_RDMA_PORT_OFFSET 1000
static int chainRdmaPort(void) { return server.rdma_migration_port + CHAIN_RDMA_PORT_OFFSET; }

/* The chain listener, on chainRdmaPort(). Kept apart from server.rdma_server:
 * on a node that has been the recipient LEADER, server.rdma_server is the
 * listener a donor asked for (INIT-SERVER, on the donor's port). Taking that
 * for the chain listener left nothing on chainRdmaPort(), while CHAIN-INIT-QP
 * still replied with that port: when leadership moved to another replica, the
 * new leader's connect to the old leader was refused and no later session
 * could be replicated ("sole surviving follower has no live QP"). */
static struct rdmamig_server *g_chain_server = NULL;

static int ensureLocalRdmamigServer(void) {
    pthread_mutex_lock(&g_rdma_server_bootstrap_mu);
    if (g_chain_server != NULL) {
        pthread_mutex_unlock(&g_rdma_server_bootstrap_mu);
        return C_OK;
    }
    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", chainRdmaPort());
    struct rdmamig_server *s = rdmamig_server_create(port_str);
    if (s == NULL) {
        pthread_mutex_unlock(&g_rdma_server_bootstrap_mu);
        serverLog(LL_WARNING,
            "CHAIN: rdmamig_server_create(port=%s) failed during bootstrap",
            port_str);
        return C_ERR;
    }
    g_chain_server = s;
    if (server.rdma_server == NULL) server.rdma_server = s;
    serverLog(LL_NOTICE,
        "CHAIN: rdmamig_server bootstrapped on port %s (lazy)", port_str);
    pthread_mutex_unlock(&g_rdma_server_bootstrap_mu);
    return C_OK;
}

/* RDMA clients (QPs) that reported a failed write or completion. A failed RC
 * QP is in the error state for good, and every later post on it is flushed,
 * but a session's QP is reused by the following sessions to the same peer
 * (findLivePeerClient / findLiveSuccessorClient). One transport error (a follower
 * host briefly not ACKing) therefore dropped that LIVE follower from every later
 * round, and it never received them (replicas diverged, 2026-10-02). A broken
 * client is never reused: the next session connects afresh (the follower's
 * listener accepts further connections). */
#define CHAIN_MAX_BROKEN_CLIENTS 256
static void *g_broken_clients[CHAIN_MAX_BROKEN_CLIENTS];
static int g_broken_clients_n = 0;
static pthread_mutex_t g_broken_clients_mu = PTHREAD_MUTEX_INITIALIZER;

static void chainMarkClientBroken(void *cli) {
    if (cli == NULL) return;
    pthread_mutex_lock(&g_broken_clients_mu);
    int found = 0;
    for (int i = 0; i < g_broken_clients_n; i++) if (g_broken_clients[i] == cli) { found = 1; break; }
    if (!found) {
        g_broken_clients[g_broken_clients_n % CHAIN_MAX_BROKEN_CLIENTS] = cli;
        if (g_broken_clients_n < CHAIN_MAX_BROKEN_CLIENTS) g_broken_clients_n++;
    }
    pthread_mutex_unlock(&g_broken_clients_mu);
    if (!found) serverLog(LL_WARNING, "CHAIN: RDMA client %p failed — it will not be reused; "
                          "the next session to that peer connects again", cli);
}

static int chainClientBroken(void *cli) {
    int b = 0;
    pthread_mutex_lock(&g_broken_clients_mu);
    for (int i = 0; i < g_broken_clients_n; i++) if (g_broken_clients[i] == cli) { b = 1; break; }
    pthread_mutex_unlock(&g_broken_clients_mu);
    return b;
}

/* ====================================================================== *
 *  Data structures (Phase A.2)                                          *
 * ====================================================================== */

/* One per chain link the recipient LEADER tracks (one entry per follower). */
typedef struct rdmaChainPeer {
    sds  host;                 /* follower's host */
    int  port;                 /* follower's RDMA migration port */
    int  chain_position;       /* 1 = first follower, ... n = chain tail */
    int  wire_position;        /* position sent to this follower in CHAIN-WIRE; it
                                * identifies itself with it when it reports a batch.
                                * Never renumbered when dead peers are dropped. */
    /* Filled by CHAIN-PREP reply from this follower. */
    uint64_t peer_pool_addr;
    uint32_t peer_pool_rkey;
    size_t   peer_pool_bytes;
    /* Outgoing QP + control channel to this follower. NULL until Phase A.full. */
    void *client;              /* struct rdmamig_client * */
    void *ctrl;                /* struct redisContext * */
    int  established;          /* AqRaft #4b: 1 iff INIT-QP+PREP succeeded; 0 =
                                * follower dead at establish, excluded from chain */
} rdmaChainPeer;

/* Per-session chain state on the recipient LEADER. Keyed in g_leader_chains
 * by src_mig_id (long long). Lives for the session lifetime. */
typedef struct rdmaLeaderChainState {
    long long src_mig_id;
    int n_peers;
    rdmaChainPeer *peers;       /* heap array, len = n_peers */
    pthread_mutex_t mu;
    /* AqRaft zero-copy chain forward: the per-session src_pool is gone — the
     * forwarder RDMA-reads the donor's landing ring buffer directly (see
     * rdmaLandingFwdBufFor / rdmaLeaderChainForwardPipelined). */
    /* Phase B.4: tail-commit ack tracking. Updated by rdmaChainAckCommand
     * when the chain tail confirms it has the bytes. */
    size_t last_acked_length;
    long long last_acked_at_ms;
    long long ack_count;
    /* Followers (bit = wire_position) that reported holding this session's
     * batch. The durability gate counts DISTINCT followers, so a repeated
     * report from one follower never stands in for another. */
    uint64_t acked_mask;
    int n_wired;                /* followers at establish (valid wire positions 1..n) */
    /* Batches the leader has finished forwarding to F1. A CHAIN-ACK before the
     * first forward is a stale AppendEntries report for an earlier session that
     * reused this id (ids restart on every new leader); refuse it so the module
     * retries instead of marking the report seen. */
    long long fwd_count;
    /* Chain repair: attempt number of the newest recipe the leader issued
     * (0 = the original forward). Followers ignore recipes older than the
     * newest they have seen. */
    int repair_attempt;
    /* Set once rdmaLeaderChainEstablish has prepared and wired every follower
     * (or given up on the dead ones). The forward of the blocks may start as
     * soon as the head is ready, but the recipe is only complete, and the
     * followers only know their positions, after this. */
    int establish_done;
} rdmaLeaderChainState;

/* Per-session chain state on a FOLLOWER. Keyed in g_follower_chains
 * by src_mig_id. */
typedef struct rdmaFollowerChainState {
    long long src_mig_id;
    int chain_position;        /* 1 .. n */
    int is_tail;
    /* Incoming: where my predecessor RDMA-WRITEs into. */
    void *landing_pool;        /* mmap'd; NULL in skeleton */
    size_t landing_pool_bytes;
    void *landing_pool_buf;    /* struct rdmamig_buffer * — set in Phase A.full */
    uint64_t landing_pool_addr;
    uint32_t landing_pool_rkey;
    /* Outgoing: only if not tail. */
    sds  successor_host;
    int  successor_port;
    void *successor_client;    /* struct rdmamig_client * */
    void *successor_ctrl;      /* struct redisContext * */
    uint64_t successor_pool_addr;
    uint32_t successor_pool_rkey;
    /* Second registration of landing_pool against successor_client's PD,
     * required for forwarding: rdmamig_client_post_write uses the buffer's
     * cm_id for the QP, so the LOCAL buffer must be on the F1→F2 QP, not
     * the leader→F1 QP. */
    void *forward_src_buf;     /* struct rdmamig_buffer * */
    void *forward_src_pool;    /* the landing pool forward_src_buf covers. A
                                * re-delivery hands this session a FRESH pool;
                                * the old registration must not be used for it. */
    /* Phase B.4: where the tail sends CHAIN-ACK after persisting bytes.
     * Passed in via CHAIN-WIRE so any follower can ack (currently only
     * the tail does, classic "tail commit" chain replication). */
    sds  leader_host;
    int  leader_port;
    /* AqRaft Patch 29: set once the landing-pool slices for this session
     * have been registered with r_allocator + enqueued for merge. Guards
     * against a retried CHAIN-FORWARDED double-registering the same slices
     * (which would leak duplicate alloc_bloc_t nodes). */
    int  applied;
    /* Chain repair (see "Chain recipes" below). */
    int  last_attempt_p1;      /* newest recipe attempt handled here, +1 (0 = none) */
    int  apply_state;          /* 0 = no apply started, 1 = pending/running, 2 = done */
    int  claim_attempt_p1;     /* attempt of the sender allowed to RDMA-write our
                                * landing pool right now, +1 (0 = unclaimed) */
    long long claim_ms;        /* when that claim was taken (it expires) */
    /* The batch this node holds for the session: block i of the landing pool
     * belongs to slots[i]. Kept so the node can serve the batch to a replica
     * that lacks it (CHAIN-STATUS ... SEND). Owned. */
    int *slots;
    int  n_slots;
} rdmaFollowerChainState;

/* ====================================================================== *
 *  Per-session state dictionaries                                       *
 * ====================================================================== */
/*
 * Phase A: a very small key→value table. For Phase A.full / Phase B we will
 * likely switch to a dict by src_mig_id, but a typical experiment has only
 * one migration at a time, so a linear array of N is fine for now.
 */

#define RDMA_CHAIN_MAX_SESSIONS 16   /* 6 real sessions (3 donors x 2 rounds) + 1 pre-warm sentinel + headroom */

/* AqRaft Stage 4 (line-rate forward): max RDMA-WRITE WRs kept in flight before
 * reaping completions. Well under MAX_SEND_WR / CQ_CAPACITY (4096). Keeps the
 * wire full instead of the old post-one/wait-one loop (~15 Gbit/s). */
#define RDMA_FWD_INFLIGHT 512

static rdmaLeaderChainState   *g_leader_chains[RDMA_CHAIN_MAX_SESSIONS]   = {0};
static rdmaFollowerChainState *g_follower_chains[RDMA_CHAIN_MAX_SESSIONS] = {0};
static pthread_mutex_t g_chain_state_mu = PTHREAD_MUTEX_INITIALIZER;
/* Serializes the leader->F1 RDMA-WRITE forward across sessions. With the
 * cross-session pipeline (rdma-chain-xsession) donor N+1's TRANSFER overlaps
 * donor N's CHAIN-REPLICATION, so two sessions' forward threads can be live at
 * once; they share ONE chain QP + send CQ, so only one may post+reap at a time
 * (else poll_send reaps the wrong session's completions). Held only around the
 * post/reap loop; uncontended when sessions run serially (xsession off). */
static pthread_mutex_t g_chain_forward_mu = PTHREAD_MUTEX_INITIALIZER;

/* AqRaft zero-copy chain forward: the process-global leader src-pool MR cache is
 * gone. The forwarder RDMA-reads the donor's landing ring buffer directly (its
 * F1-PD twin MR is pre-registered off-main by rdmaEnsureLandingFwdReg), so there
 * is no separate scratch pool to allocate, register, or cache. */

/* ====================================================================== *
 *  AqRaft local slot inventory — bookkeeping WITHOUT Raft                 *
 *                                                                         *
 *  Node-LOCAL, in-memory, per-session record of which slots this node     *
 *  physically HOLDS (received: raw landing block present + registered)    *
 *  and which it has MERGED into its live keyspace, plus a per-session     *
 *  EXECUTED flag (merge fully applied). This is deliberately NOT Raft-    *
 *  replicated: it describes node-local facts (every node's answer         *
 *  differs), so consensus would be semantically wrong and a Raft round-   *
 *  trip per slot prohibitively slow. The GLOBAL facts stay in Raft where  *
 *  they already are (mgn-log TXN_START / INDX_UPD / TXN_DONE).            *
 *                                                                         *
 *  Safety WITHOUT durability: the inventory lives and dies with the       *
 *  in-memory keyspace it describes (sg4 runs snapshot-disable). A crash   *
 *  kills bytes and bookkeeping together, so a restarted node correctly    *
 *  claims nothing. The ONE RULE that keeps claims honest: mark a bit      *
 *  only AFTER the block/merge is actually installed/applied ("apply-then- *
 *  mark"). Crash in between => data without a claim (harmless: re-pull is *
 *  idempotent), never a claim without data.                               *
 *  CAVEAT: if the keyspace ever becomes disk-persistent (snapshots        *
 *  re-enabled), this inventory must join the same persistence domain or   *
 *  be discarded/reconciled at boot.                                       *
 *                                                                         *
 *  Consumers: RDMA CHAIN-STATUS (peers answer gap-pull queries from       *
 *  their local inventory), the promoted-leader S1 reconciliation           *
 *  (gap = committed sessions from MY OWN raft log − MY inventory), and    *
 *  the S2 donor-status path (merged/remaining per slot).                  *
 *                                                                         *
 *  SESSION-ID COLLISION (real, by design of the wire protocol): every     *
 *  donor numbers its own migrations, so three concurrent donors all send  *
 *  sess=1; the recipient batch key disambiguates by (src_node_id, sess)   *
 *  but CHAIN-FORWARDED carries sess only, so this inventory CANNOT key by *
 *  node id on followers. That is safe for the per-slot bitmaps because    *
 *  donor slot RANGES are disjoint — a RANGE-scoped query (which is how    *
 *  all consumers ask, via the TXN_START payload's slots=lo-hi) is exact.  *
 *  The `executed` bool is therefore ADVISORY under collision (any donor's *
 *  merge_done sets it); the authoritative "executed for session K" is     *
 *  "all slots in K's range have the merged bit" = CHAIN-STATUS MERGED.    *
 * ====================================================================== */
typedef struct rdmaSlotInventory {
    long long sess;                              /* 0 = free entry */
    int       executed;                          /* merge fully applied locally */
    long long n_received, n_merged;
    unsigned char received[CLUSTER_SLOTS / 8];   /* raw block held locally */
    unsigned char merged[CLUSTER_SLOTS / 8];     /* applied to live keyspace */
} rdmaSlotInventory;
static rdmaSlotInventory g_slot_inv[RDMA_CHAIN_MAX_SESSIONS];
static pthread_mutex_t g_slot_inv_mu = PTHREAD_MUTEX_INITIALIZER;

#define INV_BIT_SET(bm, s)  ((bm)[(s) >> 3] |=  (1u << ((s) & 7)))
#define INV_BIT_GET(bm, s)  (((bm)[(s) >> 3] >> ((s) & 7)) & 1u)

/* Caller holds g_slot_inv_mu. */
static rdmaSlotInventory *invFind(long long sess, int create) {
    int free_i = -1;
    for (int i = 0; i < RDMA_CHAIN_MAX_SESSIONS; i++) {
        if (g_slot_inv[i].sess == sess) return &g_slot_inv[i];
        if (g_slot_inv[i].sess == 0 && free_i < 0) free_i = i;
    }
    if (!create || free_i < 0) return NULL;
    memset(&g_slot_inv[free_i], 0, sizeof(g_slot_inv[free_i]));
    g_slot_inv[free_i].sess = sess;
    return &g_slot_inv[free_i];
}

/* Mark AFTER the raw landing block for `slot` is physically present and
 * registered on THIS node (apply-then-mark). */
void rdmaInvMarkReceived(long long sess, int slot) {
    if (sess == 0 || slot < 0 || slot >= CLUSTER_SLOTS) return;
    pthread_mutex_lock(&g_slot_inv_mu);
    rdmaSlotInventory *inv = invFind(sess, 1);
    if (inv && !INV_BIT_GET(inv->received, slot)) {
        INV_BIT_SET(inv->received, slot);
        inv->n_received++;
    }
    pthread_mutex_unlock(&g_slot_inv_mu);
}

/* Mark AFTER `slot`'s shadow has been fully drained into the live keyspace. */
void rdmaInvMarkMerged(long long sess, int slot) {
    if (sess == 0 || slot < 0 || slot >= CLUSTER_SLOTS) return;
    pthread_mutex_lock(&g_slot_inv_mu);
    rdmaSlotInventory *inv = invFind(sess, 1);
    if (inv && !INV_BIT_GET(inv->merged, slot)) {
        INV_BIT_SET(inv->merged, slot);
        inv->n_merged++;
    }
    pthread_mutex_unlock(&g_slot_inv_mu);
}

/* Follower merge completions carry no session (backpatchMergeWork.batch==NULL);
 * resolve by the received bit — slot ranges are disjoint across sessions, so at
 * most one session claims the slot. */
void rdmaInvMarkMergedBySlot(int slot) {
    if (slot < 0 || slot >= CLUSTER_SLOTS) return;
    pthread_mutex_lock(&g_slot_inv_mu);
    for (int i = 0; i < RDMA_CHAIN_MAX_SESSIONS; i++) {
        rdmaSlotInventory *inv = &g_slot_inv[i];
        if (inv->sess != 0 && INV_BIT_GET(inv->received, slot)) {
            if (!INV_BIT_GET(inv->merged, slot)) {
                INV_BIT_SET(inv->merged, slot);
                inv->n_merged++;
            }
            break;
        }
    }
    pthread_mutex_unlock(&g_slot_inv_mu);
}

/* 1 iff some session delivered blocks for `slot` to THIS node and every session
 * that did has also merged them into the live keyspace. A slot with blocks but no
 * inventory entry (or with a received-but-unmerged session) returns 0, so a caller
 * deciding what still needs merging errs on the side of merging. */
int rdmaInvSlotFullyMerged(int slot) {
    if (slot < 0 || slot >= CLUSTER_SLOTS) return 0;
    int seen = 0, all = 1;
    pthread_mutex_lock(&g_slot_inv_mu);
    for (int i = 0; i < RDMA_CHAIN_MAX_SESSIONS; i++) {
        rdmaSlotInventory *inv = &g_slot_inv[i];
        if (inv->sess == 0 || !INV_BIT_GET(inv->received, slot)) continue;
        seen = 1;
        if (!INV_BIT_GET(inv->merged, slot)) { all = 0; break; }
    }
    pthread_mutex_unlock(&g_slot_inv_mu);
    return seen && all;
}

/* Mark AFTER the session's merge is FULLY applied to the live keyspace
 * (the local mgn_executed watermark — "I executed this INDX_UPD"). */
void rdmaInvMarkExecuted(long long sess) {
    if (sess == 0) return;
    pthread_mutex_lock(&g_slot_inv_mu);
    rdmaSlotInventory *inv = invFind(sess, 1);
    if (inv) inv->executed = 1;
    pthread_mutex_unlock(&g_slot_inv_mu);
    serverLog(LL_NOTICE,
        "AqRaft inventory: sess=%lld EXECUTED (merge fully applied locally)", sess);
}

/* Snapshot query helpers (single lock hold). kind: 0=received, 1=merged.
 * Fills out[] with slot ids in [lo,hi] whose bit is set; returns count, or -1
 * if this node has no inventory at all.
 *
 * Lookup is exact-session first, then RANGE-AGGREGATE (union across every
 * inventory entry): the wire chain session is the recipient-synthesized
 * unique chain_sess (7e17 namespace), while consumers (B#1 peer-pull, the
 * promoted-leader reconciliation) query by the TXN_START sess. Donor slot
 * ranges are disjoint, so the union restricted to [lo,hi] is exact for the
 * batch that owns that range regardless of which id the bits were filed
 * under. */
int rdmaInvSlotsInRange(long long sess, int kind, int lo, int hi,
                        long long *out, int cap) {
    pthread_mutex_lock(&g_slot_inv_mu);
    rdmaSlotInventory *inv = invFind(sess, 0);
    int n = 0;
    if (inv != NULL) {
        const unsigned char *bm = (kind == 1) ? inv->merged : inv->received;
        for (int s = lo; s <= hi && n < cap; s++)
            if (INV_BIT_GET(bm, s)) out[n++] = s;
        pthread_mutex_unlock(&g_slot_inv_mu);
        return n;
    }
    int have_any = 0;
    for (int i = 0; i < RDMA_CHAIN_MAX_SESSIONS; i++)
        if (g_slot_inv[i].sess != 0) { have_any = 1; break; }
    if (!have_any) {
        pthread_mutex_unlock(&g_slot_inv_mu);
        return -1;
    }
    for (int s = lo; s <= hi && n < cap; s++) {
        for (int i = 0; i < RDMA_CHAIN_MAX_SESSIONS; i++) {
            rdmaSlotInventory *e = &g_slot_inv[i];
            if (e->sess == 0) continue;
            const unsigned char *bm = (kind == 1) ? e->merged : e->received;
            if (INV_BIT_GET(bm, s)) { out[n++] = s; break; }
        }
    }
    pthread_mutex_unlock(&g_slot_inv_mu);
    return n;
}

/* Summary for logs/status: returns 1 + fills counters if the session exists. */
int rdmaInvSummary(long long sess, long long *n_received, long long *n_merged,
                   int *executed) {
    pthread_mutex_lock(&g_slot_inv_mu);
    rdmaSlotInventory *inv = invFind(sess, 0);
    if (inv == NULL) {
        pthread_mutex_unlock(&g_slot_inv_mu);
        return 0;
    }
    if (n_received) *n_received = inv->n_received;
    if (n_merged)   *n_merged   = inv->n_merged;
    if (executed)   *executed   = inv->executed;
    pthread_mutex_unlock(&g_slot_inv_mu);
    return 1;
}

/* Forward decl — definition is further down in the leader-side section. */
static rdmaLeaderChainState *findLeaderState(long long src_mig_id);
static void chainMarkForwarded(long long src_mig_id);

static rdmaFollowerChainState *findFollowerState(long long src_mig_id) {
    for (int i = 0; i < RDMA_CHAIN_MAX_SESSIONS; i++) {
        if (g_follower_chains[i] && g_follower_chains[i]->src_mig_id == src_mig_id)
            return g_follower_chains[i];
    }
    return NULL;
}

static int insertFollowerState(rdmaFollowerChainState *st) {
    for (int i = 0; i < RDMA_CHAIN_MAX_SESSIONS; i++) {
        if (g_follower_chains[i] == NULL) {
            g_follower_chains[i] = st;
            return C_OK;
        }
    }
    return C_ERR;
}

/* AqRaft Stage 2: find a live outgoing successor QP to (host, port) opened by
 * a DIFFERENT chain session. The successor's per-process rdmamig_server is a
 * singleton that already accepted our sess=1 connect; a 2nd rdma_connect for
 * sess=2 would hang. Reuse the existing QP instead — this is the follower-side
 * mirror of the leader's findLivePeerClient (which fixed the leader→F1 hop in
 * Stage 1; this fixes the F1→tail hop). `port` is the successor's redis TCP
 * port (succ_rdma_port is 0 on the reuse path, so we key off the TCP port that
 * CHAIN-WIRE always carries). Caller holds g_chain_state_mu. */
static void *findLiveSuccessorClient(const char *host, int port,
                                     long long exclude_sess) {
    for (int i = 0; i < RDMA_CHAIN_MAX_SESSIONS; i++) {
        rdmaFollowerChainState *fs = g_follower_chains[i];
        if (fs && fs->src_mig_id != exclude_sess &&
            fs->successor_client != NULL &&
            !chainClientBroken(fs->successor_client) &&
            fs->successor_host != NULL &&
            fs->successor_port == port &&
            strcmp(fs->successor_host, host) == 0) {
            return fs->successor_client;
        }
    }
    return NULL;
}

/* ====================================================================== *
 *  Per-process chain worker thread                                       *
 * ====================================================================== *
 *
 * One pthread per process drains a queue of pending chain RDMA-control
 * operations (currently: "open outgoing QP to successor"). RPC handlers
 * push work and return immediately so main thread stays unblocked —
 * critical because long rdmamig_client_connect calls were starving Raft
 * heartbeats and triggering election storms.
 *
 * Single thread per process (not per session) per the design constraint:
 * keep replication serial in one thread. Sessions are processed in FIFO
 * order, which is fine for the chain sizes we run (3-5 followers). */

typedef enum {
    CHAIN_WORK_OPEN_SUCC_QP = 1,   /* open RDMA QP to successor */
    CHAIN_WORK_FORWARD      = 2,   /* RDMA-WRITE bytes to successor + CHAIN-FORWARDED RPC */
    CHAIN_WORK_ACK_LEADER   = 3,   /* TCP CHAIN-ACK <sess> <length> to leader */
} chainWorkKind;

typedef struct chainWorkItem {
    chainWorkKind kind;
    long long src_mig_id;
    sds host;       /* successor host (OPEN_SUCC_QP) / leader host (ACK_LEADER) */
    int  port;      /* successor RDMA port (OPEN_SUCC_QP) / leader TCP port (ACK_LEADER) */
    size_t length;  /* bytes to forward (FORWARD) / bytes to ack (ACK_LEADER) */
    /* Per-slot pass-through forward (CHAIN_WORK_FORWARD only): the slot list
     * the leader's batch carried. The cascading CHAIN-FORWARDED RPC re-emits
     * this same list so the next follower knows which (2 MiB block i →
     * slot[i]) mapping to apply. Owned by the item; freed when the worker
     * drains it. NULL when not applicable. */
    int *slots;
    int n_slots;
    /* AqRaft Stage 2: when non-NULL, OPEN_SUCC_QP reuses this existing QP to
     * the successor instead of create+connect (the successor's rdmamig_server
     * singleton can't accept a 2nd connect). The worker still registers this
     * session's fresh landing pool against the reused QP's cm_id. Borrowed
     * pointer (owned by the session that created it); not freed here. */
    void *reuse_client;
    int tcp_port;   /* OPEN_SUCC_QP: successor's redis port, to ask it for its RDMA
                     * port (CHAIN-INIT-QP) when `port` is 0 and nothing can be reused */
    /* AqRaft forward gate (FORWARD only): this follower's apply job, started
     * only AFTER the forward to the successor is over. The apply (FillShadow +
     * adopt-in-place merge) rewrites the landing blocks the forward RDMA-reads,
     * so running both at once ships corrupted blocks downstream. Owned. */
    void *deferred_apply;
    /* Chain recipe (FORWARD only, has_recipe=1): the followers that come after
     * this one, in order, and the recipe's attempt number. Owned. */
    int   has_recipe;
    int   attempt;
    int   n_recipe;
    sds  *recipe_hosts;
    int  *recipe_ports;
    struct chainWorkItem *next;
} chainWorkItem;

static void chainSpawnApply(void *job);

static chainWorkItem *g_chain_work_head = NULL;
static chainWorkItem *g_chain_work_tail = NULL;
static pthread_mutex_t g_chain_work_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_chain_work_cv = PTHREAD_COND_INITIALIZER;
static pthread_t       g_chain_worker_tid;
static int             g_chain_worker_started = 0;
static pthread_mutex_t g_chain_worker_start_mu = PTHREAD_MUTEX_INITIALIZER;

static void chainWorkPush(chainWorkItem *item) {
    pthread_mutex_lock(&g_chain_work_mu);
    item->next = NULL;
    if (g_chain_work_tail) g_chain_work_tail->next = item;
    else g_chain_work_head = item;
    g_chain_work_tail = item;
    pthread_cond_signal(&g_chain_work_cv);
    pthread_mutex_unlock(&g_chain_work_mu);
}

static chainWorkItem *chainWorkPop(void) {
    pthread_mutex_lock(&g_chain_work_mu);
    while (g_chain_work_head == NULL) {
        pthread_cond_wait(&g_chain_work_cv, &g_chain_work_mu);
    }
    chainWorkItem *item = g_chain_work_head;
    g_chain_work_head = item->next;
    if (g_chain_work_head == NULL) g_chain_work_tail = NULL;
    pthread_mutex_unlock(&g_chain_work_mu);
    return item;
}

/* TCP control connection with bounded connect and reply waits (defined with
 * the chain-recipe code below). */
static redisContext *chainConnect(const char *host, int port);

/* Forward declaration (FOLLOWER → SUCCESSOR forward, runs on chain worker). */
static void chainWorkerHandleForward(chainWorkItem *item);
static void chainWorkerHandleRecipeForward(chainWorkItem *item);

static int sendChainInitQp(const char *host, int port, long long src_mig_id,
                           int *out_rdma_port, char *errbuf, size_t errbuf_len);

static void chainWorkerHandleOpenSuccQp(chainWorkItem *item) {
    char rdma_port_str[16];
    /* No QP to reuse and no RDMA port from the leader (it skips CHAIN-INIT-QP when
     * its own QP to that node is reused): ask the successor for its port. This is
     * the case after this node's QP to the successor has failed: without it the
     * successor was left out of every later session (S2/S9: the chain tail lacked
     * every round after one transport error). */
    if (item->reuse_client == NULL && item->port <= 0 && item->tcp_port > 0) {
        char ierr[200] = {0};
        int rp = 0;
        if (sendChainInitQp(item->host, item->tcp_port, item->src_mig_id, &rp, ierr, sizeof(ierr)) == C_OK)
            item->port = rp;
        else
            serverLog(LL_WARNING, "CHAIN worker: sess=%lld could not get the RDMA port of "
                      "successor %s:%d (%s)", item->src_mig_id, item->host, item->tcp_port, ierr);
        if (item->port <= 0) return;
    }
    snprintf(rdma_port_str, sizeof(rdma_port_str), "%d", item->port);

    struct rdmamig_client *cl;
    if (item->reuse_client != NULL) {
        /* AqRaft Stage 2: reuse a prior session's QP to this same successor.
         * The successor's singleton rdmamig_server cannot accept a 2nd
         * rdma_connect (it would hang — the F1→tail analog of the leader→F1
         * constraint Stage 1 fixed). We still register THIS session's fresh
         * landing pool against the reused QP's cm_id below (new forward_src_buf). */
        cl = (struct rdmamig_client *) item->reuse_client;
        serverLog(LL_NOTICE,
            "CHAIN worker: sess=%lld reusing existing QP to successor %s "
            "(skip create/connect)", item->src_mig_id, item->host);
    } else {
        cl = rdmamig_client_create((const char *) item->host, rdma_port_str);
        if (cl == NULL) {
            serverLog(LL_WARNING,
                "CHAIN worker: rdmamig_client_create(%s:%s) returned NULL",
                item->host, rdma_port_str);
            return;
        }
        if (rdmamig_client_connect(cl) != 0) {
            serverLog(LL_WARNING,
                "CHAIN worker: rdmamig_client_connect(%s:%s) failed",
                item->host, rdma_port_str);
            return;
        }
    }

    pthread_mutex_lock(&g_chain_state_mu);
    rdmaFollowerChainState *st = findFollowerState(item->src_mig_id);
    if (st == NULL) {
        pthread_mutex_unlock(&g_chain_state_mu);
        serverLog(LL_WARNING,
            "CHAIN worker: sess=%lld state vanished mid-connect; "
            "QP to %s:%s opened but unused",
            item->src_mig_id, item->host, rdma_port_str);
        return;
    }
    st->successor_client = cl;

    /* Register the landing pool a SECOND time against the successor's cm_id.
     * rdmamig_client_post_write picks the QP off the local buffer's cm_id,
     * so to RDMA-WRITE over the F→succ QP we need a buffer registered on
     * that QP's PD — the existing landing_pool_buf is on the upstream
     * (pred→F) cm_id and would route the WRITE to the wrong QP. */
    void *pool = st->landing_pool;
    size_t pool_bytes = st->landing_pool_bytes;
    pthread_mutex_unlock(&g_chain_state_mu);

    struct rdmamig_buffer *fbuf = NULL;
    if (pool != NULL && pool_bytes > 0) {
        struct rdma_cm_id *cm = rdmamig_client_cm_id(cl);
        if (cm != NULL) {
            fbuf = rdmamig_buffer_create(cm, (char *) pool, pool_bytes, 0);
            if (fbuf == NULL) {
                serverLog(LL_WARNING,
                    "CHAIN worker: sess=%lld forward_src_buf register on "
                    "succ %s:%s cm failed", item->src_mig_id,
                    item->host, rdma_port_str);
            }
        } else {
            serverLog(LL_WARNING,
                "CHAIN worker: sess=%lld successor cm_id NULL after connect",
                item->src_mig_id);
        }
    }

    pthread_mutex_lock(&g_chain_state_mu);
    /* Re-resolve in case state moved while we were registering. */
    st = findFollowerState(item->src_mig_id);
    if (st != NULL) {
        st->forward_src_buf = fbuf;
        st->forward_src_pool = (fbuf != NULL) ? pool : NULL;
        serverLog(LL_NOTICE,
            "CHAIN worker: sess=%lld outgoing RDMA QP to %s:%s established "
            "(forward_src_buf=%p)",
            item->src_mig_id, item->host, rdma_port_str, (void *) fbuf);
    }
    pthread_mutex_unlock(&g_chain_state_mu);
}

/* Forward bytes already in our landing pool down the chain to our successor.
 * Runs on chain worker thread (not main) because rdmamig_client_post_write +
 * wait_send is blocking. On success, sends "RDMA CHAIN-FORWARDED <sess> <len>"
 * to the successor over TCP so it knows its pool now has fresh bytes. */
static void chainWorkerHandleForward(chainWorkItem *item) {
    serverLog(LL_NOTICE,
        "CHAIN worker: forward sess=%lld length=%zu ENTRY",
        item->src_mig_id, item->length);
    /* Snapshot follower state under the state mutex. */
    pthread_mutex_lock(&g_chain_state_mu);
    rdmaFollowerChainState *st = findFollowerState(item->src_mig_id);
    if (st == NULL || st->is_tail) {
        int tail = st ? st->is_tail : -1;
        pthread_mutex_unlock(&g_chain_state_mu);
        serverLog(LL_NOTICE,
            "CHAIN worker: forward sess=%lld no-op (state=%p tail=%d)",
            item->src_mig_id, (void *) st, tail);
        return;
    }
    if (st->landing_pool == NULL || st->forward_src_buf == NULL ||
        st->forward_src_pool != st->landing_pool ||
        st->successor_client == NULL || st->successor_pool_addr == 0 ||
        st->successor_pool_rkey == 0) {
        pthread_mutex_unlock(&g_chain_state_mu);
        serverLog(LL_WARNING,
            "CHAIN worker: forward sess=%lld missing prerequisites "
            "(pool=%p fbuf=%p cli=%p succ_addr=0x%llx rkey=0x%x)",
            item->src_mig_id, st ? st->landing_pool : NULL,
            st ? st->forward_src_buf : NULL,
            st ? st->successor_client : NULL,
            (unsigned long long) (st ? st->successor_pool_addr : 0),
            st ? st->successor_pool_rkey : 0);
        return;
    }
    void *local_pool = st->landing_pool;
    void *local_buf = st->forward_src_buf;
    void *cli = st->successor_client;
    uint64_t remote_addr = st->successor_pool_addr;
    uint32_t remote_rkey = st->successor_pool_rkey;
    sds succ_host = sdsdup(st->successor_host);
    int succ_port = st->successor_port;
    pthread_mutex_unlock(&g_chain_state_mu);

    /* RDMA-WRITE per-slot (n_slots × 2 MiB) to stay within HCA per-WR max_msg_sz. */
    int n_slots_local = item->n_slots > 0 ? item->n_slots
                       : (int) (item->length / RDMAMIG_BLOCK_SIZE_BYTES);
    for (int i = 0; i < n_slots_local; i++) {
        char *l_addr = (char *) local_pool + (size_t) i * RDMAMIG_BLOCK_SIZE_BYTES;
        uint64_t r_addr = remote_addr + (uint64_t) i * RDMAMIG_BLOCK_SIZE_BYTES;
        if (rdmamig_client_post_write(local_buf, l_addr,
                                      r_addr, remote_rkey,
                                      RDMAMIG_BLOCK_SIZE_BYTES) != 0) {
            chainMarkClientBroken(cli);
            serverLog(LL_WARNING,
                "CHAIN worker: forward sess=%lld post_write failed slot_idx=%d",
                item->src_mig_id, i);
            sdsfree(succ_host);
            return;
        }
        if (rdmamig_client_wait_send(cli) < 0) {
            chainMarkClientBroken(cli);
            serverLog(LL_WARNING,
                "CHAIN worker: forward sess=%lld wait_send failed slot_idx=%d",
                item->src_mig_id, i);
            sdsfree(succ_host);
            return;
        }
    }
    serverLog(LL_NOTICE,
        "CHAIN worker: forward sess=%lld wrote %zu bytes (%d × 2 MiB WRs) → %s",
        item->src_mig_id, item->length, n_slots_local, succ_host);

    /* Tell the successor (over TCP) that its pool now has fresh bytes. */
    redisContext *ctx = chainConnect(succ_host, succ_port);
    if (ctx == NULL || ctx->err) {
        serverLog(LL_WARNING,
            "CHAIN worker: forward sess=%lld TCP to %s:%d failed: %s",
            item->src_mig_id, succ_host, succ_port,
            ctx ? ctx->errstr : "(null)");
        if (ctx) redisFree(ctx);
        sdsfree(succ_host);
        return;
    }
    /* Build the cascading CHAIN-FORWARDED RPC. New wire format carries the
     * per-slot list: `RDMA CHAIN-FORWARDED <sess> <n_slots> <slot_0> ...`.
     * The successor uses the list to apply each 2 MiB block at offset
     * i * RDMAMIG_BLOCK_SIZE_BYTES → slot[i]. */
    int argc = 4 + item->n_slots;
    const char **argv = zmalloc((size_t) argc * sizeof(*argv));
    size_t *argvlen = zmalloc((size_t) argc * sizeof(*argvlen));
    char sess_arg[32];
    char nslots_arg[16];
    int sess_arg_len = snprintf(sess_arg, sizeof(sess_arg), "%lld", item->src_mig_id);
    int nslots_arg_len = snprintf(nslots_arg, sizeof(nslots_arg), "%d", item->n_slots);
    char (*slot_bufs)[16] = (item->n_slots > 0)
        ? zmalloc((size_t) item->n_slots * sizeof(*slot_bufs)) : NULL;
    argv[0] = "RDMA";              argvlen[0] = 4;
    argv[1] = "CHAIN-FORWARDED";   argvlen[1] = 15;
    argv[2] = sess_arg;            argvlen[2] = (size_t) sess_arg_len;
    argv[3] = nslots_arg;          argvlen[3] = (size_t) nslots_arg_len;
    for (int i = 0; i < item->n_slots; i++) {
        argvlen[4 + i] = (size_t) snprintf(slot_bufs[i], 16, "%d", item->slots[i]);
        argv[4 + i]    = slot_bufs[i];
    }
    redisReply *r = redisCommandArgv(ctx, argc, argv, argvlen);
    if (r == NULL) {
        serverLog(LL_WARNING,
            "CHAIN worker: forward sess=%lld CHAIN-FORWARDED RPC failed: %s",
            item->src_mig_id, ctx->errstr);
    } else if (r->type == REDIS_REPLY_ERROR) {
        serverLog(LL_WARNING,
            "CHAIN worker: forward sess=%lld successor errored: %s",
            item->src_mig_id, r->str);
    } else {
        serverLog(LL_NOTICE,
            "CHAIN worker: forward sess=%lld successor %s acked (n_slots=%d)",
            item->src_mig_id, succ_host, item->n_slots);
    }
    if (r) freeReplyObject(r);
    if (slot_bufs) zfree(slot_bufs);
    zfree(argv);
    zfree(argvlen);
    redisFree(ctx);
    sdsfree(succ_host);
}

/* Send "RDMA CHAIN-ACK <sess> <length>" to (host, port) over TCP. Used by
 * the chain tail to notify the leader that its landing pool now has the
 * bytes the leader RDMA-WRITE'd into F1. Fire-and-forget — failure is
 * logged and the leader can poll DEBUG-CHAIN-STATUS / use a timeout. */
static void chainWorkerHandleAckLeader(chainWorkItem *item) {
    redisContext *ctx = redisConnect(item->host, item->port);
    if (ctx == NULL || ctx->err) {
        serverLog(LL_WARNING,
            "CHAIN worker: ack sess=%lld TCP to leader %s:%d failed: %s",
            item->src_mig_id, item->host, item->port,
            ctx ? ctx->errstr : "(null)");
        if (ctx) redisFree(ctx);
        return;
    }
    redisReply *r = redisCommand(ctx, "RDMA CHAIN-ACK %lld %lld",
                                 item->src_mig_id, (long long) item->length);
    if (r == NULL) {
        serverLog(LL_WARNING,
            "CHAIN worker: ack sess=%lld CHAIN-ACK RPC failed: %s",
            item->src_mig_id, ctx->errstr);
    } else if (r->type == REDIS_REPLY_ERROR) {
        serverLog(LL_WARNING,
            "CHAIN worker: ack sess=%lld leader errored: %s",
            item->src_mig_id, r->str);
    } else {
        serverLog(LL_NOTICE,
            "CHAIN worker: ack sess=%lld leader %s acked %zu bytes",
            item->src_mig_id, item->host, item->length);
    }
    if (r) freeReplyObject(r);
    redisFree(ctx);
}

/* ====================================================================== *
 *  Chain recipes — mid-chain repair                                      *
 * ====================================================================== *
 *
 * Every CHAIN-FORWARDED carries a RECIPE: the ordered list of followers that
 * come after the receiver, plus an attempt number:
 *
 *   RDMA CHAIN-FORWARDED <sess> <n> <slot>*n RECIPE <attempt> <k> (<host> <port>)*k
 *
 * The recipe moves down the chain as a token. Whoever holds it forwards to the
 * first follower of the list that answers, and hands that follower the rest of
 * the list:
 *   - a follower that does not answer is skipped (the chain routes around it);
 *   - a follower that already holds the session's batch gets the token only,
 *     no data: its landing pool backs live keys and must never be written again;
 *   - any other follower gets the blocks by RDMA and then the token. It is not
 *     necessarily the one this node was wired to at establish time, so the
 *     connection, its landing pool and our source registration are set up on
 *     demand (CHAIN-INIT-QP + CHAIN-PREP, the calls the leader uses).
 *
 * If the token is lost (its holder died), the leader issues a new recipe with
 * a higher attempt number, holders first (rdmaLeaderChainRepair). A follower
 * handles each attempt once and ignores older ones.
 *
 * Two rules keep a repair from damaging data:
 *   CLAIM  before writing a follower's landing pool the sender claims it
 *          (CHAIN-PING with a negative argument, see chainClaimTarget). The
 *          claim is refused while another attempt's sender holds it, and it
 *          expires, so a dead sender does not block the follower forever.
 *   GATE   a node forwards its blocks either before its own apply starts or
 *          after it finished, never during: the apply rewrites segment headers
 *          in the blocks being read. */

#define CHAIN_CLAIM_EXPIRE_MS   30000   /* a sender silent this long lost its claim */
#define CHAIN_CONNECT_TIMEOUT_MS 1000
#define CHAIN_RPC_TIMEOUT_MS    10000
#define CHAIN_APPLY_WAIT_MS     60000

/* Forward decls — definitions are in the leader-side section. */
struct rdmaChainPeer;
static int sendChainInitQp(const char *host, int port, long long src_mig_id,
                           int *out_rdma_port, char *errbuf, size_t errbuf_len);
static int sendChainPrep(const char *host, int port,
                         long long src_mig_id, long long pool_bytes,
                         struct rdmaChainPeer *peer,
                         char *errbuf, size_t errbuf_len);

/* A failed connect is retried for up to CHAIN_CONNECT_RETRY_MS. One failed
 * attempt used to drop the follower from the session: a follower whose host was
 * briefly busy (three recipient groups per host in the 3 -> 6 scale-out) was
 * treated as dead, the session committed without it, and it never received that
 * round (replicas diverged). A refused connection (killed process) is not
 * retried, so this only delays giving up on a follower that is hung or whose
 * host is gone. */
#define CHAIN_CONNECT_RETRY_MS  3000

/* Is the peer behind `ctx` answering? A killed server does not look dead to TCP
 * at once: the kernel closes its sockets only after releasing its memory, and
 * with tens of GB of registered RDMA memory that took ~2.8 s (2026-10-05, S1/S4/
 * S8: every connection to the victim was reset 2.7-2.8 s after the kill, and
 * until then a connect to it SUCCEEDED and the first RPC just waited). A host
 * that loses power never resets anything. So ask for a PING reply within
 * rdma-peer-probe-ms before trusting a control connection; two silent probes
 * (on fresh connections) = dead. Any reply, also an error, counts as alive.
 * Leaves ctx with the reply timeout `restore`. Returns 1 alive, 0 silent. */
int rdmaPeerAnswers(redisContext *ctx, struct timeval restore) {
    int ms = server.rdma_peer_probe_ms;
    if (ms <= 0 || ctx == NULL || ctx->err) return ctx != NULL && !ctx->err;
    struct timeval ptv = { ms / 1000, (ms % 1000) * 1000 };
    redisSetTimeout(ctx, ptv);
    redisReply *r = redisCommand(ctx, "PING");
    if (r == NULL) return 0;                 /* ctx->err is set: the caller frees it */
    freeReplyObject(r);
    redisSetTimeout(ctx, restore);
    return 1;
}

static redisContext *chainConnect(const char *host, int port) {
    struct timeval tv = { CHAIN_CONNECT_TIMEOUT_MS / 1000,
                          (CHAIN_CONNECT_TIMEOUT_MS % 1000) * 1000 };
    if (server.rdma_peer_probe_ms > 0) {
        struct timeval rtv0 = { CHAIN_RPC_TIMEOUT_MS / 1000, 0 };
        for (int probe = 0; probe < 2; probe++) {
            redisContext *pc = redisConnectWithTimeout(host, port, tv);
            if (pc == NULL || pc->err) {
                int refused = (pc != NULL && (errno == ECONNREFUSED ||
                               strstr(pc->errstr, "refused") != NULL));
                if (pc) redisFree(pc);
                if (refused) return NULL;
                break;                       /* a connect timeout: the retry loop below decides */
            }
            if (rdmaPeerAnswers(pc, rtv0)) return pc;
            redisFree(pc);
            if (probe == 1) {
                serverLog(LL_WARNING, "CHAIN: %s:%d accepts connections but did not answer two "
                          "PINGs within %d ms each -- treating it as dead", host, port,
                          server.rdma_peer_probe_ms);
                return NULL;
            }
        }
    }
    long long t0 = mstime();
    redisContext *ctx = NULL;
    for (;;) {
        ctx = redisConnectWithTimeout(host, port, tv);
        if (ctx != NULL && !ctx->err) break;
        /* Refused = nothing listens there: the process is dead. Give up at once.
         * Retrying it made every session after a follower's death wait the full
         * retry time before its chain was set up, with the batch unreplicated
         * meanwhile. Only a timeout (a busy, live peer) is worth retrying. */
        int refused = (ctx != NULL && (errno == ECONNREFUSED ||
                       strstr(ctx->errstr, "refused") != NULL));
        if (ctx) redisFree(ctx);
        ctx = NULL;
        if (refused || mstime() - t0 >= CHAIN_CONNECT_RETRY_MS) return NULL;
        usleep(200000);
    }
    struct timeval rtv = { CHAIN_RPC_TIMEOUT_MS / 1000, 0 };
    redisSetTimeout(ctx, rtv);
    return ctx;
}

/* Ask the follower behind ctx about `sess`.
 *   attempt <  0 : query only.
 *   attempt >= 0 : also claim its landing pool for this attempt's sender.
 * Returns 1 = it holds the batch (never write it), 0 = it does not (and, when
 * claiming, the claim is ours), 2 = another sender holds the claim, -1 = no
 * usable answer. */
static int chainClaimTarget(redisContext *ctx, long long sess, int attempt) {
    long long arg = (attempt < 0) ? -1 : -((long long) attempt + 2);
    redisReply *r = redisCommand(ctx, "RDMA CHAIN-PING %lld %lld", sess, arg);
    int out = -1;
    if (r != NULL && r->type == REDIS_REPLY_INTEGER) out = (int) r->integer;
    if (r) freeReplyObject(r);
    return out;
}

/* Send CHAIN-FORWARDED (+ recipe when attempt >= 0) on an open connection. */
static int chainSendForwardedOn(redisContext *ctx, long long sess,
                                const int *slots, int n_slots,
                                int attempt, sds *rhosts, const int *rports, int k,
                                char *errbuf, size_t errbuf_len) {
    int with_recipe = (attempt >= 0);
    int argc = 4 + n_slots + (with_recipe ? 3 + 2 * k : 0);
    const char **argv = zmalloc((size_t) argc * sizeof(*argv));
    size_t *argvlen = zmalloc((size_t) argc * sizeof(*argvlen));
    int n_num = n_slots + (with_recipe ? 2 + k : 0);
    char (*nums)[24] = zmalloc((size_t) (n_num > 0 ? n_num : 1) * sizeof(*nums));
    char sess_arg[32], nslots_arg[16];
    int a = 0, u = 0;
    argv[a] = "RDMA";            argvlen[a++] = 4;
    argv[a] = "CHAIN-FORWARDED"; argvlen[a++] = 15;
    argvlen[a] = (size_t) snprintf(sess_arg, sizeof(sess_arg), "%lld", sess);
    argv[a++] = sess_arg;
    argvlen[a] = (size_t) snprintf(nslots_arg, sizeof(nslots_arg), "%d", n_slots);
    argv[a++] = nslots_arg;
    for (int i = 0; i < n_slots; i++) {
        argvlen[a] = (size_t) snprintf(nums[u], 24, "%d", slots[i]);
        argv[a++] = nums[u++];
    }
    if (with_recipe) {
        argv[a] = "RECIPE"; argvlen[a++] = 6;
        argvlen[a] = (size_t) snprintf(nums[u], 24, "%d", attempt);
        argv[a++] = nums[u++];
        argvlen[a] = (size_t) snprintf(nums[u], 24, "%d", k);
        argv[a++] = nums[u++];
        for (int i = 0; i < k; i++) {
            argv[a] = rhosts[i]; argvlen[a++] = sdslen(rhosts[i]);
            argvlen[a] = (size_t) snprintf(nums[u], 24, "%d", rports[i]);
            argv[a++] = nums[u++];
        }
    }
    redisReply *r = redisCommandArgv(ctx, argc, argv, argvlen);
    int rc = C_OK;
    if (r == NULL) {
        snprintf(errbuf, errbuf_len, "CHAIN-FORWARDED failed: %s", ctx->errstr);
        rc = C_ERR;
    } else if (r->type == REDIS_REPLY_ERROR) {
        snprintf(errbuf, errbuf_len, "CHAIN-FORWARDED refused: %s", r->str);
        rc = C_ERR;
    }
    if (r) freeReplyObject(r);
    zfree(argv); zfree(argvlen); zfree(nums);
    return rc;
}

/* Outgoing links this follower opened for a repair (to a follower it was not
 * wired to). Touched only by the single chain worker thread. A link, and the
 * registration of one source pool on it, are kept for reuse: the peer's
 * rdmamig_server already accepted this node, and nothing here is torn down. */
#define CHAIN_REPAIR_LINKS 16
static struct {
    sds   host;
    int   port;            /* peer's redis TCP port */
    void *client;          /* struct rdmamig_client * */
    void *src_pool;        /* pool the cached source registration covers */
    void *src_buf;         /* struct rdmamig_buffer * on this link */
} g_repair_link[CHAIN_REPAIR_LINKS];

/* Make (host, port) writable from this node for `sess`: an RDMA link to it,
 * its landing pool for the session, and our pool registered as the source on
 * that link. Returns C_OK and fills the outputs, or C_ERR. Chain worker only. */
static int chainRepairLink(const char *host, int port, long long sess,
                           size_t length, void *pool, size_t pool_bytes,
                           void **cli_out, void **fbuf_out,
                           uint64_t *addr_out, uint32_t *rkey_out,
                           char *errbuf, size_t errbuf_len) {
    int rdma_port = 0;
    if (sendChainInitQp(host, port, sess, &rdma_port, errbuf, errbuf_len) != C_OK)
        return C_ERR;

    int idx = -1, free_idx = -1;
    for (int i = 0; i < CHAIN_REPAIR_LINKS; i++) {
        if (g_repair_link[i].host == NULL) { if (free_idx < 0) free_idx = i; continue; }
        if (g_repair_link[i].port == port && strcmp(g_repair_link[i].host, host) == 0) {
            idx = i; break;
        }
    }
    void *cl = (idx >= 0) ? g_repair_link[idx].client : NULL;
    if (cl != NULL && chainClientBroken(cl)) {
        /* This link's QP failed earlier: drop it and its source registration. */
        cl = NULL;
        g_repair_link[idx].client = NULL;
        g_repair_link[idx].src_pool = NULL;
        g_repair_link[idx].src_buf = NULL;
    }
    if (cl == NULL) {
        /* A link an earlier session wired to this same follower is reusable. */
        pthread_mutex_lock(&g_chain_state_mu);
        cl = findLiveSuccessorClient(host, port, -1);
        pthread_mutex_unlock(&g_chain_state_mu);
    }
    if (cl == NULL) {
        if (rdma_port <= 0) {
            snprintf(errbuf, errbuf_len, "%s:%d reported no RDMA port", host, port);
            return C_ERR;
        }
        char port_str[16];
        snprintf(port_str, sizeof(port_str), "%d", rdma_port);
        struct rdmamig_client *nc = rdmamig_client_create(host, port_str);
        if (nc == NULL || rdmamig_client_connect(nc) != 0) {
            snprintf(errbuf, errbuf_len, "RDMA connect to %s:%s failed", host, port_str);
            return C_ERR;
        }
        cl = nc;
        serverLog(LL_NOTICE, "CHAIN repair: sess=%lld opened RDMA link to %s:%s",
                  sess, host, port_str);
    }
    if (idx < 0) {
        idx = (free_idx >= 0) ? free_idx : 0;   /* table full: recycle entry 0 */
        if (g_repair_link[idx].host) sdsfree(g_repair_link[idx].host);
        g_repair_link[idx].host = sdsnew(host);
        g_repair_link[idx].port = port;
        g_repair_link[idx].src_pool = NULL;
        g_repair_link[idx].src_buf = NULL;
    }
    g_repair_link[idx].client = cl;

    /* The follower's landing pool for this session (it claims one if it has
     * no state yet, e.g. it was not part of the chain at establish time). */
    rdmaChainPeer tmp;
    memset(&tmp, 0, sizeof(tmp));
    if (sendChainPrep(host, port, sess, (long long) length, &tmp,
                      errbuf, errbuf_len) != C_OK)
        return C_ERR;
    if (tmp.peer_pool_addr == 0 || tmp.peer_pool_rkey == 0 ||
        length > tmp.peer_pool_bytes) {
        snprintf(errbuf, errbuf_len, "%s:%d landing pool unusable (addr=0x%llx rkey=0x%x "
                 "bytes=%zu need=%zu)", host, port,
                 (unsigned long long) tmp.peer_pool_addr, tmp.peer_pool_rkey,
                 tmp.peer_pool_bytes, length);
        return C_ERR;
    }

    /* Our pool as the RDMA source on this link. */
    if (g_repair_link[idx].src_buf == NULL || g_repair_link[idx].src_pool != pool) {
        struct rdma_cm_id *cm = rdmamig_client_cm_id((struct rdmamig_client *) cl);
        struct rdmamig_buffer *fb = (cm != NULL)
            ? rdmamig_buffer_create(cm, (char *) pool, pool_bytes, 0) : NULL;
        if (fb == NULL) {
            snprintf(errbuf, errbuf_len, "source registration for %s:%d failed", host, port);
            return C_ERR;
        }
        g_repair_link[idx].src_buf = fb;    /* older one is leaked: no destroy helper */
        g_repair_link[idx].src_pool = pool;
    }
    *cli_out = cl;
    *fbuf_out = g_repair_link[idx].src_buf;
    *addr_out = tmp.peer_pool_addr;
    *rkey_out = tmp.peer_pool_rkey;
    return C_OK;
}

/* RDMA-WRITE n_blocks 2 MiB blocks of `pool` to (addr, rkey), one WR at a time. */
static int chainWriteBlocks(void *cli, void *fbuf, void *pool,
                            uint64_t addr, uint32_t rkey, int n_blocks) {
    for (int i = 0; i < n_blocks; i++) {
        char *l_addr = (char *) pool + (size_t) i * RDMAMIG_BLOCK_SIZE_BYTES;
        uint64_t r_addr = addr + (uint64_t) i * RDMAMIG_BLOCK_SIZE_BYTES;
        if (rdmamig_client_post_write(fbuf, l_addr, r_addr, rkey,
                                      RDMAMIG_BLOCK_SIZE_BYTES) != 0) { chainMarkClientBroken(cli); return C_ERR; }
        if (rdmamig_client_wait_send(cli) < 0) { chainMarkClientBroken(cli); return C_ERR; }
    }
    return C_OK;
}

/* GATE: wait until this node's own apply of `sess` is over. */
static int chainWaitApplyDone(long long sess) {
    long long t0 = mstime();
    for (;;) {
        pthread_mutex_lock(&g_chain_state_mu);
        rdmaFollowerChainState *st = findFollowerState(sess);
        int state = st ? st->apply_state : -1;
        pthread_mutex_unlock(&g_chain_state_mu);
        if (state == 2) return 1;
        if (state != 1) return 0;                     /* never applied here */
        if (mstime() - t0 > CHAIN_APPLY_WAIT_MS) return 0;
        usleep(2000);
    }
}

/* Forward down the recipe (see "Chain recipes"). Chain worker thread. */
static void chainWorkerHandleRecipeForward(chainWorkItem *item) {
    long long sess = item->src_mig_id;
    pthread_mutex_lock(&g_chain_state_mu);
    rdmaFollowerChainState *st = findFollowerState(sess);
    if (st == NULL) {
        pthread_mutex_unlock(&g_chain_state_mu);
        serverLog(LL_WARNING, "CHAIN recipe: sess=%lld no local state — token dropped", sess);
        return;
    }
    void *pool = st->landing_pool;
    size_t pool_bytes = st->landing_pool_bytes;
    /* The pre-wired successor link, usable when the recipe's next follower is
     * the one we were wired to (the normal, no-failure case). */
    sds wired_host = st->successor_host ? sdsdup(st->successor_host) : NULL;
    int wired_port = st->successor_port;
    void *wired_cli = st->successor_client;
    /* Only if that registration covers the pool we hold NOW (see forward_src_pool). */
    void *wired_buf = (st->forward_src_pool == st->landing_pool) ? st->forward_src_buf : NULL;
    uint64_t wired_addr = st->successor_pool_addr;
    uint32_t wired_rkey = st->successor_pool_rkey;
    pthread_mutex_unlock(&g_chain_state_mu);

    int n_blocks = item->n_slots;
    size_t length = (size_t) n_blocks * (size_t) RDMAMIG_BLOCK_SIZE_BYTES;
    int source_ready = (item->deferred_apply != NULL) ? 1 : -1;   /* -1 = not checked */
    int handed = 0;

    for (int t = 0; t < item->n_recipe && !handed; t++) {
        const char *host = item->recipe_hosts[t];
        int port = item->recipe_ports[t];
        sds *rest_hosts = item->recipe_hosts + t + 1;
        const int *rest_ports = item->recipe_ports + t + 1;
        int rest = item->n_recipe - t - 1;
        char err[256] = {0};

        redisContext *ctx = chainConnect(host, port);
        if (ctx == NULL) {
            serverLog(LL_WARNING, "CHAIN recipe: sess=%lld attempt=%d %s:%d unreachable — "
                      "skipping it", sess, item->attempt, host, port);
            continue;
        }
        int holds = chainClaimTarget(ctx, sess, -1);
        if (holds < 0) {
            serverLog(LL_WARNING, "CHAIN recipe: sess=%lld %s:%d gave no answer — skipping it",
                      sess, host, port);
            redisFree(ctx);
            continue;
        }
        if (holds != 1) {
            /* It needs the blocks. GATE first: our copy must be readable. */
            if (source_ready < 0) source_ready = chainWaitApplyDone(sess);
            if (pool == NULL || length > pool_bytes || !source_ready) {
                serverLog(LL_WARNING, "CHAIN recipe: sess=%lld cannot serve as source "
                          "(pool=%p length=%zu/%zu apply_done=%d) — token dropped, "
                          "the leader will re-issue", sess, pool, length, pool_bytes,
                          source_ready);
                redisFree(ctx);
                break;
            }
            void *cli = NULL, *fbuf = NULL; uint64_t addr = 0; uint32_t rkey = 0;
            int wired = (wired_host != NULL && wired_port == port &&
                         strcmp(wired_host, host) == 0 && wired_cli != NULL &&
                         !chainClientBroken(wired_cli) &&   /* a failed QP: open a new link */
                         wired_buf != NULL && wired_addr != 0 && wired_rkey != 0);
            if (wired) {
                cli = wired_cli; fbuf = wired_buf; addr = wired_addr; rkey = wired_rkey;
            } else if (chainRepairLink(host, port, sess, length, pool, pool_bytes,
                                       &cli, &fbuf, &addr, &rkey, err, sizeof(err)) != C_OK) {
                serverLog(LL_WARNING, "CHAIN recipe: sess=%lld no link to %s:%d (%s) — "
                          "skipping it", sess, host, port, err);
                redisFree(ctx);
                continue;
            }
            /* CLAIM its landing pool for this attempt before the first byte. */
            int claim = chainClaimTarget(ctx, sess, item->attempt);
            if (claim == 0) {
                long long t0 = mstime();
                if (chainWriteBlocks(cli, fbuf, pool, addr, rkey, n_blocks) != C_OK) {
                    serverLog(LL_WARNING, "CHAIN recipe: sess=%lld RDMA write to %s:%d "
                              "failed — skipping it", sess, host, port);
                    redisFree(ctx);
                    continue;
                }
                serverLog(LL_NOTICE, "CHAIN recipe: sess=%lld attempt=%d wrote %zu bytes "
                          "to %s:%d (%s link, %lld ms)", sess, item->attempt, length, host,
                          port, wired ? "wired" : "repair", mstime() - t0);
            } else if (claim == 2) {
                serverLog(LL_NOTICE, "CHAIN recipe: sess=%lld %s:%d is being written by "
                          "another attempt — leaving it to that sender", sess, host, port);
                redisFree(ctx);
                continue;
            } else if (claim != 1) {
                redisFree(ctx);
                continue;
            }
            /* claim == 1: it got the batch meanwhile — token only. */
        }
        if (chainSendForwardedOn(ctx, sess, item->slots, item->n_slots, item->attempt,
                                 rest_hosts, rest_ports, rest, err, sizeof(err)) == C_OK) {
            handed = 1;
            serverLog(LL_NOTICE, "CHAIN recipe: sess=%lld attempt=%d token handed to %s:%d "
                      "(%s, %d followers after it)", sess, item->attempt, host, port,
                      holds == 1 ? "already held the batch" : "data sent", rest);
        } else {
            serverLog(LL_WARNING, "CHAIN recipe: sess=%lld %s:%d did not take the token "
                      "(%s) — trying the next follower", sess, host, port, err);
        }
        redisFree(ctx);
    }
    if (!handed)
        serverLog(LL_NOTICE, "CHAIN recipe: sess=%lld attempt=%d ends here (no further "
                  "follower took the token)", sess, item->attempt);
    if (wired_host) sdsfree(wired_host);
}

static void *chainWorkerMain(void *arg) {
    (void) arg;
    serverLog(LL_NOTICE, "CHAIN worker thread started");
    while (1) {
        chainWorkItem *item = chainWorkPop();
        switch (item->kind) {
            case CHAIN_WORK_OPEN_SUCC_QP:
                chainWorkerHandleOpenSuccQp(item);
                break;
            case CHAIN_WORK_FORWARD:
                if (item->has_recipe) chainWorkerHandleRecipeForward(item);
                else                  chainWorkerHandleForward(item);
                break;
            case CHAIN_WORK_ACK_LEADER:
                chainWorkerHandleAckLeader(item);
                break;
            default:
                serverLog(LL_WARNING,
                    "CHAIN worker: unknown work kind %d", item->kind);
        }
        if (item->deferred_apply) chainSpawnApply(item->deferred_apply);
        if (item->host) sdsfree(item->host);
        if (item->slots) zfree(item->slots);
        if (item->recipe_hosts) {
            for (int i = 0; i < item->n_recipe; i++) sdsfree(item->recipe_hosts[i]);
            zfree(item->recipe_hosts);
        }
        if (item->recipe_ports) zfree(item->recipe_ports);
        zfree(item);
    }
    return NULL;
}

static void ensureChainWorker(void) {
    pthread_mutex_lock(&g_chain_worker_start_mu);
    if (!g_chain_worker_started) {
        if (pthread_create(&g_chain_worker_tid, NULL, chainWorkerMain, NULL) == 0) {
            pthread_detach(g_chain_worker_tid);
            g_chain_worker_started = 1;
        } else {
            serverLog(LL_WARNING, "CHAIN: pthread_create for worker failed");
        }
    }
    pthread_mutex_unlock(&g_chain_worker_start_mu);
}

/* ====================================================================== *
 *  RPC handlers                                                          *
 * ====================================================================== */

/*
 * RDMA CHAIN-INIT-QP <src_mig_id>
 *
 * Phase A.full step 1 (connect-then-register). Issued by the recipient
 * leader BEFORE CHAIN-PREP so each follower has its rdmamig_server up
 * and ready to accept the leader's incoming RDMA QP. Without this, a
 * follower's rdmamig_server_cm_id is NULL and rdmamig_buffer_create
 * inside CHAIN-PREP would fail.
 *
 * Reply: 2-element array [status, rdma_port].
 */
void rdmaChainInitQpCommand(client *c) {
    long long src_mig_id;
    if (getLongLongFromObjectOrReply(c, c->argv[2], &src_mig_id, NULL) != C_OK)
        return;

    /* Best-effort bootstrap. Failure (e.g., no RDMA hardware in dev/CI)
     * is logged but not fatal — we still return the port so the control
     * plane can complete; the chain transport just won't work in degraded
     * mode. The leader-side rdmamig_client_connect attempt that follows
     * will surface the real hardware status. */
    int bootstrapped = (ensureLocalRdmamigServer() == C_OK);

    serverLog(LL_NOTICE,
        "CHAIN-INIT-QP: sess=%lld; rdmamig_server %s on port %d",
        src_mig_id,
        bootstrapped ? "up, awaiting upstream RDMA connect"
                     : "BOOTSTRAP FAILED (no RDMA hw?) — degraded mode",
        chainRdmaPort());

    addReplyArrayLen(c, 2);
    addReplyBulkCString(c,
        bootstrapped ? "CHAIN-INIT-QP-OK" : "CHAIN-INIT-QP-DEGRADED");
    addReplyLongLong(c, chainRdmaPort());
}

/* ====================================================================== *
 *  AqRaft: pre-registered FOLLOWER landing-pool ring                      *
 * ---------------------------------------------------------------------- *
 *  CHAIN-PREP used to mmap + ibv_reg_mr a fresh ~1.43 GB landing pool per *
 *  session (~340 ms). The leader's forward waits for the CHAIN-PREP reply *
 *  (the follower's addr/rkey), so across the 2 followers that ~680 ms     *
 *  GATED the leader->F1 forward — chain replication started ~680 ms AFTER *
 *  backpatch instead of with it. Fix: register K pools ONCE (pre-warmed   *
 *  off-thread, triggered on the first CHAIN-PREP which is the CHAIN-WARM   *
 *  one during the pre-reshard pause) and round-robin them per session. K  *
 *  is chosen > sessions-per-migration so a pool is NEVER reused within a   *
 *  migration → the follower's decode + F2-forward of a pool always finish  *
 *  long before that pool is handed to another session (no refcount needed).*
 *  Pools persist + are reused across migrations. Registrations are rounded *
 *  up to a grain so 682- and 683-slot rounds share one pre-warmed pool.    */
#define N_FOLLOWER_LANDING_POOLS 8   /* > sessions/migration (n_rounds*n_donors) */
#define FLP_GRAIN ((size_t) 64 * 1024 * 1024)
static void                  *g_flp_pool[N_FOLLOWER_LANDING_POOLS]  = {0};
static size_t                 g_flp_bytes[N_FOLLOWER_LANDING_POOLS] = {0};
static struct rdmamig_buffer *g_flp_buf[N_FOLLOWER_LANDING_POOLS]   = {0};
/* PD each pool is registered on. The MR belongs to the device's shared PD, not to
 * one connection, so a new upstream (a new leader, or the leader replacing a dead
 * predecessor) reuses it; keying on the cm_id re-registered every pool (2.86 GB
 * ibv_reg_mr, ~0.5 s) on the main thread in CHAIN-PREP. */
static void                  *g_flp_pd[N_FOLLOWER_LANDING_POOLS]    = {0};
static int                    g_flp_next      = 0;
static int                    g_flp_prewarmed = 0;
static pthread_mutex_t        g_flp_mu = PTHREAD_MUTEX_INITIALIZER;

/* AqRaft: a follower merges a pool's kvobjs IN PLACE, so once a session has been
 * applied its ring pool is live keyspace storage. Retire the ring slot (NULL it,
 * keep the mapping + MR — the keyspace references them) so the next claim of this
 * index mmaps a FRESH pool instead of letting a later session RDMA-write over
 * live keys. Same contract as the leader's landing-pool retire. */
static void followerRetirePool(void *pool) {
    if (pool == NULL) return;
    pthread_mutex_lock(&g_flp_mu);
    for (int i = 0; i < N_FOLLOWER_LANDING_POOLS; i++) {
        if (g_flp_pool[i] == pool) {
            g_flp_pool[i] = NULL; g_flp_buf[i] = NULL; g_flp_bytes[i] = 0; g_flp_pd[i] = NULL;
            serverLog(LL_NOTICE, "CHAIN: ring pool[%d] @ %p retired (now live keyspace storage)", i, pool);
            break;
        }
    }
    pthread_mutex_unlock(&g_flp_mu);
}

/* Ensure ring slot idx is registered against cm with capacity >= bytes.
 * Idempotent (reuses a compatible existing registration). The mmap + ibv_reg_mr
 * (the slow ~340 ms part) runs OUTSIDE g_flp_mu. Returns 0 on success. */
static int followerEnsurePool(int idx, struct rdma_cm_id *cm, size_t bytes) {
    if (cm == NULL) return -1;
    size_t cap = (bytes + FLP_GRAIN - 1) & ~(FLP_GRAIN - 1);   /* round up to grain */
    pthread_mutex_lock(&g_flp_mu);
    void *pd = rdmamig_cm_pd(cm);
    int ok = (g_flp_buf[idx] != NULL && g_flp_pd[idx] == pd && g_flp_bytes[idx] >= bytes);
    pthread_mutex_unlock(&g_flp_mu);
    if (ok) return 0;
    void *pool = mmap(NULL, cap, PROT_READ | PROT_WRITE,
                      MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (pool == MAP_FAILED) return -1;
    struct rdmamig_buffer *buf = rdmamig_buffer_create(cm, (char *) pool, cap, 0);
    if (buf == NULL) { munmap(pool, cap); return -1; }
    pthread_mutex_lock(&g_flp_mu);
    /* Publish (leak any prior MR for this slot — no destroy helper; matches the
     * existing recipient big-MR no-destroy contract). */
    g_flp_pool[idx] = pool; g_flp_buf[idx] = buf;
    g_flp_bytes[idx] = cap; g_flp_pd[idx] = pd;
    pthread_mutex_unlock(&g_flp_mu);
    return 0;
}

/* Background pre-warm: register ALL ring slots against cm. Spawned (detached)
 * from the FIRST CHAIN-PREP (the CHAIN-WARM one) so the K * ~340 ms ibv_reg_mr
 * cost is paid during the pre-reshard pause, off the main thread; real sessions
 * then reuse with no in-window registration. */
typedef struct { struct rdma_cm_id *cm; size_t bytes; } flpPrewarmArg;
static void *followerPrewarmThread(void *arg) {
    flpPrewarmArg *a = arg;
    int done = 0;
    for (int i = 0; i < N_FOLLOWER_LANDING_POOLS; i++)
        if (followerEnsurePool(i, a->cm, a->bytes) == 0) done++;
    serverLog(LL_NOTICE,
        "CHAIN-PREP: follower pre-warmed %d/%d landing pools (~%zu B each, off-main)",
        done, N_FOLLOWER_LANDING_POOLS, a->bytes);
    zfree(a);
    return NULL;
}

/* Startup pre-registration of the follower ring on the device's shared PD (the
 * keeper cm_id pins it), off the main thread and before any client traffic, so no
 * CHAIN-PREP — not even the first — registers memory on the main thread. Sized
 * like the leader's request for rdma-landing-prereg-slots slots (grain-rounded). */
static void *followerPreregThread(void *arg) {
    UNUSED(arg);
    struct rdma_cm_id *keeper = rdmaPreregKeeperGet();
    if (keeper == NULL) {
        serverLog(LL_WARNING, "CHAIN FOLLOWER-PREREG: no RDMA device for the keeper cm_id; "
                  "follower pools will be registered lazily");
        return NULL;
    }
    size_t bytes = ((size_t) server.rdma_landing_prereg_slots + 2) * (size_t) RDMAMIG_BLOCK_SIZE_BYTES;
    long long t0 = ustime();
    int done = 0;
    for (int i = 0; i < N_FOLLOWER_LANDING_POOLS; i++)
        if (followerEnsurePool(i, keeper, bytes) == 0) done++;
    pthread_mutex_lock(&g_flp_mu);
    g_flp_prewarmed = 1;
    pthread_mutex_unlock(&g_flp_mu);
    serverLog(LL_NOTICE, "CHAIN FOLLOWER-PREREG: %d/%d follower pools ready (%zu B each, pd=%p) "
              "in %lld ms [startup, off-main]", done, N_FOLLOWER_LANDING_POOLS, bytes,
              rdmamig_cm_pd(keeper), (ustime() - t0) / 1000);
    return NULL;
}

void rdmaFollowerPreregStart(void) {
    if (server.rdma_landing_prereg_pools <= 0 || server.rdma_landing_prereg_slots <= 0) return;
    pthread_t tid;
    if (pthread_create(&tid, NULL, followerPreregThread, NULL) == 0) pthread_detach(tid);
    else serverLog(LL_WARNING, "CHAIN FOLLOWER-PREREG: pthread_create failed; pools registered lazily");
}

/*
 * RDMA CHAIN-PREP <src_mig_id> <pool_bytes>
 *
 * Issued by the recipient leader to each follower at session start. Claims a
 * pre-registered ring pool (round-robin) and replies with its (addr, rkey,
 * bytes) so the upstream peer can RDMA-WRITE this session's blocks into it.
 *
 * Reply format (multi-bulk array, 4 elements):
 *   1. status string ("CHAIN-PREP-OK" on success, error otherwise)
 *   2. landing-pool addr (integer)
 *   3. landing-pool rkey (integer)
 *   4. landing-pool bytes (integer)
 */
void rdmaChainPrepCommand(client *c) {
    long long src_mig_id, pool_bytes;
    if (getLongLongFromObjectOrReply(c, c->argv[2], &src_mig_id, NULL) != C_OK)
        return;
    if (getLongLongFromObjectOrReply(c, c->argv[3], &pool_bytes, NULL) != C_OK)
        return;
    if (pool_bytes <= 0) {
        addReplyError(c, "CHAIN-PREP: pool_bytes must be positive");
        return;
    }
    /* AqRaft piggyback: chain session ids restart on every new sg4 leader, so a
     * receipt recorded for an earlier chain with the same id must not be reported
     * for this one. Forget it before any data for the new chain can arrive (the
     * forget and the later receipt go through the same ordered loopback). */
    rdmaMgnReceivedAsync(src_mig_id, -1, 0);

    pthread_mutex_lock(&g_chain_state_mu);
    rdmaFollowerChainState *st = findFollowerState(src_mig_id);
    if (st == NULL) {
        size_t bytes = (size_t) pool_bytes;
        /* AqRaft: claim a PRE-REGISTERED ring pool (round-robin) instead of a
         * fresh mmap+ibv_reg_mr — that ~340 ms registration is exactly what gated
         * the leader->F1 forward. K > sessions/migration ⇒ no within-migration
         * reuse, so no refcount is needed. On the first CHAIN-PREP (the CHAIN-WARM
         * one) we also kick off a background pre-warm of the whole ring so real
         * sessions reuse with zero in-window registration.
         *
         * On hardware-less dev machines server.rdma_server is NULL → leave
         * rkey=0 (control-plane / pytest still pass; chain RDMA-WRITE won't). */
        struct rdma_cm_id *cm = (server.rdma_server != NULL)
                              ? rdmamig_server_cm_id(server.rdma_server) : NULL;
        int idx, need_prewarm;
        pthread_mutex_lock(&g_flp_mu);
        idx = g_flp_next++ % N_FOLLOWER_LANDING_POOLS;
        need_prewarm = (!g_flp_prewarmed && cm != NULL);
        if (need_prewarm) g_flp_prewarmed = 1;
        pthread_mutex_unlock(&g_flp_mu);

        st = zcalloc(sizeof(*st));
        st->src_mig_id = src_mig_id;
        st->landing_pool_bytes = bytes;

        long long t_ens = ustime();
        int ens_rc = (cm != NULL) ? followerEnsurePool(idx, cm, bytes) : -1;
        long long ens_ms = (ustime() - t_ens) / 1000;
        if (ens_ms >= 50)
            serverLog(LL_WARNING, "CHAIN-PREP: sess=%lld ring pool[%d] registered on the main "
                      "thread (%lld ms) — not pre-registered for this PD", src_mig_id, idx, ens_ms);
        if (ens_rc == 0) {
            pthread_mutex_lock(&g_flp_mu);
            st->landing_pool      = g_flp_pool[idx];
            st->landing_pool_buf  = g_flp_buf[idx];
            st->landing_pool_addr = (uint64_t) (uintptr_t) g_flp_pool[idx];
            st->landing_pool_rkey = rdmamig_buffer_rkey(g_flp_buf[idx]);
            /* AqRaft fix: advertise the ACTUAL pool capacity (FLP_GRAIN rounds the
             * request up to a 64 MiB boundary, so the real pool is larger than the
             * request), NOT the requested bytes. The leader gates each forward on
             * `length <= peer_pool_bytes`; advertising the smaller request made it
             * reject a session whose total_blocks exceeded the request by even one
             * block (a multi-block / fat slot → 683 blocks vs a 682-block request),
             * even though the registered pool had ample room. */
            st->landing_pool_bytes = g_flp_bytes[idx];
            pthread_mutex_unlock(&g_flp_mu);
            serverLog(LL_NOTICE,
                "CHAIN-PREP: sess=%lld using ring pool[%d] @ %p (%zu B) rkey=0x%x",
                src_mig_id, idx, st->landing_pool, bytes, st->landing_pool_rkey);
        } else {
            /* No cm (degraded) or registration failed: rkey=0, chain WRITE no-ops. */
            st->landing_pool = NULL; st->landing_pool_buf = NULL;
            st->landing_pool_addr = 0; st->landing_pool_rkey = 0;
            if (cm != NULL)
                serverLog(LL_WARNING,
                    "CHAIN-PREP: sess=%lld ring pool[%d] register failed", src_mig_id, idx);
        }

        /* Pre-warm the rest of the ring off-thread during the pre-reshard pause. */
        if (need_prewarm) {
            flpPrewarmArg *a = zmalloc(sizeof(*a));
            a->cm = cm; a->bytes = bytes;
            pthread_t tid;
            if (pthread_create(&tid, NULL, followerPrewarmThread, a) == 0)
                pthread_detach(tid);
            else zfree(a);
        }

        if (insertFollowerState(st) != C_OK) {
            pthread_mutex_unlock(&g_chain_state_mu);
            zfree(st);   /* do NOT free the ring pool — it's shared + persistent */
            addReplyError(c, "CHAIN-PREP: too many concurrent chain sessions");
            return;
        }
    } else if (st->applied) {
        /* AqRaft: re-delivery of a session this follower already applied (a
         * promoted leader re-forwards a recovered session under the same id).
         * Its pool now backs live keys — hand out a FRESH pool, or the upstream's
         * RDMA-write overwrites them (S1: follower SIGSEGV in siphash on the next
         * SET). The re-delivered slots are already registered, so the apply skips
         * them (g_chain_landing_registered); only slots never applied here land. */
        struct rdma_cm_id *cm = (server.rdma_server != NULL)
                              ? rdmamig_server_cm_id(server.rdma_server) : NULL;
        pthread_mutex_lock(&g_flp_mu);
        int idx = g_flp_next++ % N_FOLLOWER_LANDING_POOLS;
        pthread_mutex_unlock(&g_flp_mu);
        if (cm == NULL || followerEnsurePool(idx, cm, (size_t) pool_bytes) != 0) {
            pthread_mutex_unlock(&g_chain_state_mu);
            addReplyErrorFormat(c, "CHAIN-PREP: sess=%lld re-delivery: no fresh pool", src_mig_id);
            return;
        }
        pthread_mutex_lock(&g_flp_mu);
        st->landing_pool      = g_flp_pool[idx];
        st->landing_pool_buf  = g_flp_buf[idx];
        st->landing_pool_addr = (uint64_t) (uintptr_t) g_flp_pool[idx];
        st->landing_pool_rkey = rdmamig_buffer_rkey(g_flp_buf[idx]);
        st->landing_pool_bytes = g_flp_bytes[idx];
        pthread_mutex_unlock(&g_flp_mu);
        st->applied = 0;
        st->apply_state = 0;
        /* A new chain under a reused session id (ids restart on every new
         * leader): its recipes start again at attempt 0, and a claim left by
         * the previous chain's sender means nothing to it. */
        st->last_attempt_p1 = 0;
        st->claim_attempt_p1 = 0;
        serverLog(LL_NOTICE,
            "CHAIN-PREP: sess=%lld already applied here — re-delivery gets fresh ring pool[%d] @ %p",
            src_mig_id, idx, st->landing_pool);
    } else if (st->landing_pool_bytes < (size_t) pool_bytes) {
        /* Repeated CHAIN-PREP: only an error if the already-claimed pool is too
         * SMALL for the new request. st->landing_pool_bytes now holds the actual
         * (grain-rounded) capacity, which is >= the original request, so a retry
         * asking for the same-or-smaller size is fine. */
        pthread_mutex_unlock(&g_chain_state_mu);
        addReplyErrorFormat(c,
            "CHAIN-PREP: repeated call for sess=%lld asks for more than the "
            "claimed pool (have=%zu, asked=%lld)",
            src_mig_id, st->landing_pool_bytes, pool_bytes);
        return;
    }
    uint64_t addr = st->landing_pool_addr;
    uint32_t rkey = st->landing_pool_rkey;
    size_t bytes = st->landing_pool_bytes;
    pthread_mutex_unlock(&g_chain_state_mu);

    addReplyArrayLen(c, 4);
    addReplyBulkCString(c, "CHAIN-PREP-OK");
    addReplyLongLong(c, (long long) addr);
    addReplyLongLong(c, (long long) rkey);
    addReplyLongLong(c, (long long) bytes);

    serverLog(LL_NOTICE,
        "RDMA CHAIN-PREP: sess=%lld pool_bytes=%zu addr=0x%llx rkey=0x%x%s",
        src_mig_id, bytes, (unsigned long long) addr, rkey,
        rkey == 0 ? " (no upstream connect yet → chain RDMA-WRITE will fail)" : "");
}

/*
 * RDMA CHAIN-WIRE <src_mig_id> <position> <n_peers>
 *                 <pred_host> <pred_port>
 *                 <succ_host> <succ_port> <succ_rdma_port>
 *                 <succ_addr> <succ_rkey>
 *                 <leader_host> <leader_port>
 *
 * Issued by the recipient leader to each follower after all CHAIN-PREPs
 * have replied. Conveys this follower's position in the chain, its
 * successor's (host, port, rdma_port, addr, rkey), and the leader's
 * (host, port) so the tail can send CHAIN-ACK directly back. For
 * non-tail followers, opens an outgoing RDMA QP via rdmamig_client_create
 * + connect to the successor.
 */
void rdmaChainWireCommand(client *c) {
    long long src_mig_id, position, n_peers, pred_port;
    long long succ_port, succ_rdma_port, succ_addr, succ_rkey, leader_port;
    if (getLongLongFromObjectOrReply(c, c->argv[2],  &src_mig_id,     NULL) != C_OK) return;
    if (getLongLongFromObjectOrReply(c, c->argv[3],  &position,       NULL) != C_OK) return;
    if (getLongLongFromObjectOrReply(c, c->argv[4],  &n_peers,        NULL) != C_OK) return;
    /* argv[5]  pred_host (string) */
    if (getLongLongFromObjectOrReply(c, c->argv[6],  &pred_port,      NULL) != C_OK) return;
    /* argv[7]  succ_host (string) */
    if (getLongLongFromObjectOrReply(c, c->argv[8],  &succ_port,      NULL) != C_OK) return;
    if (getLongLongFromObjectOrReply(c, c->argv[9],  &succ_rdma_port, NULL) != C_OK) return;
    if (getLongLongFromObjectOrReply(c, c->argv[10], &succ_addr,      NULL) != C_OK) return;
    if (getLongLongFromObjectOrReply(c, c->argv[11], &succ_rkey,      NULL) != C_OK) return;
    /* argv[12] leader_host (string) */
    if (getLongLongFromObjectOrReply(c, c->argv[13], &leader_port,    NULL) != C_OK) return;

    sds succ_host = sdsdup(c->argv[7]->ptr);
    sds leader_host = sdsdup(c->argv[12]->ptr);

    pthread_mutex_lock(&g_chain_state_mu);
    rdmaFollowerChainState *st = findFollowerState(src_mig_id);
    if (st == NULL) {
        pthread_mutex_unlock(&g_chain_state_mu);
        sdsfree(succ_host);
        sdsfree(leader_host);
        addReplyErrorFormat(c,
            "CHAIN-WIRE: no CHAIN-PREP state for sess=%lld",
            src_mig_id);
        return;
    }

    st->chain_position = (int) position;
    st->is_tail = (position == n_peers);

    /* The link and source registration in this state belong to the successor it
     * was wired to before. A session id is reused across leaders, and a new
     * chain may give this follower a DIFFERENT successor: keeping the old link
     * would send the new successor's blocks (its address and rkey) down the
     * connection to the old one. Drop them; the link to the new successor is
     * opened below, or on demand when the recipe is followed. */
    if (st->successor_client != NULL &&
        (st->successor_host == NULL || st->successor_port != (int) succ_port ||
         strcmp(st->successor_host, succ_host) != 0)) {
        serverLog(LL_NOTICE, "RDMA CHAIN-WIRE: sess=%lld successor changed %s:%d -> %s:%lld — "
                  "dropping the old link from this session", src_mig_id,
                  st->successor_host ? st->successor_host : "-", st->successor_port,
                  succ_host, succ_port);
        st->successor_client = NULL;
        st->forward_src_buf = NULL;
        st->forward_src_pool = NULL;
    }

    if (st->successor_host) sdsfree(st->successor_host);
    st->successor_host = succ_host;
    st->successor_port = (int) succ_port;
    st->successor_pool_addr = (uint64_t) succ_addr;
    st->successor_pool_rkey = (uint32_t) succ_rkey;

    if (st->leader_host) sdsfree(st->leader_host);
    st->leader_host = leader_host;
    st->leader_port = (int) leader_port;

    /* Phase A.full: open the outgoing RDMA QP to the successor on a
     * dedicated chain worker thread so the main thread isn't blocked by
     * rdmamig_client_connect (which can take seconds and starves Raft
     * heartbeats — observed to trigger election storms). Push the work
     * item; the worker pops it FIFO and stores successor_client into the
     * state when the connect completes. */
    int enqueued_qp_work = 0;
    if (!st->is_tail && st->successor_client == NULL) {
        /* AqRaft Stage 2: prefer reusing a prior session's QP to this
         * successor — its rdmamig_server singleton can't accept a 2nd connect,
         * and succ_rdma_port is 0 on the reuse path (the leader skipped
         * CHAIN-INIT-QP for sess>=2), so it can't gate the open. Match the
         * reuse by the successor's redis TCP port, which CHAIN-WIRE always
         * carries. Fall back to a fresh create+connect only when no prior QP
         * exists AND we actually have an rdma_port. */
        void *reuse = findLiveSuccessorClient(succ_host, (int) succ_port,
                                              src_mig_id);
        if (reuse != NULL || succ_rdma_port > 0 || succ_port > 0) {
            ensureChainWorker();
            /* zcalloc so the new ->slots / ->n_slots / ->reuse_client fields
             * default to NULL/0; the worker frees ->slots only when non-NULL. */
            chainWorkItem *item = zcalloc(sizeof(*item));
            item->kind = CHAIN_WORK_OPEN_SUCC_QP;
            item->src_mig_id = src_mig_id;
            item->host = sdsdup(succ_host);
            item->port = (int) succ_rdma_port;
            item->tcp_port = (int) succ_port;
            item->reuse_client = reuse;
            item->next = NULL;
            chainWorkPush(item);
            enqueued_qp_work = 1;
        }
    }
    int is_tail = st->is_tail;
    int qp_up = (st->successor_client != NULL);
    pthread_mutex_unlock(&g_chain_state_mu);

    serverLog(LL_NOTICE,
        "RDMA CHAIN-WIRE: sess=%lld pos=%lld/%lld "
        "pred=%s:%lld succ=%s:%lld rdma=%lld (addr=0x%llx rkey=0x%llx) "
        "leader=%s:%lld tail=%d qp_up=%d qp_work_enqueued=%d",
        src_mig_id, position, n_peers,
        (char *) c->argv[5]->ptr, pred_port,
        (char *) c->argv[7]->ptr, succ_port, succ_rdma_port,
        (unsigned long long) succ_addr, (unsigned long long) succ_rkey,
        (char *) c->argv[12]->ptr, leader_port,
        is_tail, qp_up, enqueued_qp_work);

    addReply(c, shared.ok);
}

/*
 * RDMA CHAIN-FORWARDED <src_mig_id> <n_slots> <slot_0> <slot_1> ... <slot_n>
 *
 * Pass-through chain hop. Sent over TCP by an upstream peer (leader → F1,
 * or Fk → F(k+1)) AFTER it has RDMA-WRITTEN <n_slots * RDMAMIG_BLOCK_SIZE_BYTES>
 * into this follower's landing pool. The payload is laid out as N raw 2 MiB
 * slot blocks at offset i * RDMAMIG_BLOCK_SIZE_BYTES, each one in the same
 * format the donor produced (uint32_t n_entries header + (klen, key, vlen,
 * value) tuples). slot_ids[i] is the slot id for the i-th 2 MiB block.
 *
 * On receipt this follower:
 *   1. Per-slot decode+install via rdmaApplySlotBlock.
 *   2. If not the chain tail, enqueues a CHAIN_WORK_FORWARD item carrying
 *      the same slot list so the worker cascades downstream.
 *   3. If the tail, enqueues a CHAIN_WORK_ACK_LEADER item.
 */
/* AqRaft Patch 16(E): chain follower apply job + worker.
 *
 * Used to live inline on the follower's main thread (1365 slots ×
 * r_allocator_walk_used_segments + kvstoreDictAddRaw → hundreds of ms of
 * CPU). That stall caused the follower to miss AppendEntries acks, which
 * (combined with the leader's own main-thread stalls fixed by Patches 15
 * and 16(A-D)) contributed to the sg4 leader's check-quorum step-down.
 *
 * The worker iterates the slot list, wraps each rdmaApplySlotBlock in
 * clusterSlotLockWriteNoTopology(slot) ... clusterSlotUnlockNoTopology(slot)
 * so that concurrent raft-apply on main thread (for YCSB SETs in the
 * same slot) serialises against our insertion.
 *
 * Ownership: worker frees its job + slots array. local_pool is borrowed
 * (lives in the follower's chain state for the session lifetime). */
typedef struct {
    redisDb     *db;
    long long    src_mig_id;
    int         *slots;        /* owned: zfree on exit */
    int          n_slots;
    void        *local_pool;
    size_t       pool_bytes;
} chainApplyJob;

/* AqRaft fix: per-slot flag set the first time we register a chain LANDING block
 * for a slot. Used to skip duplicate cross-round deliveries (round 2 re-ships
 * round-1 slots with empty data → a 2nd register_existing_block on the same slot
 * corrupts the allocator). Precise on purpose: only landing-block registration
 * sets it, so managed blocks (raft-applied client writes / Part-1 copy-outs) do
 * NOT mask a legitimate round-2 apply of a fresh slot. Reset per process; the
 * rounds of one migration share these flags. */
static unsigned char g_chain_landing_registered[CLUSTER_SLOTS];

void rdmaMergeResizeHold(int delta);   /* cluster_rdma.c */

/* Follower merge on several threads. One thread per round took ~1.2 s for 2.5M
 * keys, and a follower only starts after it has forwarded the round onward, so
 * the three rounds of a 3 -> 4 migration queued up and the followers finished
 * 3.4 s after the leader -- with the recipient group's replication slowed the
 * whole time (throughput ~5% lower for 3 s after the migration, 2026-10-05).
 * Each thread takes every K-th slot; the per-slot work is the same the leader's
 * pool workers run concurrently for different slots. Registration of the
 * landing blocks stays on the calling thread. */
typedef struct chainMergePart {
    chainApplyJob *job;
    const int *slots;
    int n, k, K;
    _Atomic int *staged;
} chainMergePart;

static void *chainApplyMergeThread(void *arg) {
    chainMergePart *p = arg;
    for (int i = p->k; i < p->n; i += p->K) {
        int slot = p->slots[i];
        int st = server.rdma_merge_background
            ? rdmaFollowerMergeSlotBackground(p->job->db, slot,
                            (const char *) p->job->local_pool, p->job->pool_bytes)
            : rdmaFollowerEnqueueSlotMerge(p->job->db, slot,
                            (const char *) p->job->local_pool, p->job->pool_bytes);
        atomic_fetch_add_explicit(p->staged, st, memory_order_relaxed);
    }
    return NULL;
}

static void *chainApplyWorker(void *arg) {
    chainApplyJob *job = arg;
    size_t length = (size_t) job->n_slots * (size_t) RDMAMIG_BLOCK_SIZE_BYTES;
    rdmaMergeResizeHold(1);    /* no dict resize/rehash while this thread merges */
    int total_staged = 0;
    int any_registered = 0;
    if (job->local_pool != NULL && length <= job->pool_bytes && job->n_slots > 0) {
        /* AqRaft Patch 29: converge the follower apply onto the proven leader
         * design. For each slot: (1) register the landing-pool slice as an
         * r_allocator-owned block (mirrors the leader's REGISTER-BLOCK-SLOTS
         * path; without this the migrated kvobjs live in unmanaged memory and
         * the free/coalesce path corrupts them), then (2) build the shadow
         * off-main + hand it to the main-thread merge queue so the live
         * keyspace insert is serialized with raft-apply of client writes on
         * the event loop. Replaces the old off-main direct kvstoreDictAddRaw
         * (rdmaApplySlotBlock) which raced the main thread (the per-slot
         * cluster lock is a no-op under redisraft) and installed kvobjs into
         * an unregistered pool. */
        /* AqRaft Stage 3: the position list groups a fat slot's blocks into a
         * consecutive RUN of equal slot ids (the leader appends them contiguously
         * in Phase C and forwards covered_slots in order). Register EVERY block in
         * the run so a multi-block slot is fully reconstructed on this follower
         * over RDMA — no raft-log fallback.
         *
         * The g_chain_landing_registered[slot] guard stays, but now gates the
         * whole slot, not a single block: never register a slot's landing blocks
         * a SECOND time across deliveries. In multi-round migration the chain
         * forward can re-deliver an earlier round's slots (observed: round-2
         * forwarding round-1 slots with empty/garbage data). Linking duplicate
         * blocks left stale blocks whose later walk/sanitize/free corrupted the
         * heap → intermittent recipient-follower SIGSEGV. Skipping the whole run
         * for an already-registered slot preserves that protection. The flag is
         * set ONLY by landing-block registration, so a round-2 slot that merely
         * picked up a managed block from a raft-applied client write is still
         * applied. */
        int *todo = zmalloc(sizeof(int) * (size_t) job->n_slots);
        int n_todo = 0;
        for (int i = 0; i < job->n_slots; ) {
            int slot = job->slots[i];
            int run = 1;
            while (i + run < job->n_slots && job->slots[i + run] == slot) run++;
            if (slot < 0 || slot >= CLUSTER_SLOTS) { i += run; continue; }
            if (g_chain_landing_registered[slot]) {
                serverLog(LL_WARNING,
                    "CHAIN apply: slot=%d landing block(s) already registered — "
                    "skipping duplicate cross-round delivery (sess=%lld, %d blocks)",
                    slot, job->src_mig_id, run);
                i += run; continue;
            }
            int reg = 0;
            for (int m = 0; m < run; m++) {
                void *sub = (char *) job->local_pool
                          + (size_t) (i + m) * RDMAMIG_BLOCK_SIZE_BYTES;
                /* The leader already RDMA-wrote the donor bytes here: register
                 * WITHOUT init_bloc_layout (it would overwrite the first segment
                 * header and hide the whole block from the walker). */
                if (r_allocator_register_filled_block(slot, sub) == NULL) {
                    serverLog(LL_WARNING,
                        "CHAIN apply: r_allocator_register_filled_block failed "
                        "(sess=%lld slot=%d block=%d/%d) — skipping block",
                        job->src_mig_id, slot, m, run);
                    continue;
                }
                reg++;
            }
            if (reg > 0) {
                /* One merge enqueue per slot drains ALL its registered blocks
                 * (the merge walks the slot's full block list). */
                g_chain_landing_registered[slot] = 1;
                any_registered = 1;
                /* Local inventory (apply-then-mark): the raw block is now
                 * physically present AND registered on this node. */
                rdmaInvMarkReceived(job->src_mig_id, slot);
                todo[n_todo++] = slot;
            }
            i += run;
        }
        {
            _Atomic int staged = 0;
            int K = server.rdma_backpatch_pool_size;
            if (K > 8) K = 8;
            if (K < 1 || !server.rdma_merge_background) K = 1;   /* the tick path keeps one thread */
            if (K > n_todo) K = n_todo > 0 ? n_todo : 1;
            chainMergePart parts[8];
            pthread_t tids[8];
            int started[8] = {0};
            for (int k = 0; k < K; k++) {
                parts[k] = (chainMergePart){ job, todo, n_todo, k, K, &staged };
                if (k > 0 && pthread_create(&tids[k], NULL, chainApplyMergeThread, &parts[k]) == 0)
                    started[k] = 1;
            }
            chainApplyMergeThread(&parts[0]);
            for (int k = 1; k < K; k++) {
                if (started[k]) pthread_join(tids[k], NULL);
                else chainApplyMergeThread(&parts[k]);      /* could not spawn: do its share here */
            }
            total_staged = atomic_load(&staged);
        }
        zfree(todo);
        /* The pool now backs live keys (merged in place): never hand it out again. */
        if (any_registered) followerRetirePool(job->local_pool);
        /* Slots skipped above (already registered / nothing registered) were marked
         * active at CHAIN-FORWARDED too: clear them (idempotent for merged ones). */
        if (server.rdma_merge_background)
            for (int k = 0; k < job->n_slots; k++) bgMergeSlotSetActive(job->slots[k], 0);
        serverLog((total_staged == 0 && any_registered) ? LL_WARNING : LL_NOTICE,
            "CHAIN apply: sess=%lld n_slots=%d staged=%d "
            "consumed=%zu/%zu bytes [register+main-merge, Patch 29]%s",
            job->src_mig_id, job->n_slots, total_staged, length, job->pool_bytes,
            (total_staged == 0 && any_registered)
                ? " — NO keys in the delivered blocks: the predecessor's write did not land" : "");
    }
    /* GATE: this node's blocks are stable again, so it may serve as a source
     * for a later repair of this session. */
    pthread_mutex_lock(&g_chain_state_mu);
    {
        rdmaFollowerChainState *ast = findFollowerState(job->src_mig_id);
        if (ast != NULL) ast->apply_state = 2;
    }
    pthread_mutex_unlock(&g_chain_state_mu);
    rdmaMergeResizeHold(-1);
    zfree(job->slots);
    zfree(job);
    return NULL;
}

/* Run a chainApplyJob on a detached thread (inline if pthread_create fails). */
static void chainSpawnApply(void *job) {
    pthread_t tid;
    if (pthread_create(&tid, NULL, chainApplyWorker, job) != 0) {
        serverLog(LL_WARNING,
            "CHAIN apply: pthread_create(chainApplyWorker) failed for sess=%lld "
            "— running inline", ((chainApplyJob *) job)->src_mig_id);
        chainApplyWorker(job);   /* frees job */
    } else {
        pthread_detach(tid);
    }
}

void rdmaChainForwardedCommand(client *c) {
    if (c->argc < 4) {
        addReplyError(c,
            "syntax: RDMA CHAIN-FORWARDED <sess> <n_slots> <slot_0> ...");
        return;
    }
    long long src_mig_id, n_slots_ll;
    if (getLongLongFromObjectOrReply(c, c->argv[2], &src_mig_id, NULL) != C_OK) return;
    if (getLongLongFromObjectOrReply(c, c->argv[3], &n_slots_ll, NULL) != C_OK) return;
    if (n_slots_ll < 0 || n_slots_ll > CLUSTER_SLOTS) {
        addReplyErrorFormat(c, "CHAIN-FORWARDED: bad n_slots=%lld", n_slots_ll);
        return;
    }
    int n_slots = (int) n_slots_ll;
    /* Optional trailer: RECIPE <attempt> <k> (<host> <port>)*k — the followers
     * after this one, in order (see "Chain recipes"). */
    int has_recipe = 0, attempt = 0, n_recipe = 0;
    int rbase = 4 + n_slots;
    if (c->argc > rbase) {
        long long a = -1, k = -1;
        if (c->argc < rbase + 3 || strcasecmp(c->argv[rbase]->ptr, "RECIPE") != 0 ||
            getLongLongFromObject(c->argv[rbase + 1], &a) != C_OK ||
            getLongLongFromObject(c->argv[rbase + 2], &k) != C_OK ||
            a < 0 || a > 1000000 || k < 0 || k > CLUSTER_NAMELEN ||
            c->argc != rbase + 3 + 2 * (int) k) {
            addReplyError(c, "CHAIN-FORWARDED: malformed RECIPE trailer");
            return;
        }
        for (int i = 0; i < (int) k; i++) {
            long long p;
            if (getLongLongFromObject(c->argv[rbase + 3 + 2 * i + 1], &p) != C_OK ||
                p <= 0 || p > 65535) {
                addReplyError(c, "CHAIN-FORWARDED: bad port in RECIPE");
                return;
            }
        }
        has_recipe = 1; attempt = (int) a; n_recipe = (int) k;
    } else if (c->argc != rbase) {
        addReplyErrorFormat(c,
            "CHAIN-FORWARDED: argc=%d expected %d (4 header + %d slot ids)",
            c->argc, 4 + n_slots, n_slots);
        return;
    }
    /* Parse slot ids. */
    int *slots = (n_slots > 0) ? zmalloc((size_t) n_slots * sizeof(int)) : NULL;
    for (int i = 0; i < n_slots; i++) {
        long long s;
        if (getLongLongFromObject(c->argv[4 + i], &s) != C_OK ||
            s < 0 || s >= CLUSTER_SLOTS) {
            if (slots) zfree(slots);
            addReplyErrorFormat(c, "CHAIN-FORWARDED: bad slot id at argv[%d]", 4 + i);
            return;
        }
        slots[i] = (int) s;
    }
    size_t length = (size_t) n_slots * (size_t) RDMAMIG_BLOCK_SIZE_BYTES;

    pthread_mutex_lock(&g_chain_state_mu);
    rdmaFollowerChainState *st = findFollowerState(src_mig_id);
    if (st == NULL) {
        pthread_mutex_unlock(&g_chain_state_mu);
        if (slots) zfree(slots);
        addReplyErrorFormat(c, "CHAIN-FORWARDED: no chain state for sess=%lld", src_mig_id);
        return;
    }
    int is_tail = st->is_tail;
    if (has_recipe) {
        /* Each attempt is handled once; an older attempt's token is stale (the
         * leader has re-issued the recipe since). */
        if (attempt + 1 <= st->last_attempt_p1) {
            int newest = st->last_attempt_p1 - 1;
            pthread_mutex_unlock(&g_chain_state_mu);
            if (slots) zfree(slots);
            serverLog(LL_NOTICE, "CHAIN-FORWARDED: sess=%lld recipe attempt=%d ignored "
                      "(already handled attempt %d)", src_mig_id, attempt, newest);
            addReply(c, shared.ok);
            return;
        }
        st->last_attempt_p1 = attempt + 1;
        /* With a recipe, "tail" means nobody is left after us in THIS recipe,
         * whatever the position we were wired at. */
        is_tail = (n_recipe == 0);
    }
    /* The batch is here: whoever claimed our landing pool is done writing it. */
    st->claim_attempt_p1 = 0;
    /* AqRaft majority-commit: EVERY follower reports to the leader once it has
     * the batch in its landing pool (not just the tail), identified by the
     * position CHAIN-WIRE gave it. The leader commits once f distinct followers
     * of its 2f+1 group have reported, without waiting for the rest of the
     * chain. Non-tail followers ALSO still forward down the chain. */
    int my_position = st->chain_position;
    /* Snapshot landing pool ptr so we can apply locally after dropping the
     * state mutex. The pool is mmap'd at PREP time and stable for the
     * session's lifetime. */
    void *local_pool = st->landing_pool;
    size_t pool_bytes = st->landing_pool_bytes;
    /* AqRaft Patch 29: claim the apply for this session. A retried
     * CHAIN-FORWARDED must NOT re-register the landing-pool slices (that
     * would leak duplicate alloc_bloc_t nodes and double-stage the slots). */
    int already_applied = st->applied;
    st->applied = 1;
    if (!already_applied) {
        st->apply_state = 1;   /* pending until chainApplyWorker ends */
        if (st->slots) zfree(st->slots);
        st->slots = (n_slots > 0) ? zmalloc((size_t) n_slots * sizeof(int)) : NULL;
        if (st->slots) memcpy(st->slots, slots, (size_t) n_slots * sizeof(int));
        st->n_slots = n_slots;
    }
    pthread_mutex_unlock(&g_chain_state_mu);

    /* AqRaft Patch 16(E): per-slot apply (1365 slots × walk_used_segments
     * + kvstoreDictAddRaw) moves OFF the follower main thread into a
     * one-shot detached pthread chainApplyWorker. Main thread proceeds
     * straight to enqueueing the forward to the next chain peer (or the
     * tail ack), then replies OK. The follower's event loop stays free
     * to send AppendEntries acks to the sg4 leader. */
    int apply_spawned = 0;
    chainApplyJob *deferred_job = NULL;
    if (already_applied) {
        serverLog(LL_NOTICE,
            "CHAIN-FORWARDED: sess=%lld already applied — skipping re-apply "
            "(retry); forwarding/ack only", src_mig_id);
    } else if (local_pool != NULL && length <= pool_bytes && n_slots > 0) {
        /* Background follower merge: mark the slots active HERE, on the main
         * thread, before any apply can touch them, so main-thread accessors of
         * these slots take the per-slot lock from now on (the leader does the
         * same at DONE-SLOTS-CHUNK). The apply clears each slot once merged. */
        if (server.rdma_merge_background)
            for (int k = 0; k < n_slots; k++) bgMergeSlotSetActive(slots[k], 1);
        chainApplyJob *job = zcalloc(sizeof(*job));
        job->db          = c->db;
        job->src_mig_id  = src_mig_id;
        job->slots       = zmalloc((size_t) n_slots * sizeof(int));
        memcpy(job->slots, slots, (size_t) n_slots * sizeof(int));
        job->n_slots     = n_slots;
        job->local_pool  = local_pool;
        job->pool_bytes  = pool_bytes;
        /* AqRaft: run the apply on a BACKGROUND thread (detached chainApplyWorker)
         * so the follower's event loop stays free to send AppendEntries acks to
         * the sg4 leader during the migration window (per-entry apply over ~1365
         * slots would otherwise stall raft heartbeats on the main thread).
         *
         * Thread-safety against the main thread's r_allocator_insert_kv (which
         * raft-applies client SETs to the same migrating slots) is provided by
         * per-slot locking on the recursive r_allocator.mutexes[slot]: held
         * inside r_allocator_register_existing_block for the block-list append,
         * and across the whole block-walk / sanitize / freelist-reset critical
         * section inside rdmaBackpatchSlotFillShadow. Without it, those concurrent
         * allocator mutations corrupted the heap (recursive SIGSEGV at 400 YCSB
         * threads). On spawn failure, run the same worker inline. */
        /* AqRaft forward gate: a non-tail follower forwards this same landing
         * pool downstream; apply only after that forward is over (the FORWARD
         * work item spawns it). The tail applies now. */
        if (!is_tail) {
            deferred_job = job;
        } else {
            chainSpawnApply(job);
            apply_spawned = 1;
        }
    } else if (length > pool_bytes) {
        serverLog(LL_WARNING,
            "CHAIN apply: sess=%lld length %zu > pool %zu — dropping",
            src_mig_id, length, pool_bytes);
    }
    (void) apply_spawned;

    /* If not tail, ALWAYS enqueue forward — the worker pops FIFO, so any
     * pending OPEN_SUCC_QP enqueued by CHAIN-WIRE is processed first and
     * successor_client will be live by the time the worker handles this
     * FORWARD. */
    int ack_enqueued = 0;
    serverLog(LL_NOTICE,
        "RDMA CHAIN-FORWARDED: sess=%lld length=%zu n_slots=%d tail=%d "
        "(forward enqueued=%d)",
        src_mig_id, length, n_slots, is_tail, !is_tail);

    /* AqRaft majority-commit: this follower has the batch in its landing pool,
     * so report it NOW — regardless of tail position. The report rides on this
     * node's Raft AppendEntries replies (RAFT.MGN-RECEIVED); the leader counts
     * distinct followers. */
    rdmaMgnReceivedAsync(src_mig_id, (long long) length, my_position);
    ack_enqueued = 1;

    /* Non-tail followers ALSO forward down the chain so the tail eventually
     * receives the data (full durability still converges to all followers;
     * the majority ack above just unblocks the leader earlier). */
    if (!is_tail) {
        ensureChainWorker();
        chainWorkItem *item = zcalloc(sizeof(*item));
        item->kind = CHAIN_WORK_FORWARD;
        item->src_mig_id = src_mig_id;
        item->length = length;
        item->slots = slots;  /* worker takes ownership */
        item->n_slots = n_slots;
        item->deferred_apply = deferred_job;   /* spawned after the forward */
        deferred_job = NULL;
        if (has_recipe) {
            item->has_recipe = 1;
            item->attempt = attempt;
            item->n_recipe = n_recipe;
            item->recipe_hosts = zmalloc((size_t) n_recipe * sizeof(sds));
            item->recipe_ports = zmalloc((size_t) n_recipe * sizeof(int));
            for (int i = 0; i < n_recipe; i++) {
                long long p = 0;
                item->recipe_hosts[i] = sdsdup(c->argv[rbase + 3 + 2 * i]->ptr);
                getLongLongFromObject(c->argv[rbase + 3 + 2 * i + 1], &p);
                item->recipe_ports[i] = (int) p;
            }
        }
        slots = NULL;
        chainWorkPush(item);
    }
    if (deferred_job != NULL) chainSpawnApply(deferred_job);   /* not reached: !is_tail forwards */
    if (is_tail) {
        serverLog(LL_NOTICE,
            "RDMA CHAIN-FORWARDED: sess=%lld tail ack_enqueued=%d",
            src_mig_id, ack_enqueued);
    }
    /* If `slots` wasn't taken by a forward worker item (tail path), free it. */
    if (slots) zfree(slots);

    addReply(c, shared.ok);
}

static void chainMarkForwarded(long long src_mig_id) {
    pthread_mutex_lock(&g_chain_state_mu);
    rdmaLeaderChainState *ls = findLeaderState(src_mig_id);
    if (ls) ls->fwd_count++;
    pthread_mutex_unlock(&g_chain_state_mu);
}

/* A follower (identified by the position CHAIN-WIRE gave it) reports that the
 * session's batch is in its landing pool. Called by the redisraft module for
 * every report it reads off an AppendEntries reply (weak symbol). Returns C_ERR
 * when this leader has no such session, or has not forwarded its batch yet (a
 * stale report for an earlier session that reused the id): the module then
 * retries instead of marking the report seen. */
int rdmaLeaderChainAckFrom(long long src_mig_id, long long length, int position) {
    pthread_mutex_lock(&g_chain_state_mu);
    rdmaLeaderChainState *ls = findLeaderState(src_mig_id);
    if (ls == NULL || ls->fwd_count == 0 ||
        position < 1 || position > ls->n_wired || position > 63) {
        pthread_mutex_unlock(&g_chain_state_mu);
        return C_ERR;
    }
    ls->last_acked_length = (size_t) length;
    ls->last_acked_at_ms = mstime();
    ls->ack_count++;
    ls->acked_mask |= (1ULL << position);
    int distinct = __builtin_popcountll(ls->acked_mask);
    pthread_mutex_unlock(&g_chain_state_mu);
    serverLog(LL_NOTICE,
        "CHAIN: sess=%lld follower at position %d holds the batch (length=%lld, "
        "%d distinct followers so far)", src_mig_id, position, length, distinct);
    return C_OK;
}

/* Number of DISTINCT followers that reported holding the session's batch, or -1
 * if there is no chain state for the session. */
int rdmaLeaderChainAckedFollowers(long long src_mig_id) {
    pthread_mutex_lock(&g_chain_state_mu);
    rdmaLeaderChainState *ls = findLeaderState(src_mig_id);
    int n = (ls == NULL) ? -1 : __builtin_popcountll(ls->acked_mask);
    pthread_mutex_unlock(&g_chain_state_mu);
    return n;
}

/*
 * RDMA CHAIN-ACK <src_mig_id> <length>
 *
 * Debug/status only. It does not say which follower sent it, so it only
 * updates the DEBUG-CHAIN-STATUS counters and never counts toward the
 * durability gate; followers report through their AppendEntries replies
 * (rdmaLeaderChainAckFrom).
 */
void rdmaChainAckCommand(client *c) {
    long long src_mig_id, length;
    if (getLongLongFromObjectOrReply(c, c->argv[2], &src_mig_id, NULL) != C_OK) return;
    if (getLongLongFromObjectOrReply(c, c->argv[3], &length,     NULL) != C_OK) return;

    pthread_mutex_lock(&g_chain_state_mu);
    rdmaLeaderChainState *ls = findLeaderState(src_mig_id);
    if (ls == NULL) {
        pthread_mutex_unlock(&g_chain_state_mu);
        addReplyErrorFormat(c, "CHAIN-ACK: no leader state for sess=%lld",
                            src_mig_id);
        return;
    }
    if (ls->fwd_count == 0) {
        pthread_mutex_unlock(&g_chain_state_mu);
        addReplyErrorFormat(c, "CHAIN-ACK: forward not complete for sess=%lld",
                            src_mig_id);
        return;
    }
    ls->last_acked_length = (size_t) length;
    ls->last_acked_at_ms = mstime();
    ls->ack_count++;
    long long count = ls->ack_count;
    pthread_mutex_unlock(&g_chain_state_mu);

    serverLog(LL_NOTICE,
        "RDMA CHAIN-ACK: sess=%lld length=%lld (count=%lld)",
        src_mig_id, length, count);

    addReply(c, shared.ok);
}

/*
 * RDMA CHAIN-PING <src_mig_id> <expected_bytes>
 *
 * Phase B verification. Reads the follower's landing pool and checks that
 * the first <expected_bytes> contain the test pattern (byte[i] = i % 256).
 * The pattern is what DEBUG-CHAIN-FORWARD writes on the leader before
 * starting the chain forward. Replies +OK on match; +ERR with the first
 * mismatch byte index on failure.
 */
void rdmaChainPingCommand(client *c) {
    long long src_mig_id, expected_bytes;
    if (getLongLongFromObjectOrReply(c, c->argv[2], &src_mig_id,     NULL) != C_OK) return;
    if (getLongLongFromObjectOrReply(c, c->argv[3], &expected_bytes, NULL) != C_OK) return;

    /* expected_bytes < 0: chain repair's "do you hold this session?" (see
     * chainClaimTarget). -1 only asks; -(attempt+2) also claims the landing
     * pool for that attempt's sender. Reply: 1 = the batch is here (never write
     * this pool again), 0 = it is not (and the claim, if asked, is granted),
     * 2 = another attempt's sender holds an unexpired claim. */
    if (expected_bytes < 0) {
        int reply = 0;
        pthread_mutex_lock(&g_chain_state_mu);
        rdmaFollowerChainState *qs = findFollowerState(src_mig_id);
        if (qs != NULL && qs->applied) {
            reply = 1;
        } else if (qs != NULL && expected_bytes <= -2) {
            int want_p1 = (int) (-expected_bytes - 2) + 1;
            long long now = mstime();
            if (qs->claim_attempt_p1 != 0 && qs->claim_attempt_p1 != want_p1 &&
                now - qs->claim_ms < CHAIN_CLAIM_EXPIRE_MS) {
                reply = 2;
            } else {
                qs->claim_attempt_p1 = want_p1;
                qs->claim_ms = now;
            }
        }
        pthread_mutex_unlock(&g_chain_state_mu);
        addReplyLongLong(c, reply);
        return;
    }

    pthread_mutex_lock(&g_chain_state_mu);
    rdmaFollowerChainState *st = findFollowerState(src_mig_id);
    if (st == NULL) {
        pthread_mutex_unlock(&g_chain_state_mu);
        addReplyErrorFormat(c, "CHAIN-PING: no chain state for sess=%lld", src_mig_id);
        return;
    }
    void *pool = st->landing_pool;
    size_t bytes = st->landing_pool_bytes;
    int pos = st->chain_position;
    int is_tail = st->is_tail;
    pthread_mutex_unlock(&g_chain_state_mu);

    if (pool == NULL) {
        addReplyError(c, "CHAIN-PING: landing pool unmapped");
        return;
    }
    /* expected_bytes == 0 → state-existence check only (no byte comparison).
     * Used by pytest skeleton tests that just want to verify the chain
     * session is alive. */
    if (expected_bytes == 0) {
        serverLog(LL_NOTICE,
            "RDMA CHAIN-PING: sess=%lld pos=%d/%s state-only OK",
            src_mig_id, pos, is_tail ? "tail" : "interior");
        addReply(c, shared.ok);
        return;
    }
    size_t check_len = (size_t) expected_bytes;
    if (check_len > bytes) {
        addReplyErrorFormat(c,
            "CHAIN-PING: expected_bytes %lld > pool_bytes %zu",
            expected_bytes, bytes);
        return;
    }

    const unsigned char *p = (const unsigned char *) pool;
    size_t mismatch_at = (size_t) -1;
    for (size_t i = 0; i < check_len; i++) {
        if (p[i] != (unsigned char) (i % 256)) {
            mismatch_at = i;
            break;
        }
    }

    if (mismatch_at == (size_t) -1) {
        serverLog(LL_NOTICE,
            "RDMA CHAIN-PING: sess=%lld pos=%d/%s pattern OK over %zu bytes",
            src_mig_id, pos, is_tail ? "tail" : "interior", check_len);
        addReply(c, shared.ok);
    } else {
        unsigned int saw = p[mismatch_at];
        unsigned int want = (unsigned int) (mismatch_at % 256);
        serverLog(LL_WARNING,
            "RDMA CHAIN-PING: sess=%lld pos=%d MISMATCH at byte %zu "
            "(saw 0x%02x want 0x%02x)",
            src_mig_id, pos, mismatch_at, saw, want);
        addReplyErrorFormat(c,
            "CHAIN-PING: mismatch at byte %zu (saw 0x%02x want 0x%02x)",
            mismatch_at, saw, want);
    }
}

/* ====================================================================== *
 *  Leader-side: leaderEstablishChain (Phase A.full orchestration)       *
 * ====================================================================== */

static rdmaLeaderChainState *findLeaderState(long long src_mig_id) {
    for (int i = 0; i < RDMA_CHAIN_MAX_SESSIONS; i++) {
        if (g_leader_chains[i] && g_leader_chains[i]->src_mig_id == src_mig_id)
            return g_leader_chains[i];
    }
    return NULL;
}

/* AqRaft Stage 1 (chain transport reuse): find a LIVE leader->follower QP to
 * (host,port) from any EXISTING leader session other than `exclude_sess`. The
 * follower's rdmamig_server is a per-process singleton that accepts exactly one
 * leader connection; a 2nd rdma_connect to it hangs forever (confirmed via
 * Stage-0 diagnostics: sess=2 blocks in rdmamig_client_connect). So instead of
 * opening a new QP per migration round, round 2+ reuses round 1's QP. Returns
 * the rdmamig_client* (peer->client) or NULL if none established yet. Caller
 * holds g_chain_state_mu. NOTE: the returned client is still OWNED by the
 * original session; with no teardown today the alias is safe (the QP outlives
 * all rounds). */
static void *findLivePeerClient(const char *host, int port, long long exclude_sess) {
    for (int i = 0; i < RDMA_CHAIN_MAX_SESSIONS; i++) {
        rdmaLeaderChainState *ls = g_leader_chains[i];
        if (ls == NULL || ls->src_mig_id == exclude_sess) continue;
        for (int p = 0; p < ls->n_peers; p++) {
            if (ls->peers[p].client != NULL &&
                !chainClientBroken(ls->peers[p].client) &&
                ls->peers[p].established &&
                ls->peers[p].host != NULL &&
                ls->peers[p].port == port &&
                strcmp(ls->peers[p].host, host) == 0) {
                return ls->peers[p].client;
            }
        }
    }
    return NULL;
}

static int insertLeaderState(rdmaLeaderChainState *st) {
    for (int i = 0; i < RDMA_CHAIN_MAX_SESSIONS; i++) {
        if (g_leader_chains[i] == NULL) {
            g_leader_chains[i] = st;
            return C_OK;
        }
    }
    return C_ERR;
}

/* Send "RDMA CHAIN-INIT-QP <src_mig_id>" to (host, port). Returns the
 * follower's RDMA port on success in *out_rdma_port, or C_ERR otherwise. */
static int sendChainInitQp(const char *host, int port, long long src_mig_id,
                           int *out_rdma_port,
                           char *errbuf, size_t errbuf_len) {
    redisContext *ctx = chainConnect(host, port);   /* bounded connect + reply wait */
    if (ctx == NULL || ctx->err) {
        snprintf(errbuf, errbuf_len, "connect(%s:%d) failed: %s",
                 host, port, ctx ? ctx->errstr : "(null)");
        if (ctx) redisFree(ctx);
        return C_ERR;
    }
    redisReply *r = redisCommand(ctx, "RDMA CHAIN-INIT-QP %lld", src_mig_id);
    int rc = C_OK;
    if (r == NULL) {
        snprintf(errbuf, errbuf_len, "CHAIN-INIT-QP %s:%d: %s",
                 host, port, ctx->errstr);
        rc = C_ERR;
    } else if (r->type == REDIS_REPLY_ERROR) {
        snprintf(errbuf, errbuf_len, "CHAIN-INIT-QP %s:%d: %s",
                 host, port, r->str);
        rc = C_ERR;
    } else if (r->type != REDIS_REPLY_ARRAY || r->elements != 2 ||
               r->element[0]->type != REDIS_REPLY_STRING ||
               (strcmp(r->element[0]->str, "CHAIN-INIT-QP-OK") != 0 &&
                strcmp(r->element[0]->str, "CHAIN-INIT-QP-DEGRADED") != 0)) {
        snprintf(errbuf, errbuf_len, "CHAIN-INIT-QP %s:%d: bad reply",
                 host, port);
        rc = C_ERR;
    } else {
        *out_rdma_port = (int) r->element[1]->integer;
        if (strcmp(r->element[0]->str, "CHAIN-INIT-QP-DEGRADED") == 0) {
            serverLog(LL_NOTICE,
                "CHAIN: follower %s:%d returned DEGRADED — no RDMA hw on that node",
                host, port);
        }
    }
    if (r) freeReplyObject(r);
    redisFree(ctx);
    return rc;
}

/* Open a leader-to-follower RDMA QP via rdmamig_client_create + connect.
 * Stores the client handle into peer->client. Returns C_OK on success. */
static int leaderConnectToFollower(const char *host, int rdma_port,
                                   rdmaChainPeer *peer,
                                   char *errbuf, size_t errbuf_len) {
    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", rdma_port);
    struct rdmamig_client *cl = rdmamig_client_create(host, port_str);
    if (cl == NULL) {
        snprintf(errbuf, errbuf_len,
                 "rdmamig_client_create(%s:%s) returned NULL",
                 host, port_str);
        return C_ERR;
    }
    if (rdmamig_client_connect(cl) != 0) {
        snprintf(errbuf, errbuf_len,
                 "rdmamig_client_connect(%s:%s) failed", host, port_str);
        /* rdmamig owns the client on failure; we don't free explicitly. */
        return C_ERR;
    }
    peer->client = cl;
    serverLog(LL_NOTICE,
        "CHAIN: leader RDMA-connected to follower %s:%s (chain QP up)",
        host, port_str);
    return C_OK;
}

/* Send "RDMA CHAIN-PREP <src_mig_id> <pool_bytes>" to (host, port), parse the
 * 4-element reply, populate peer->peer_pool_addr / rkey / bytes. Returns
 * C_OK on success; on failure writes a short message into errbuf. */
static int sendChainPrep(const char *host, int port,
                         long long src_mig_id, long long pool_bytes,
                         rdmaChainPeer *peer,
                         char *errbuf, size_t errbuf_len) {
    redisContext *ctx = chainConnect(host, port);   /* bounded connect + reply wait */
    if (ctx == NULL || ctx->err) {
        snprintf(errbuf, errbuf_len, "connect(%s:%d) failed: %s",
                 host, port, ctx ? ctx->errstr : "(null)");
        if (ctx) redisFree(ctx);
        return C_ERR;
    }
    redisReply *r = redisCommand(ctx, "RDMA CHAIN-PREP %lld %lld",
                                 src_mig_id, pool_bytes);
    int rc = C_OK;
    if (r == NULL) {
        snprintf(errbuf, errbuf_len, "CHAIN-PREP %s:%d: %s", host, port, ctx->errstr);
        rc = C_ERR;
    } else if (r->type == REDIS_REPLY_ERROR) {
        snprintf(errbuf, errbuf_len, "CHAIN-PREP %s:%d: %s", host, port, r->str);
        rc = C_ERR;
    } else if (r->type != REDIS_REPLY_ARRAY || r->elements != 4 ||
               r->element[0]->type != REDIS_REPLY_STRING ||
               strcmp(r->element[0]->str, "CHAIN-PREP-OK") != 0) {
        snprintf(errbuf, errbuf_len, "CHAIN-PREP %s:%d: bad reply", host, port);
        rc = C_ERR;
    } else {
        peer->peer_pool_addr  = (uint64_t) r->element[1]->integer;
        peer->peer_pool_rkey  = (uint32_t) r->element[2]->integer;
        peer->peer_pool_bytes = (size_t)   r->element[3]->integer;
    }
    if (r) freeReplyObject(r);
    redisFree(ctx);
    return rc;
}

/* Send "RDMA CHAIN-WIRE ..." with predecessor + successor + leader info.
 * Returns C_OK on success. */
static int sendChainWire(const char *host, int port, long long src_mig_id,
                         int position, int n_peers,
                         const char *pred_host, int pred_port,
                         const char *succ_host, int succ_port,
                         int succ_rdma_port,
                         uint64_t succ_addr, uint32_t succ_rkey,
                         const char *leader_host, int leader_port,
                         char *errbuf, size_t errbuf_len) {
    redisContext *ctx = chainConnect(host, port);   /* bounded connect + reply wait */
    if (ctx == NULL || ctx->err) {
        snprintf(errbuf, errbuf_len, "connect(%s:%d) failed: %s",
                 host, port, ctx ? ctx->errstr : "(null)");
        if (ctx) redisFree(ctx);
        return C_ERR;
    }
    redisReply *r = redisCommand(ctx,
        "RDMA CHAIN-WIRE %lld %d %d %s %d %s %d %d %lld %lld %s %d",
        src_mig_id, position, n_peers,
        pred_host, pred_port,
        succ_host, succ_port, succ_rdma_port,
        (long long) succ_addr, (long long) succ_rkey,
        leader_host, leader_port);
    int rc = C_OK;
    if (r == NULL) {
        snprintf(errbuf, errbuf_len, "CHAIN-WIRE %s:%d: %s", host, port, ctx->errstr);
        rc = C_ERR;
    } else if (r->type == REDIS_REPLY_ERROR) {
        snprintf(errbuf, errbuf_len, "CHAIN-WIRE %s:%d: %s", host, port, r->str);
        rc = C_ERR;
    } else if (r->type != REDIS_REPLY_STATUS ||
               r->len != 2 || memcmp(r->str, "OK", 2) != 0) {
        snprintf(errbuf, errbuf_len, "CHAIN-WIRE %s:%d: bad reply", host, port);
        rc = C_ERR;
    }
    if (r) freeReplyObject(r);
    redisFree(ctx);
    return rc;
}

/* Orchestrate chain establishment on the recipient leader. Given a session
 * id, the size each follower should register, and an ordered list of
 * follower (host, port) pairs, drive two passes:
 *   pass 1: send CHAIN-PREP to each follower in order; collect (addr, rkey).
 *   pass 2: send CHAIN-WIRE to each follower with its predecessor + successor.
 * Stores the assembled rdmaLeaderChainState keyed by src_mig_id.
 *
 * Returns C_OK on full success; on any failure, writes a description into
 * errbuf and returns C_ERR. Partial state may be left allocated — Phase F
 * cleanup is out of scope for the skeleton.
 *
 * This function runs on the CALLER's thread. For now used from the debug
 * command (main thread) and from a worker thread (registerWorkerThread) in
 * the eventual integration. Avoids calling RedisModule_* / event-loop APIs
 * so it's safe from either context. */
/* Liveness of all followers at once, before a chain is set up.
 *
 * Followers used to be contacted one after the other, so a dead one at the head
 * of the list delayed the live one behind it by the whole dead-peer check (two
 * PINGs of rdma-peer-probe-ms: 0.65 s in S1, 2026-10-05, with the round's
 * replication -- and so its merge and the clients -- waiting). Now every follower
 * is pinged in parallel and the chain is formed from those that answered within
 * rdma-peer-probe-grace-ms of the first answer. A follower left out although it
 * is alive (it stalled for longer than the grace) misses this round's chain and
 * gets the round later through the catch-up path, like any follower that was
 * down. alive_out[i] = 1 for the followers to use. */
typedef struct {
    _Atomic int refs;
    int n;
    _Atomic int state[CLUSTER_NAMELEN];      /* 0 pending, 1 answered, 2 silent / refused */
    char host[CLUSTER_NAMELEN][256];
    int port[CLUSTER_NAMELEN];
} chainProbeSet;
typedef struct { chainProbeSet *s; int i; } chainProbeArg;

static void chainProbeSetUnref(chainProbeSet *s) {
    if (atomic_fetch_sub(&s->refs, 1) == 1) zfree(s);
}

static void *chainProbeThread(void *arg) {
    chainProbeArg *a = arg;
    chainProbeSet *s = a->s;
    int i = a->i, res = 2;
    zfree(a);
    struct timeval ctv = { CHAIN_CONNECT_TIMEOUT_MS / 1000, (CHAIN_CONNECT_TIMEOUT_MS % 1000) * 1000 };
    struct timeval rtv = { CHAIN_RPC_TIMEOUT_MS / 1000, 0 };
    for (int probe = 0; probe < 2 && res != 1; probe++) {
        redisContext *pc = redisConnectWithTimeout(s->host[i], s->port[i], ctv);
        if (pc == NULL || pc->err) { if (pc) redisFree(pc); break; }
        if (rdmaPeerAnswers(pc, rtv)) res = 1;
        redisFree(pc);
    }
    atomic_store(&s->state[i], res);
    chainProbeSetUnref(s);
    return NULL;
}

static void chainProbeFollowers(long long sess, int n, const char **hosts, int *ports,
                                unsigned char *alive_out) {
    for (int i = 0; i < n; i++) alive_out[i] = 1;
    if (n < 2 || server.rdma_peer_probe_ms <= 0 || server.rdma_peer_probe_grace_ms < 0) return;
    chainProbeSet *s = zcalloc(sizeof(*s));
    s->n = n;
    atomic_store(&s->refs, 1);
    for (int i = 0; i < n; i++) {
        snprintf(s->host[i], sizeof(s->host[i]), "%s", hosts[i]);
        s->port[i] = ports[i];
        chainProbeArg *a = zmalloc(sizeof(*a));
        a->s = s; a->i = i;
        atomic_fetch_add(&s->refs, 1);
        pthread_t tid;
        if (pthread_create(&tid, NULL, chainProbeThread, a) != 0) {
            atomic_store(&s->state[i], 1);         /* cannot probe: let the old path decide */
            zfree(a);
            chainProbeSetUnref(s);
        } else pthread_detach(tid);
    }
    long long t0 = mstime(), t_first = 0;
    long long limit = 2LL * (server.rdma_peer_probe_ms + CHAIN_CONNECT_TIMEOUT_MS) + 500;
    for (;;) {
        int pending = 0, alive = 0;
        for (int i = 0; i < n; i++) {
            int st = atomic_load(&s->state[i]);
            if (st == 0) pending++; else if (st == 1) alive++;
        }
        if (pending == 0) break;
        if (alive > 0) {
            if (t_first == 0) t_first = mstime();
            if (mstime() - t_first >= server.rdma_peer_probe_grace_ms) break;
        }
        if (mstime() - t0 > limit) break;
        usleep(500);
    }
    for (int i = 0; i < n; i++) {
        alive_out[i] = (atomic_load(&s->state[i]) == 1);
        if (!alive_out[i])
            serverLog(LL_WARNING, "CHAIN: sess=%lld follower %s:%d did not answer within %d ms of "
                      "the first one (%lld ms after the check began) -- left out of this chain",
                      sess, hosts[i], ports[i], server.rdma_peer_probe_grace_ms, mstime() - t0);
    }
    chainProbeSetUnref(s);
}

static int rdmaLeaderChainEstablishLocked(long long src_mig_id, long long pool_bytes,
                             int n_followers,
                             const char **hosts, int *ports,
                             char *errbuf, size_t errbuf_len);

/* One chain is set up at a time: a follower accepts a single RDMA connection on
 * its chain listener, and the warm-up a newly elected leader starts (see
 * rdmaRecipientRecover) may still be running when the first re-sent round asks
 * for its chain. The second caller then finds the first one's connections and
 * reuses them. */
static pthread_mutex_t g_chain_establish_mu = PTHREAD_MUTEX_INITIALIZER;
int rdmaLeaderChainEstablish(long long src_mig_id, long long pool_bytes,
                             int n_followers,
                             const char **hosts, int *ports,
                             char *errbuf, size_t errbuf_len) {
    pthread_mutex_lock(&g_chain_establish_mu);
    int rc = rdmaLeaderChainEstablishLocked(src_mig_id, pool_bytes, n_followers, hosts, ports,
                                            errbuf, errbuf_len);
    pthread_mutex_unlock(&g_chain_establish_mu);
    return rc;
}

static int rdmaLeaderChainEstablishLocked(long long src_mig_id, long long pool_bytes,
                             int n_followers,
                             const char **hosts, int *ports,
                             char *errbuf, size_t errbuf_len) {
    if (n_followers < 0 || n_followers > CLUSTER_NAMELEN) {
        snprintf(errbuf, errbuf_len, "n_followers out of range");
        return C_ERR;
    }

    pthread_mutex_lock(&g_chain_state_mu);
    if (findLeaderState(src_mig_id) != NULL) {
        pthread_mutex_unlock(&g_chain_state_mu);
        snprintf(errbuf, errbuf_len, "session %lld already established",
                 src_mig_id);
        return C_ERR;
    }
    rdmaLeaderChainState *st = zcalloc(sizeof(*st));
    st->src_mig_id = src_mig_id;
    st->n_peers = n_followers;
    st->n_wired = n_followers;
    st->peers = (n_followers > 0)
        ? zcalloc((size_t) n_followers * sizeof(rdmaChainPeer))
        : NULL;
    pthread_mutex_init(&st->mu, NULL);
    if (insertLeaderState(st) != C_OK) {
        pthread_mutex_unlock(&g_chain_state_mu);
        if (st->peers) zfree(st->peers);
        zfree(st);
        snprintf(errbuf, errbuf_len, "too many concurrent leader chains");
        return C_ERR;
    }
    pthread_mutex_unlock(&g_chain_state_mu);

    /* Pass 1: for each follower, INIT-QP → leader RDMA-connect → PREP.
     * Strict order: INIT-QP starts the follower's rdmamig_server, leader's
     * RDMA-connect populates server_cm_id, then PREP can rdmamig_buffer_create
     * against that cm_id and return a real rkey. */
    int peer_rdma_ports[CLUSTER_NAMELEN] = {0};
    int n_live = 0;
    unsigned char probe_alive[CLUSTER_NAMELEN];
    chainProbeFollowers(src_mig_id, n_followers, hosts, ports, probe_alive);
    for (int i = 0; i < n_followers; i++) {
        st->peers[i].host = sdsnew(hosts[i]);
        st->peers[i].port = ports[i];
        st->peers[i].chain_position = i + 1;
        st->peers[i].wire_position = 0;        /* set when the follower is wired */
        st->peers[i].established = 0;
        if (!probe_alive[i]) continue;         /* logged by chainProbeFollowers */

        /* AqRaft Stage 1: reuse a prior round's live QP to this follower if
         * one exists (the follower's singleton rdmamig_server can't accept a
         * 2nd connect — a fresh leaderConnectToFollower would hang). Only the
         * INIT-QP + RDMA-connect are skipped; we still send a FRESH CHAIN-PREP
         * so this session gets its own landing pool (pool reuse is a later
         * stage that needs copy-out + a drain barrier first). */
        pthread_mutex_lock(&g_chain_state_mu);
        void *reused = findLivePeerClient(hosts[i], ports[i], src_mig_id);
        pthread_mutex_unlock(&g_chain_state_mu);
        if (reused != NULL) {
            st->peers[i].client = reused;
            serverLog(LL_NOTICE,
                "CHAIN: sess=%lld peer%d %s:%d reusing existing chain QP "
                "(skip INIT-QP/connect)", src_mig_id, i, hosts[i], ports[i]);
        } else {
            if (sendChainInitQp(hosts[i], ports[i], src_mig_id,
                                &peer_rdma_ports[i],
                                errbuf, errbuf_len) != C_OK) {
                /* AqRaft #4b: follower dead at establish-time. DON'T abort the
                 * whole chain (that left later peers unpopulated so a re-form had
                 * nothing to promote — S4 run 2310). Exclude + keep going. */
                serverLog(LL_WARNING,
                    "CHAIN: sess=%lld peer%d %s:%d INIT-QP failed (%s) — excluding "
                    "from chain (establish-time death), survivors continue",
                    src_mig_id, i, hosts[i], ports[i], errbuf);
                continue;
            }
            if (leaderConnectToFollower(hosts[i], peer_rdma_ports[i],
                                        &st->peers[i],
                                        errbuf, errbuf_len) != C_OK) {
                /* Without RDMA hardware leaderConnectToFollower returns C_ERR;
                 * we still want the control-plane PREP/WIRE round-trips to work
                 * so pytest verification can run. Log + continue. */
                serverLog(LL_WARNING,
                    "CHAIN: leader RDMA-connect to %s:%d failed (%s) — "
                    "control plane will continue; chain transport will be no-op",
                    hosts[i], peer_rdma_ports[i], errbuf);
            }
        }
        if (sendChainPrep(hosts[i], ports[i], src_mig_id, pool_bytes,
                          &st->peers[i], errbuf, errbuf_len) != C_OK) {
            /* AqRaft #4b: dead at PREP — exclude + continue (see INIT-QP above). */
            serverLog(LL_WARNING,
                "CHAIN: sess=%lld peer%d %s:%d PREP failed (%s) — excluding from "
                "chain (establish-time death), survivors continue",
                src_mig_id, i, hosts[i], ports[i], errbuf);
            continue;
        }
        st->peers[i].established = 1;
        n_live++;
    }
    if (n_live == 0) {
        snprintf(errbuf, errbuf_len,
                 "sess=%lld: no live followers established", src_mig_id);
        pthread_mutex_lock(&g_chain_state_mu);
        st->establish_done = 1;
        pthread_mutex_unlock(&g_chain_state_mu);
        return C_ERR;
    }

    /* Resolve our own (leader's) host:port so followers can CHAIN-ACK back.
     * gethostname is sufficient for the cloudlab/test setup where hostnames
     * are resolvable; production will plumb raft.addr or equivalent. */
    char leader_host[256];
    if (gethostname(leader_host, sizeof(leader_host)) != 0) {
        snprintf(leader_host, sizeof(leader_host), "127.0.0.1");
    }
    leader_host[sizeof(leader_host) - 1] = '\0';
    int leader_port = server.port;

    /* Pass 2: WIRE each follower with its pred + succ + leader (including
     * succ's rdma_port so the follower can rdmamig_client_create to its
     * successor, and leader's host:port so the tail can CHAIN-ACK back). */
    /* Wire only the LIVE followers as a chain (peers excluded in Pass 1 are
     * skipped, and a shorter chain is wired over the survivors). When every
     * follower is healthy nlive==n_followers and live[j]==j, so this is
     * byte-identical to the plain 0..n-1 chain — zero change on the hot path. */
    int live[CLUSTER_NAMELEN]; int nlive = 0;
    for (int i = 0; i < n_followers; i++)
        if (st->peers[i].established) live[nlive++] = i;
    for (int j = 0; j < nlive; j++) {
        int i  = live[j];
        int pj = (j == 0)         ? -1 : live[j - 1];
        int sj = (j == nlive - 1) ? -1 : live[j + 1];
        const char *pred_host = (pj < 0) ? "-" : hosts[pj];
        int pred_port         = (pj < 0) ?  0 : ports[pj];
        const char *succ_host = (sj < 0) ? "-" : hosts[sj];
        int succ_port         = (sj < 0) ?  0 : ports[sj];
        int succ_rdma_port    = (sj < 0) ?  0 : peer_rdma_ports[sj];
        uint64_t succ_addr    = (sj < 0) ?  0 : st->peers[sj].peer_pool_addr;
        uint32_t succ_rkey    = (sj < 0) ?  0 : st->peers[sj].peer_pool_rkey;
        st->peers[i].wire_position = j + 1;   /* what this follower will report as */
        if (sendChainWire(hosts[i], ports[i], src_mig_id,
                          j + 1, nlive,
                          pred_host, pred_port,
                          succ_host, succ_port,
                          succ_rdma_port,
                          succ_addr, succ_rkey,
                          leader_host, leader_port,
                          errbuf, errbuf_len) != C_OK) {
            serverLog(LL_WARNING,
                "CHAIN: sess=%lld WIRE to %s:%d failed (%s) — excluding, survivors continue",
                src_mig_id, hosts[i], ports[i], errbuf);
            st->peers[i].established = 0;
            continue;
        }
    }

    /* AqRaft S1: the forward path and the off-main source-pool pre-registration
     * always use peers[0] as the chain head. A follower that died before or
     * during establish (the old leader, when a promoted leader builds its first
     * chain) was only marked !established and stayed in peers[0], so the
     * pre-registration failed ("chain not established") and fell back to a
     * 2.86 GB ibv_reg_mr on the main thread: the new leader stalled ~1.5 s,
     * lost quorum and stepped down. Compact the live followers to the front,
     * in chain order. No-op when every follower is live or none is. */
    pthread_mutex_lock(&g_chain_state_mu);
    int n_est = 0;
    for (int i = 0; i < st->n_peers; i++) if (st->peers[i].established) n_est++;
    if (n_est > 0 && n_est < st->n_peers) {
        int w = 0, before = st->n_peers;
        for (int i = 0; i < before; i++) {
            if (st->peers[i].established) {
                if (w != i) st->peers[w] = st->peers[i];
                st->peers[w].chain_position = w + 1;
                w++;
            } else if (st->peers[i].host) {
                sdsfree(st->peers[i].host);
                st->peers[i].host = NULL;
            }
        }
        st->n_peers = w;
        serverLog(LL_NOTICE,
            "CHAIN: sess=%lld dropped %d dead follower(s) at establish; head is now %s:%d",
            src_mig_id, before - w, st->peers[0].host ? st->peers[0].host : "?",
            st->peers[0].port);
    }
    st->establish_done = 1;
    pthread_mutex_unlock(&g_chain_state_mu);

    serverLog(LL_NOTICE,
        "RDMA chain established: sess=%lld n_followers=%d pool_bytes=%lld",
        src_mig_id, n_followers, pool_bytes);
    return C_OK;
}

/* AqRaft #4 Part B (chain re-form): a forward to the current head peers[0]
 * (F1) failed because F1 is dead. Drop the dead head and promote the next
 * follower into peers[0] so a re-invoked forward (which always targets
 * peers[0], registering the source against that peer's PD lazily) routes
 * straight to the surviving follower — e.g. leader -> F2 directly. The leader
 * already opened a QP + PREP'd a landing pool to EVERY follower at establish
 * time (see Pass 1 above), so peers[1] already has a live client + pool
 * addr/rkey; no new RDMA handshake is needed. F2 was WIRE'd as the tail
 * (is_tail=1, holds the leader's host:port), so on receiving CHAIN-FORWARDED
 * directly from the leader it applies + CHAIN-ACKs back — giving a REAL
 * majority (leader + F2). We do NOT destroy F1's broken QP here (it may be
 * shared across sessions and is in an error state); we just detach it.
 *
 * Returns C_OK if peers[0] is now a live follower to re-forward to, or C_ERR
 * if no live follower remains (only the dead head was left -> a majority is
 * unreachable -> caller must NOT fake durability). */
/* Snapshot the recipe the leader sends with a forward to peers[0]: the
 * established followers after it, in chain order, and the current attempt.
 * Caller frees with leaderRecipeFree. Returns the number of entries. */
static int leaderRecipeSnapshot(long long src_mig_id, int from, int *attempt_out,
                                sds **hosts_out, int **ports_out) {
    *hosts_out = NULL; *ports_out = NULL; *attempt_out = 0;
    int k = 0;
    pthread_mutex_lock(&g_chain_state_mu);
    rdmaLeaderChainState *st = findLeaderState(src_mig_id);
    if (st != NULL) {
        *attempt_out = st->repair_attempt;
        int cap = (st->n_peers > from) ? st->n_peers - from : 0;
        if (cap > 0) {
            *hosts_out = zmalloc((size_t) cap * sizeof(sds));
            *ports_out = zmalloc((size_t) cap * sizeof(int));
            for (int i = from; i < st->n_peers; i++) {
                if (!st->peers[i].established || st->peers[i].host == NULL) continue;
                (*hosts_out)[k] = sdsdup(st->peers[i].host);
                (*ports_out)[k] = st->peers[i].port;
                k++;
            }
        }
    }
    pthread_mutex_unlock(&g_chain_state_mu);
    return k;
}

static void leaderRecipeFree(sds *hosts, int *ports, int k) {
    if (hosts) { for (int i = 0; i < k; i++) sdsfree(hosts[i]); zfree(hosts); }
    if (ports) zfree(ports);
}

/* CLAIM the chain head's landing pool before the leader writes it. Returns
 * 0 = go ahead, 1 = it already holds the batch (send the token only, never the
 * blocks), C_ERR (-1) with errbuf set = do not write (unreachable, or another
 * attempt's sender is writing it). */
static int leaderClaimHead(long long src_mig_id, const char *host, int port,
                           char *errbuf, size_t errbuf_len) {
    pthread_mutex_lock(&g_chain_state_mu);
    rdmaLeaderChainState *st = findLeaderState(src_mig_id);
    int attempt = st ? st->repair_attempt : 0;
    pthread_mutex_unlock(&g_chain_state_mu);
    redisContext *ctx = chainConnect(host, port);
    if (ctx == NULL) {
        snprintf(errbuf, errbuf_len, "chain head %s:%d unreachable", host, port);
        return C_ERR;
    }
    int claim = chainClaimTarget(ctx, src_mig_id, attempt);
    redisFree(ctx);
    if (claim == 0 || claim == 1) return claim;
    snprintf(errbuf, errbuf_len, claim == 2
             ? "chain head %s:%d is being written by another attempt"
             : "chain head %s:%d gave no answer to the claim", host, port);
    return C_ERR;
}

/* Leader -> peers[0]: CHAIN-FORWARDED carrying the recipe for the rest of the
 * chain. The blocks must already be in that follower's landing pool. */
static int leaderSendForwardedWithRecipe(long long src_mig_id, const char *host, int port,
                                         const int *slots, int n_slots,
                                         char *errbuf, size_t errbuf_len) {
    /* The pipelined forward starts as soon as the chain HEAD is ready, and can
     * finish while the establish thread is still preparing the other followers
     * (it waits on a dying one for seconds). A token sent now would carry an
     * incomplete recipe to a head that does not know its position yet: the
     * chain would end at the head and its report would be unattributable, so
     * the batch would sit until the timed repair. Wait for the establish; give
     * up after 10 s and let the repair handle what is missing. */
    for (int w = 0; w < 5000; w++) {
        pthread_mutex_lock(&g_chain_state_mu);
        rdmaLeaderChainState *est = findLeaderState(src_mig_id);
        int done = (est == NULL) || est->establish_done;
        pthread_mutex_unlock(&g_chain_state_mu);
        if (done) break;
        usleep(2000);
    }
    sds *rh = NULL; int *rp = NULL; int attempt = 0;
    int k = leaderRecipeSnapshot(src_mig_id, 1, &attempt, &rh, &rp);
    redisContext *ctx = chainConnect(host, port);
    int rc;
    if (ctx == NULL) {
        snprintf(errbuf, errbuf_len, "connect(%s:%d) failed", host, port);
        rc = C_ERR;
    } else {
        rc = chainSendForwardedOn(ctx, src_mig_id, slots, n_slots, attempt,
                                  rh, rp, k, errbuf, errbuf_len);
        redisFree(ctx);
    }
    leaderRecipeFree(rh, rp, k);
    return rc;
}

/* ---------------------------------------------------------------------- *
 *  Leader-driven chain repair (see "Chain recipes")                       *
 * ---------------------------------------------------------------------- *
 * Called when the followers holding a batch are still short of a majority
 * and no new one has reported for a while: the token was lost, or it could
 * not get past a failed follower. The leader
 *   1. asks every follower whether it holds the batch (its own word beats the
 *      AppendEntries reports, which lag);
 *   2. reorders the chain: holders first, then reachable followers that lack
 *      the batch, then the ones that do not answer;
 *   3. bumps the attempt number and hands the new recipe to the first holder,
 *      which carries on from there without the leader's bandwidth.
 * If no reachable follower holds the batch the leader must be the source
 * again: it claims the first lacking follower, moves it to the head of the
 * chain and returns *need_data = 1; the caller then re-runs the leader forward
 * (rdmaLeaderChainForwardPerSlot), which sends the blocks and the recipe.
 * Returns C_ERR when nothing can be done now (the caller retries later). */
int rdmaLeaderChainRepair(long long src_mig_id, const int *slots, int n_slots,
                          int *need_data, char *errbuf, size_t errbuf_len) {
    *need_data = 0;
    pthread_mutex_lock(&g_chain_state_mu);
    rdmaLeaderChainState *st = findLeaderState(src_mig_id);
    if (st == NULL || st->n_peers < 1) {
        pthread_mutex_unlock(&g_chain_state_mu);
        snprintf(errbuf, errbuf_len, "no chain state / no followers for sess=%lld", src_mig_id);
        return C_ERR;
    }
    int n = st->n_peers;
    int attempt = ++st->repair_attempt;
    sds *hosts = zmalloc((size_t) n * sizeof(sds));
    int *ports = zmalloc((size_t) n * sizeof(int));
    int *cls   = zmalloc((size_t) n * sizeof(int));   /* 0 holds, 1 lacks, 2 no answer */
    for (int i = 0; i < n; i++) {
        hosts[i] = st->peers[i].host ? sdsdup(st->peers[i].host) : NULL;
        ports[i] = st->peers[i].port;
        cls[i]   = (st->peers[i].established && hosts[i] != NULL) ? -1 : 2;
    }
    pthread_mutex_unlock(&g_chain_state_mu);

    int n_hold = 0, n_lack = 0;
    for (int i = 0; i < n; i++) {
        if (cls[i] == 2) continue;
        redisContext *ctx = chainConnect(hosts[i], ports[i]);
        int holds = (ctx != NULL) ? chainClaimTarget(ctx, src_mig_id, -1) : -1;
        if (ctx) redisFree(ctx);
        if (holds == 1) cls[i] = 0;
        else if (holds == 0 || holds == 2) cls[i] = 1;
        else cls[i] = 2;
        /* Only the follower's own answer counts: a token-only hand-off to a
         * follower that does not hold the batch would make it apply an empty
         * landing pool. */
        if (cls[i] == 0) n_hold++;
        else if (cls[i] == 1) n_lack++;
    }

    /* New order: holders, lacking, silent — each group keeps its chain order. */
    int *order = zmalloc((size_t) n * sizeof(int));
    int m = 0;
    for (int g = 0; g <= 2; g++)
        for (int i = 0; i < n; i++) if (cls[i] == g) order[m++] = i;

    /* No holder: the leader is the source. Claim the first lacking follower
     * that lets us and put it at the head. */
    int rc = C_ERR;
    if (n_hold == 0 && n_lack > 0) {
        int head = -1;
        for (int q = 0; q < n_lack && head < 0; q++) {
            int i = order[q];
            redisContext *ctx = chainConnect(hosts[i], ports[i]);
            int claim = (ctx != NULL) ? chainClaimTarget(ctx, src_mig_id, attempt) : -1;
            if (ctx) redisFree(ctx);
            if (claim == 0) head = q;
        }
        if (head > 0) { int tmp = order[0]; order[0] = order[head]; order[head] = tmp; }
        if (head < 0)
            snprintf(errbuf, errbuf_len, "sess=%lld: every lacking follower is being "
                     "written by an earlier attempt", src_mig_id);
        else { *need_data = 1; rc = C_OK; }
    } else if (n_hold == 0) {
        snprintf(errbuf, errbuf_len, "sess=%lld: no follower reachable", src_mig_id);
    }

    /* Install the new order (the peers are matched by identity, not index, in
     * case the array changed while we were probing). */
    pthread_mutex_lock(&g_chain_state_mu);
    st = findLeaderState(src_mig_id);
    if (st != NULL && st->n_peers == n) {
        rdmaChainPeer *np = zcalloc((size_t) n * sizeof(rdmaChainPeer));
        int ok = 1;
        for (int q = 0; q < n && ok; q++) {
            int i = order[q];
            if (st->peers[i].port != ports[i] ||
                (st->peers[i].host == NULL) != (hosts[i] == NULL) ||
                (hosts[i] != NULL && strcmp(st->peers[i].host, hosts[i]) != 0)) ok = 0;
            np[q] = st->peers[i];
            np[q].chain_position = q + 1;
        }
        if (ok) { zfree(st->peers); st->peers = np; }
        else    { zfree(np); rc = C_ERR;
                  snprintf(errbuf, errbuf_len, "sess=%lld: chain changed during repair", src_mig_id); }
    } else {
        rc = C_ERR;
        snprintf(errbuf, errbuf_len, "sess=%lld: chain changed during repair", src_mig_id);
    }
    int still_ok = (rc == C_OK || n_hold > 0) && st != NULL && st->n_peers == n;
    if (st != NULL && st->n_peers == n && n_hold > 0) {
        /* (rc is still C_ERR here when holders exist; "installed" = order matches) */
        still_ok = 1;
        for (int q = 0; q < n; q++) {
            int i = order[q];
            if (hosts[i] != NULL && (st->peers[q].host == NULL ||
                strcmp(st->peers[q].host, hosts[i]) != 0 || st->peers[q].port != ports[i])) {
                still_ok = 0; break;
            }
        }
    }
    pthread_mutex_unlock(&g_chain_state_mu);

    serverLog(LL_WARNING,
        "CHAIN repair: sess=%lld attempt=%d — %d followers hold the batch, %d lack it, "
        "%d do not answer", src_mig_id, attempt, n_hold, n_lack, n - n_hold - n_lack);

    /* Holders exist: hand the token to the first one that takes it. Its recipe
     * is everything after it in the new order. */
    if (n_hold > 0 && still_ok) {
        for (int q = 0; q < n_hold && rc != C_OK; q++) {
            int i = order[q];
            sds *rh = NULL; int *rp = NULL; int cur = 0;
            int k = leaderRecipeSnapshot(src_mig_id, q + 1, &cur, &rh, &rp);
            redisContext *ctx = chainConnect(hosts[i], ports[i]);
            char err[200] = {0};
            if (ctx != NULL &&
                chainSendForwardedOn(ctx, src_mig_id, slots, n_slots, attempt,
                                     rh, rp, k, err, sizeof(err)) == C_OK) {
                rc = C_OK;
                serverLog(LL_NOTICE, "CHAIN repair: sess=%lld attempt=%d recipe handed to "
                          "holder %s:%d (%d followers after it)", src_mig_id, attempt,
                          hosts[i], ports[i], k);
            } else {
                snprintf(errbuf, errbuf_len, "holder %s:%d did not take the recipe (%s)",
                         hosts[i], ports[i], ctx ? err : "unreachable");
            }
            if (ctx) redisFree(ctx);
            leaderRecipeFree(rh, rp, k);
        }
    }

    for (int i = 0; i < n; i++) if (hosts[i]) sdsfree(hosts[i]);
    zfree(hosts); zfree(ports); zfree(cls); zfree(order);
    return rc;
}

/* After a batch is durable (a majority holds it and MGN_INDX_UPD is committed),
 * bring the LIVE followers that still lack it up to date: a follower that was
 * skipped while the chain routed around a failure would otherwise never get
 * the range. Ask a follower that holds the batch to serve it to the ones that
 * lack it (CHAIN-STATUS ... SEND, the same hand-off as a repair), a few times,
 * a few seconds apart. Best effort and off the critical path; blocking TCP, so
 * never on the main thread. Returns the number of followers still lacking. */
int rdmaLeaderChainCatchUp(long long src_mig_id, int slot_lo, int slot_hi) {
    int lacking = 0;
    /* The commit needs only a majority, so the rest of the chain is usually
     * still receiving the batch at this point: give it a moment first. */
    sleep(3);
    for (int round = 0; round < 12; round++) {
        pthread_mutex_lock(&g_chain_state_mu);
        rdmaLeaderChainState *st = findLeaderState(src_mig_id);
        int n = st ? st->n_peers : 0;
        /* Room for the configured followers too: one that the chain routed around
         * (RE-FORM "dropped dead head") is no longer among the session's peers, so
         * it was never caught up even when it was alive all along (a slow or
         * briefly unreachable follower): it then lacked that round for good. */
        int ncfg = 0;
        sds *cfg = NULL;
        if (st != NULL && server.rdma_chain_followers != NULL && sdslen(server.rdma_chain_followers) > 0)
            cfg = sdssplitlen(server.rdma_chain_followers, (ssize_t) sdslen(server.rdma_chain_followers),
                              " ", 1, &ncfg);
        int cap = n + ncfg;
        sds *hosts = (cap > 0) ? zmalloc((size_t) cap * sizeof(sds)) : NULL;
        int *ports = (cap > 0) ? zmalloc((size_t) cap * sizeof(int)) : NULL;
        int *cls   = (cap > 0) ? zmalloc((size_t) cap * sizeof(int)) : NULL;
        for (int i = 0; i < n; i++) {
            hosts[i] = st->peers[i].host ? sdsdup(st->peers[i].host) : NULL;
            ports[i] = st->peers[i].port;
            cls[i] = (st->peers[i].established && hosts[i] != NULL) ? -1 : 2;
        }
        for (int c = 0; c < ncfg; c++) {
            char *colon = strrchr(cfg[c], ':');
            if (colon == NULL || colon == cfg[c]) continue;
            int cport = atoi(colon + 1);
            size_t hlen = (size_t) (colon - cfg[c]);
            int known = 0;
            for (int i = 0; i < n && !known; i++)
                known = (hosts[i] != NULL && ports[i] == cport &&
                         sdslen(hosts[i]) == hlen && memcmp(hosts[i], cfg[c], hlen) == 0);
            if (known || cport <= 0) continue;
            hosts[n] = sdsnewlen(cfg[c], hlen);
            ports[n] = cport;
            cls[n] = -1;          /* probed below like any other follower */
            n++;
        }
        if (cfg) sdsfreesplitres(cfg, ncfg);
        int attempt = st ? ++st->repair_attempt : 0;
        pthread_mutex_unlock(&g_chain_state_mu);
        if (n == 0) { zfree(hosts); zfree(ports); zfree(cls); return 0; }

        int holder = -1;
        lacking = 0;
        for (int i = 0; i < n; i++) {
            if (cls[i] == 2) continue;
            redisContext *ctx = chainConnect(hosts[i], ports[i]);
            int holds = (ctx != NULL) ? chainClaimTarget(ctx, src_mig_id, -1) : -1;
            if (ctx) redisFree(ctx);
            cls[i] = (holds == 1) ? 0 : (holds == 0 || holds == 2) ? 1 : 2;
            if (cls[i] == 0 && holder < 0) holder = i;
            if (cls[i] == 1) lacking++;
        }
        if (lacking > 0 && holder >= 0) {
            sds cmd = sdscatprintf(sdsempty(), "RDMA CHAIN-STATUS %lld %d %d SEND %d",
                                   src_mig_id, slot_lo, slot_hi, attempt);
            for (int i = 0; i < n; i++)
                if (cls[i] == 1) cmd = sdscatprintf(cmd, " %s %d", hosts[i], ports[i]);
            redisContext *ctx = chainConnect(hosts[holder], ports[holder]);
            redisReply *r = (ctx != NULL) ? redisCommand(ctx, cmd) : NULL;
            serverLog(LL_NOTICE,
                "CHAIN catch-up: sess=%lld slots=%d-%d — %d live followers lack the committed "
                "batch; asked holder %s:%d to serve them (round %d)%s%s", src_mig_id, slot_lo,
                slot_hi, lacking, hosts[holder], ports[holder], round + 1,
                (r && r->type == REDIS_REPLY_ERROR) ? ": " : "",
                (r && r->type == REDIS_REPLY_ERROR) ? r->str : "");
            if (r) freeReplyObject(r);
            if (ctx) redisFree(ctx);
            sdsfree(cmd);
        }
        for (int i = 0; i < n; i++) if (hosts[i]) sdsfree(hosts[i]);
        zfree(hosts); zfree(ports); zfree(cls);
        if (lacking == 0) {
            if (round > 0)
                serverLog(LL_NOTICE, "CHAIN catch-up: sess=%lld slots=%d-%d — every live "
                          "follower holds the batch", src_mig_id, slot_lo, slot_hi);
            return 0;
        }
        if (holder < 0) break;       /* nobody to serve it */
        sleep(5);
    }
    serverLog(LL_WARNING, "CHAIN catch-up: sess=%lld slots=%d-%d — %d live followers still "
              "lack the committed batch (they hold a majority-durable range only through "
              "the others)", src_mig_id, slot_lo, slot_hi, lacking);
    return lacking;
}

int rdmaLeaderChainDropDeadHead(long long src_mig_id,
                                char *errbuf, size_t errbuf_len) {
    pthread_mutex_lock(&g_chain_state_mu);
    rdmaLeaderChainState *st = findLeaderState(src_mig_id);
    if (st == NULL) {
        pthread_mutex_unlock(&g_chain_state_mu);
        snprintf(errbuf, errbuf_len, "no leader chain state for sess=%lld", src_mig_id);
        return C_ERR;
    }
    if (st->n_peers < 1) {
        pthread_mutex_unlock(&g_chain_state_mu);
        snprintf(errbuf, errbuf_len,
                 "no followers left for sess=%lld (majority unreachable)", src_mig_id);
        return C_ERR;
    }
    if (st->n_peers == 1) {
        /* IMPORTANT: multiple forward-batches share one session's chain state.
         * A prior/concurrent batch already dropped the dead head, so peers[0] is
         * the sole SURVIVING follower — do NOT drop it (that would leave zero
         * followers = no majority). Just report OK so the caller re-forwards to
         * it. If that follower is itself dead the re-forward fails and the batch
         * fails loud (one attempt, no infinite retry). */
        void *only_client = st->peers[0].client;
        sds only_host = st->peers[0].host ? sdsdup(st->peers[0].host) : NULL;
        int only_port = st->peers[0].port;
        pthread_mutex_unlock(&g_chain_state_mu);
        serverLog(LL_NOTICE,
            "CHAIN: sess=%lld RE-FORM — dead head already dropped; re-forwarding to "
            "sole surviving follower %s:%d", src_mig_id,
            only_host ? only_host : "?", only_port);
        if (only_host) sdsfree(only_host);
        if (only_client == NULL) {
            snprintf(errbuf, errbuf_len,
                     "sole surviving follower for sess=%lld has no live QP", src_mig_id);
            return C_ERR;
        }
        return C_OK;
    }
    /* n_peers >= 2: drop the dead head, promote the next.
     * Save the dead head's identity for logging + freeing, then shift the
     * remaining peers down so peers[0] becomes the next follower. */
    sds dead_host = st->peers[0].host;   /* now unreferenced after memmove */
    int dead_port = st->peers[0].port;
    memmove(&st->peers[0], &st->peers[1],
            (size_t) (st->n_peers - 1) * sizeof(rdmaChainPeer));
    st->n_peers--;
    st->repair_attempt++;   /* the re-forward carries a new recipe */
    for (int i = 0; i < st->n_peers; i++) st->peers[i].chain_position = i + 1;
    void *new_client   = st->peers[0].client;
    sds   new_host_ref = st->peers[0].host;   /* NULL if this peer was never established */
    int   new_port     = st->peers[0].port;
    /* Dup for logging ONLY when valid: sdsdup(NULL) segfaults, and a chain
     * establish that failed partway (e.g. the dead follower died mid-establish,
     * before the successor's peer entry was filled with sdsnew(host)) leaves the
     * promoted entry all-zero (host=NULL, client=NULL). */
    sds new_host = (new_host_ref != NULL) ? sdsdup(new_host_ref) : NULL;
    pthread_mutex_unlock(&g_chain_state_mu);

    serverLog(LL_WARNING,
        "CHAIN: sess=%lld RE-FORM — dropped dead head %s:%d, promoted %s:%d to "
        "direct successor (leader will re-forward straight to it)",
        src_mig_id, dead_host ? dead_host : "?", dead_port,
        new_host ? new_host : "?", new_port);
    if (dead_host) sdsfree(dead_host);
    if (new_host)  sdsfree(new_host);

    if (new_client == NULL || new_host_ref == NULL) {
        snprintf(errbuf, errbuf_len,
                 "promoted follower for sess=%lld not established "
                 "(client=%p host=%p) — chain establish likely failed; fail loud",
                 src_mig_id, new_client, (void *) new_host_ref);
        return C_ERR;
    }
    return C_OK;
}

/* ====================================================================== *
 *  RDMA DEBUG-CHAIN-ESTABLISH <src_mig_id> <pool_bytes>                 *
 *                             <host1> <port1> [<host2> <port2> ...]     *
 *                                                                       *
 *  Debug-only entry point so the orchestrator can be exercised end-to-  *
 *  end without going through the donor↔leader PREP flow. Removed once   *
 *  registerWorkerThread invokes leaderEstablishChain directly.          *
 * ====================================================================== */
void rdmaDebugChainEstablishCommand(client *c) {
    if (c->argc < 4 || (c->argc % 2) != 0) {
        addReplyError(c,
            "DEBUG-CHAIN-ESTABLISH: usage: <src_mig_id> <pool_bytes> "
            "[<host> <port>]*");
        return;
    }

    long long src_mig_id, pool_bytes;
    if (getLongLongFromObjectOrReply(c, c->argv[2], &src_mig_id, NULL) != C_OK) return;
    if (getLongLongFromObjectOrReply(c, c->argv[3], &pool_bytes, NULL) != C_OK) return;

    int n_followers = (c->argc - 4) / 2;
    const char **hosts = (n_followers > 0) ? zmalloc((size_t) n_followers * sizeof(char *)) : NULL;
    int *ports = (n_followers > 0) ? zmalloc((size_t) n_followers * sizeof(int)) : NULL;
    for (int i = 0; i < n_followers; i++) {
        hosts[i] = (const char *) c->argv[4 + i * 2]->ptr;
        long long p;
        if (getLongLongFromObjectOrReply(c, c->argv[5 + i * 2], &p, NULL) != C_OK) {
            if (hosts) zfree(hosts);
            if (ports) zfree(ports);
            return;
        }
        ports[i] = (int) p;
    }

    char errbuf[256] = {0};
    int rc = rdmaLeaderChainEstablish(src_mig_id, pool_bytes, n_followers,
                                      hosts, ports, errbuf, sizeof(errbuf));
    if (hosts) zfree(hosts);
    if (ports) zfree(ports);
    if (rc == C_OK) {
        addReply(c, shared.ok);
    } else {
        addReplyErrorFormat(c, "DEBUG-CHAIN-ESTABLISH: %s", errbuf);
    }
}

/* ====================================================================== *
 *  rdmaLeaderChainForward (library entry)                                *
 *                                                                        *
 *  On the leader: ensure the per-session src_pool is registered against  *
 *  peers[0].client's PD (lazy on first call), optionally fill the test   *
 *  pattern, RDMA-WRITE the first <length> bytes to peers[0]'s landing    *
 *  pool, then send CHAIN-FORWARDED to peers[0] over TCP. F1's chain      *
 *  worker cascades down the chain; tail will CHAIN-ACK back to leader.  *
 *                                                                        *
 *  Caller's thread blocks for the RDMA-WRITE + wait_send + TCP RPC, so   *
 *  callers from the Redis main thread should consider pushing this onto  *
 *  a worker (cluster_rdma.c already has migration worker threads it can  *
 *  reuse). For Phase B this is fine inline since the debug command is    *
 *  driven from a test and the leader has no Raft peers blocking on it.  *
 * ====================================================================== */
/* Pass-through chain forward — replaces the legacy rdmaChainEncodeBatch +
 * rdmaLeaderChainForward dense path. For each slot in `slots`:
 *   1. Lookup the r_allocator's first block buffer for that slot (the same
 *      memory the donor RDMA-WROTE into; encoded by rdmaEncodeSlotEntries).
 *   2. memcpy 2 MiB → leader's chain src_pool at offset i * 2 MiB.
 *
 * Then ONE RDMA-WRITE of n_slots * 2 MiB to F1's landing pool, followed by
 * a CHAIN-FORWARDED RPC carrying the slot list so F1 can cascade.
 *
 * The src_pool is sized to fit at chain establish time (PREP carries the
 * same pool size to each follower) — caller is responsible for ensuring
 * the chain was established with pool_bytes >= n_slots * 2 MiB.
 */
/* AqRaft Patch 15: pre-register the leader's chain source pool off-main-thread.
 *
 * Without this, the first call to rdmaLeaderChainForwardPerSlot triggers a
 * lazy ibv_reg_mr(2.86 GB) inside mergeBackpatchTick — which runs on the
 * main thread and blocks the event loop for ~3 seconds while the kernel pins
 * the pages. During that window raft can't send / process AppendEntries acks,
 * so the leader's check-quorum (election_timeout * 2 = 2000 ms by default)
 * trips and the leader steps down to follower in the same term. Clients
 * pinned to the old leader then see "TIMEOUT no reply from leader" until a
 * new election settles. By pre-registering inside chainEstablishThread (which
 * is already a detached pthread), the heavy work stays off the event loop.
 *
 * Idempotent: returns C_OK immediately if the src_pool is already registered. */
int rdmaLeaderChainEnsureSrcPool(long long src_mig_id,
                                 char *errbuf, size_t errbuf_len) {
    pthread_mutex_lock(&g_chain_state_mu);
    rdmaLeaderChainState *st = findLeaderState(src_mig_id);
    if (st == NULL || st->n_peers < 1 || st->peers[0].client == NULL) {
        pthread_mutex_unlock(&g_chain_state_mu);
        snprintf(errbuf, errbuf_len,
                 "chain not established for sess=%lld", src_mig_id);
        return C_ERR;
    }
    /* Capture cm_id under the lock; the ibv_reg_mr inside rdmaEnsureLandingFwdReg
     * may take a while — that's exactly what we keep off the main thread. */
    struct rdma_cm_id *cm = rdmamig_client_cm_id(st->peers[0].client);
    pthread_mutex_unlock(&g_chain_state_mu);

    /* AqRaft zero-copy chain forward: there is no separate src_pool anymore — the
     * forwarder RDMA-reads the donor's landing ring buffer directly. Pre-register
     * the F1-PD twins of all landing ring buffers here (off-main), so the
     * in-window forward doesn't pay ibv_reg_mr. Reuses the CHAIN-WARM QP's cm_id;
     * idempotent across rounds (re-registers only on a fresh F1 cm_id). */
    rdmaEnsureLandingFwdReg(cm);
    serverLog(LL_NOTICE,
        "CHAIN: sess=%lld leader landing F1-PD twins ensured [off-main, zero-copy]",
        src_mig_id);
    return C_OK;
}

/* If the RDMA client to this session's chain head has failed (see
 * chainMarkClientBroken), connect a new one before forwarding. The re-forward
 * after a RE-FORM, and every later repair, used to go out on the same failed QP
 * ("re-forward after RE-FORM also failed ... 0/455 reaped"): with one follower
 * left the batch could never be replicated and the round stayed pending. The
 * follower's pool keeps its address and rkey (shared PD), so only the QP is
 * replaced. */
static void leaderEnsureHeadClient(long long src_mig_id) {
    char host[256]; int port = 0;
    pthread_mutex_lock(&g_chain_state_mu);
    rdmaLeaderChainState *st = findLeaderState(src_mig_id);
    int broken = (st != NULL && st->n_peers >= 1 && st->peers[0].client != NULL &&
                  st->peers[0].host != NULL && chainClientBroken(st->peers[0].client));
    if (broken) {
        snprintf(host, sizeof(host), "%s", st->peers[0].host);
        port = st->peers[0].port;
    }
    pthread_mutex_unlock(&g_chain_state_mu);
    if (!broken) return;

    char err[200] = {0};
    int rdma_port = 0;
    rdmaChainPeer tmp;
    memset(&tmp, 0, sizeof(tmp));
    if (sendChainInitQp(host, port, src_mig_id, &rdma_port, err, sizeof(err)) != C_OK ||
        leaderConnectToFollower(host, rdma_port, &tmp, err, sizeof(err)) != C_OK ||
        tmp.client == NULL) {
        serverLog(LL_WARNING, "CHAIN: sess=%lld could not reconnect the failed RDMA client to "
                  "%s:%d (%s)", src_mig_id, host, port, err);
        return;
    }
    pthread_mutex_lock(&g_chain_state_mu);
    st = findLeaderState(src_mig_id);
    if (st != NULL && st->n_peers >= 1 && st->peers[0].host != NULL &&
        st->peers[0].port == port && strcmp(st->peers[0].host, host) == 0)
        st->peers[0].client = tmp.client;
    pthread_mutex_unlock(&g_chain_state_mu);
    serverLog(LL_NOTICE, "CHAIN: sess=%lld new RDMA client to chain head %s:%d (the previous "
              "one had failed)", src_mig_id, host, port);
}

int rdmaLeaderChainForwardPerSlot(long long src_mig_id,
                                  const int *slots, int n_slots,
                                  void *const *landing_va,
                                  void *landing_buf,
                                  char *errbuf, size_t errbuf_len) {
    if (n_slots <= 0) {
        snprintf(errbuf, errbuf_len, "n_slots must be positive");
        return C_ERR;
    }
    size_t length = (size_t) n_slots * (size_t) RDMAMIG_BLOCK_SIZE_BYTES;
    if (landing_va == NULL || landing_buf == NULL) {
        snprintf(errbuf, errbuf_len, "bad landing args (perslot)");
        return C_ERR;
    }
    leaderEnsureHeadClient(src_mig_id);

    pthread_mutex_lock(&g_chain_state_mu);
    rdmaLeaderChainState *st = findLeaderState(src_mig_id);
    if (st == NULL || st->n_peers < 1 || st->peers[0].client == NULL ||
        st->peers[0].peer_pool_addr == 0 || st->peers[0].peer_pool_rkey == 0) {
        pthread_mutex_unlock(&g_chain_state_mu);
        snprintf(errbuf, errbuf_len,
                 "chain not established for sess=%lld", src_mig_id);
        return C_ERR;
    }
    if (length > st->peers[0].peer_pool_bytes) {
        size_t pool_bytes = st->peers[0].peer_pool_bytes;
        pthread_mutex_unlock(&g_chain_state_mu);
        snprintf(errbuf, errbuf_len,
                 "n_slots * 2 MiB = %zu > F1 pool %zu", length, pool_bytes);
        return C_ERR;
    }

    /* Snapshot what we need to do the WRITE outside the lock. */
    void *cli = st->peers[0].client;
    uint64_t remote_addr = st->peers[0].peer_pool_addr;
    uint32_t remote_rkey = st->peers[0].peer_pool_rkey;
    sds f1_host = sdsdup(st->peers[0].host);
    int f1_port = st->peers[0].port;
    pthread_mutex_unlock(&g_chain_state_mu);

    /* AqRaft zero-copy chain forward: RDMA-WRITE straight from the donor's
     * landing blocks (landing_va[i]) — no snapshot, no src_pool. Resolve the
     * F1-PD twin MR of the landing buffer (lazily registers it if needed). */
    (void) slots;  /* slot list only used for the CHAIN-FORWARDED RPC below */
    void *fwd_buf = rdmaLandingFwdBufFor(landing_buf, rdmamig_client_cm_id(cli));
    if (fwd_buf == NULL) {
        snprintf(errbuf, errbuf_len,
                 "no F1-PD twin MR for landing buf (perslot) sess=%lld", src_mig_id);
        sdsfree(f1_host);
        return C_ERR;
    }
    int skip_data = leaderClaimHead(src_mig_id, f1_host, f1_port, errbuf, errbuf_len);
    if (skip_data == C_ERR) { sdsfree(f1_host); return C_ERR; }

    /* RDMA-WRITE per-slot: each 2 MiB chunk is its own WR (a single ~1.43 GiB
     * write would exceed IB HCA max_msg_sz, typically 2 GiB).
     *
     * AqRaft Stage 4 (line-rate): keep up to RDMA_FWD_INFLIGHT WRs in flight
     * and reap completions in bulk, instead of the old post-one / wait-one loop
     * which left only ONE WR on the wire at a time (round-trip-bound → measured
     * ~15 Gbit/s). All n_slots WRs (<= 682) fit comfortably under MAX_SEND_WR
     * (4096) / CQ_CAPACITY (4096); the window cap is just a safety bound so this
     * stays correct if a future batch ever exceeds the queue depth. post_write
     * signals every WR, so #completions == #posts. */
    /* Serialize against any other session's forward (shared QP+CQ) — see mutex.
     * skip_data: the head already holds the batch (repair) — token only. */
    if (!skip_data) {
        pthread_mutex_lock(&g_chain_forward_mu);
        const int INFLIGHT = RDMA_FWD_INFLIGHT;
        struct ibv_wc wc[64];
        int posted = 0, reaped = 0;
        while (reaped < n_slots) {
            /* Post until the in-flight window is full (or all WRs are out). */
            while (posted < n_slots && (posted - reaped) < INFLIGHT) {
                char *local = (char *) landing_va[posted];   /* zero-copy source */
                if (local == NULL) {
                    pthread_mutex_unlock(&g_chain_forward_mu);
                    sdsfree(f1_host);
                    snprintf(errbuf, errbuf_len,
                             "landing_va[%d] NULL (perslot)", posted);
                    return C_ERR;
                }
                uint64_t remote = remote_addr + (uint64_t) posted * RDMAMIG_BLOCK_SIZE_BYTES;
                if (rdmamig_client_post_write(fwd_buf, local, remote, remote_rkey,
                                              RDMAMIG_BLOCK_SIZE_BYTES) != 0) {
                    chainMarkClientBroken(cli);
                    pthread_mutex_unlock(&g_chain_forward_mu);
                    sdsfree(f1_host);
                    snprintf(errbuf, errbuf_len,
                             "post_write to F1 failed at slot_idx=%d", posted);
                    return C_ERR;
                }
                posted++;
            }
            /* Reap whatever completions are ready (non-blocking); spin if none
             * yet but WRs are outstanding. */
            int n = rdmamig_client_poll_send(cli, wc,
                        (int) (sizeof(wc) / sizeof(wc[0])));
            if (n < 0) {
                chainMarkClientBroken(cli);
                pthread_mutex_unlock(&g_chain_forward_mu);
                sdsfree(f1_host);
                snprintf(errbuf, errbuf_len,
                         "poll_send for F1 failed after %d/%d reaped", reaped, n_slots);
                return C_ERR;
            }
            if (n > posted - reaped) n = posted - reaped;   /* leftovers of an earlier session */
            reaped += n;
        }
        pthread_mutex_unlock(&g_chain_forward_mu);
    }
    serverLog(LL_NOTICE,
        "CHAIN: sess=%lld %s %zu bytes (n_slots=%d, %d × 2 MiB WRs) leader → F1 (%s)",
        src_mig_id, skip_data ? "head already holds" : "wrote", length, n_slots, n_slots, f1_host);

    chainMarkForwarded(src_mig_id);

    /* Send CHAIN-FORWARDED to F1 with the per-slot list. F1 will cascade
     * via its chain worker carrying the same slot list. */
    int rc = leaderSendForwardedWithRecipe(src_mig_id, f1_host, f1_port,
                                           slots, n_slots, errbuf, errbuf_len);
    sdsfree(f1_host);
    return rc;
}

/* AqRaft chain-pipeline: forward the snapshot to F1 incrementally, one 2 MiB
 * block per slot, RDMA-WRITing each block AS SOON AS the backpatch pool worker
 * marks it captured in snapshot_ready[]. This overlaps the recipient->F1 write
 * with the still-ongoing donor->recipient transfer + merge (the recipient NIC
 * is full-duplex), instead of one bulk forward after the whole merge finishes.
 * Single-threaded (this is the only forwarder for the session), so it owns the
 * chain QP with no locking. Sends one CHAIN-FORWARDED at the end (the single
 * "DONE"). Falls back to spin-waiting for the per-session chain to come up
 * (the establish thread runs concurrently; with CHAIN-WARM it is near-instant). */
/* AqRaft: per-slot registration generation on the recipient leader. Bumped for a
 * session's slots each time a donor registers them (REGISTER-BLOCK-SLOTS). A
 * forwarder whose first slot's generation changes while it is stalled has been
 * superseded: a newer session (a resumed / re-homed / re-driven donor) now owns
 * those slots, and the old session's donor is gone. */
static _Atomic unsigned int g_slot_reg_gen[CLUSTER_SLOTS];

unsigned int rdmaSlotRegGen(int slot) {
    if (slot < 0 || slot >= CLUSTER_SLOTS) return 0;
    return atomic_load_explicit(&g_slot_reg_gen[slot], memory_order_acquire);
}

void rdmaSlotRegGenBump(const int *slots, int n) {
    if (slots == NULL) return;
    for (int i = 0; i < n; i++)
        if (slots[i] >= 0 && slots[i] < CLUSTER_SLOTS)
            atomic_fetch_add_explicit(&g_slot_reg_gen[slots[i]], 1, memory_order_release);
}

int rdmaLeaderChainForwardPipelined(long long src_mig_id,
                                    const int *slots, int n_slots,
                                    void *const *landing_va,
                                    void *landing_buf,
                                    const _Atomic unsigned char *snapshot_ready,
                                    const int *chunk_slots, _Atomic uint64_t *ch_chunk_logged,
                                    void (*blk_done)(void *ctx, int idx), void *blk_ctx,
                                    char *errbuf, size_t errbuf_len) {
    if (n_slots <= 0) {
        snprintf(errbuf, errbuf_len, "n_slots must be positive");
        return C_ERR;
    }
    size_t length = (size_t) n_slots * (size_t) RDMAMIG_BLOCK_SIZE_BYTES;
    if (landing_va == NULL || landing_buf == NULL || snapshot_ready == NULL) {
        snprintf(errbuf, errbuf_len, "bad landing args (pipelined)");
        return C_ERR;
    }
    leaderEnsureHeadClient(src_mig_id);

    /* Wait for the per-session chain to be established (QP up + F1 pool advertised).
     * The establish thread (spawned at DONE-SLOTS-INIT) reuses the CHAIN-WARM QP,
     * so this resolves in a few ms. Bound at ~10 s. No src_pool to wait on — the
     * forward RDMA-reads the landing buffer directly. */
    void *cli = NULL;
    uint64_t remote_addr = 0; uint32_t remote_rkey = 0;
    sds f1_host = NULL; int f1_port = 0;
    for (int tries = 0; tries < 10000; tries++) {
        pthread_mutex_lock(&g_chain_state_mu);
        rdmaLeaderChainState *st = findLeaderState(src_mig_id);
        if (st != NULL && st->n_peers >= 1 && st->peers[0].client != NULL &&
            st->peers[0].peer_pool_addr != 0 && st->peers[0].peer_pool_rkey != 0) {
            if (length > st->peers[0].peer_pool_bytes) {
                size_t pb = st->peers[0].peer_pool_bytes;
                pthread_mutex_unlock(&g_chain_state_mu);
                snprintf(errbuf, errbuf_len,
                         "n_slots * 2 MiB = %zu > F1 pool %zu", length, pb);
                return C_ERR;
            }
            cli = st->peers[0].client;
            remote_addr = st->peers[0].peer_pool_addr;
            remote_rkey = st->peers[0].peer_pool_rkey;
            f1_host = sdsdup(st->peers[0].host); f1_port = st->peers[0].port;
            pthread_mutex_unlock(&g_chain_state_mu);
            break;
        }
        pthread_mutex_unlock(&g_chain_state_mu);
        usleep(1000);
    }
    if (cli == NULL) {
        snprintf(errbuf, errbuf_len, "chain not ready for pipelined forward sess=%lld",
                 src_mig_id);
        return C_ERR;
    }
    /* Resolve the F1-PD twin MR of the landing buffer (lazily registers it if the
     * pre-registration at establish/warm time didn't cover this ring slot). */
    void *fwd_buf = rdmaLandingFwdBufFor(landing_buf, rdmamig_client_cm_id(cli));
    if (fwd_buf == NULL) {
        snprintf(errbuf, errbuf_len,
                 "no F1-PD twin MR for landing buf (pipelined) sess=%lld", src_mig_id);
        sdsfree(f1_host);
        return C_ERR;
    }
    int skip_data = leaderClaimHead(src_mig_id, f1_host, f1_port, errbuf, errbuf_len);
    if (skip_data == C_ERR) { sdsfree(f1_host); return C_ERR; }

    /* Scan-post loop: forward any captured-but-unposted block; reap completions.
     * Out-of-order capture (4 concurrent pool workers) is fine — each WR targets
     * remote_addr + idx*BLOCK independently. */
    /* Serialize this session's forward against any other session's (shared QP+CQ). */
    long long t_first_post = 0;   /* REAL forward start (first WR on the wire) */
    if (!skip_data) {
        pthread_mutex_lock(&g_chain_forward_mu);
        const int INFLIGHT = RDMA_FWD_INFLIGHT;
        struct ibv_wc wc[64];
        unsigned char *posted = zcalloc((size_t) n_slots);
        /* Blocks in the order they were posted. Completions on one QP come back in
         * that order, so order[reaped .. reaped+n) are the blocks the head now
         * holds: blk_done lets the caller start merging exactly those. */
        int *order = zmalloc(sizeof(int) * (size_t) n_slots);
        int n_posted = 0, reaped = 0;
        long long stall = 0;
        /* The send CQ is shared by every session on this QP. A session that was
         * abandoned mid-forward (its donor died) can leave completions behind; the
         * next session used to count them as its own ("posted=0 reaped=269"),
         * reached reaped == n_slots after posting only part of its blocks and told
         * the followers the whole batch was in their pool: they applied empty
         * blocks and lacked those keys (S9). Drain what is there before posting,
         * and below never count more completions than this session has in flight. */
        {
            int stale = 0, dn;
            while ((dn = rdmamig_client_poll_send(cli, wc, (int) (sizeof(wc) / sizeof(wc[0])))) > 0)
                stale += dn;
            if (stale > 0)
                serverLog(LL_WARNING, "CHAIN: sess=%lld drained %d send completions left by an "
                          "earlier session on this QP", src_mig_id, stale);
        }
        int ref_slot = -1; unsigned int ref_gen = 0;   /* superseded check (see g_slot_reg_gen) */
        while (reaped < n_slots) {
            int progressed = 0;
            for (int idx = 0; idx < n_slots && (n_posted - reaped) < INFLIGHT; idx++) {
                if (posted[idx]) continue;
                if (!atomic_load_explicit(&snapshot_ready[idx], memory_order_acquire))
                    continue;
                /* Zero-copy: RDMA-WRITE straight from the donor's landing block.
                 * landing_va[idx] was captured by the pool worker (with the same
                 * release barrier as snapshot_ready[idx]) and the buffer is held
                 * by the forward refcount, so these pages are valid + pristine. */
                char *local = (char *) landing_va[idx];
                if (local == NULL) {
                    pthread_mutex_unlock(&g_chain_forward_mu);
                    zfree(posted); zfree(order); sdsfree(f1_host);
                    snprintf(errbuf, errbuf_len,
                             "landing_va[%d] NULL despite ready (pipelined)", idx);
                    return C_ERR;
                }
                uint64_t remote = remote_addr + (uint64_t) idx * RDMAMIG_BLOCK_SIZE_BYTES;
                if (rdmamig_client_post_write(fwd_buf, local, remote, remote_rkey,
                                              RDMAMIG_BLOCK_SIZE_BYTES) != 0) {
                    chainMarkClientBroken(cli);
                    pthread_mutex_unlock(&g_chain_forward_mu);
                    zfree(posted); zfree(order); sdsfree(f1_host);
                    snprintf(errbuf, errbuf_len,
                             "post_write to F1 failed at idx=%d (pipelined)", idx);
                    return C_ERR;
                }
                posted[idx] = 1; order[n_posted++] = idx; progressed = 1;
                if (ref_slot < 0 && slots[idx] >= 0 && slots[idx] < CLUSTER_SLOTS) {
                    ref_slot = slots[idx];
                    ref_gen = atomic_load_explicit(&g_slot_reg_gen[ref_slot], memory_order_acquire);
                }
                if (n_posted == 1) {
                    t_first_post = mstime();
                    serverLog(LL_NOTICE,
                        "CHAIN: sess=%lld forward FIRST-POST — leader → F1 begins "
                        "(F1 pool ready; this is the real chain-replication start)",
                        src_mig_id);
                }
                /* Per-chunk CHAIN-start timestamp: the first slot of each chunk to
                 * be POSTED logs once (covered position idx → seq = idx/chunk_slots;
                 * slot0 identifies the donor since the round shares src_mig_id). */
                int _cs = (chunk_slots != NULL) ? *chunk_slots : 0;
                if (ch_chunk_logged != NULL && _cs > 0) {
                    int cseq = idx / _cs;
                    if (cseq < 64) {
                        uint64_t cb = 1ULL << (unsigned) cseq;
                        uint64_t cp = atomic_fetch_or_explicit(ch_chunk_logged, cb,
                                                              memory_order_relaxed);
                        if (!(cp & cb))
                            serverLog(LL_NOTICE,
                                "PERCHUNK CHAIN sess=%lld slot0=%d seq=%d start",
                                src_mig_id, slots[0], cseq);
                    }
                }
            }
            int n = rdmamig_client_poll_send(cli, wc,
                        (int) (sizeof(wc) / sizeof(wc[0])));
            if (n < 0) {
                chainMarkClientBroken(cli);
                pthread_mutex_unlock(&g_chain_forward_mu);
                zfree(posted); zfree(order); sdsfree(f1_host);
                snprintf(errbuf, errbuf_len,
                         "poll_send for F1 failed after %d/%d reaped (pipelined)",
                         reaped, n_slots);
                return C_ERR;
            }
            if (n > n_posted - reaped) n = n_posted - reaped;   /* not ours (see above) */
            if (blk_done != NULL)
                for (int k = reaped; k < reaped + n; k++) blk_done(blk_ctx, order[k]);
            reaped += n;
            if (progressed || n > 0) {
                stall = 0;   /* real progress -> reset the stall watchdog */
            } else if (n_posted < n_slots) {
                /* No completion and nothing newly postable -> waiting on the merge
                 * to capture more snapshots. In crash recovery the ORIGINAL
                 * (crashed-donor) session's snapshots stop mid-stream and never
                 * complete, so this forwarder would spin forever holding
                 * g_chain_forward_mu and starve the recovery session's forward.
                 * Abort after ~10s of zero progress and release the mutex; leave
                 * the batch un-finalized (loud fail, never fake durability). The
                 * STALL-ABORT sentinel tells the caller NOT to re-form (the donor
                 * is dead -- a dead F1 is not the cause). */
                /* Superseded: a newer session registered this forwarder's slots, so
                 * its donor is gone and the rest of its data will never arrive.
                 * Leave now through the STALL-ABORT path (release the mutex, batch
                 * left un-finalized) instead of after ~10 s, so the recovery
                 * sessions queued on g_chain_forward_mu can forward and commit. */
                if (ref_slot >= 0 && (stall % 500) == 499 &&
                    atomic_load_explicit(&g_slot_reg_gen[ref_slot], memory_order_acquire) != ref_gen) {
                    stall = 50000;
                    serverLog(LL_WARNING,
                        "CHAIN: sess=%lld forward superseded (slot %d re-registered by a newer "
                        "session) at %d/%d posted — aborting now",
                        src_mig_id, ref_slot, n_posted, n_slots);
                }
                if ((++stall % 5000) == 0) {   /* ~1s of no forward progress */
                    int nready = 0;
                    for (int q = 0; q < n_slots; q++)
                        if (atomic_load_explicit(&snapshot_ready[q],
                                                 memory_order_acquire)) nready++;
                    serverLog(LL_WARNING,
                        "CHAIN: sess=%lld forward STALL — ready=%d/%d posted=%d "
                        "reaped=%d (blocked awaiting snapshot capture)",
                        src_mig_id, nready, n_slots, n_posted, reaped);
                }
                if (stall >= 50000) {   /* ~10s of zero progress -> donor dead */
                    /* Reap our own in-flight writes first (bounded ~2 s), so they
                     * are not left on the shared CQ for the next session. */
                    for (int w = 0; w < 10000 && reaped < n_posted; w++) {
                        int dn = rdmamig_client_poll_send(cli, wc, (int) (sizeof(wc) / sizeof(wc[0])));
                        if (dn < 0) { chainMarkClientBroken(cli); break; }
                        if (dn == 0) usleep(200); else reaped += dn;
                    }
                    pthread_mutex_unlock(&g_chain_forward_mu);
                    serverLog(LL_WARNING,
                        "CHAIN: sess=%lld forward STALL-ABORT at %d/%d posted "
                        "(donor likely dead) — releasing forward mutex so a "
                        "recovery session can proceed; batch left un-finalized",
                        src_mig_id, n_posted, n_slots);
                    zfree(posted); zfree(order); sdsfree(f1_host);
                    snprintf(errbuf, errbuf_len,
                        "STALL-ABORT: forward stalled at %d/%d posted (donor dead)",
                        n_posted, n_slots);
                    return C_ERR;
                }
                usleep(200);
            }
        }
        zfree(posted); zfree(order);
        pthread_mutex_unlock(&g_chain_forward_mu);
    }
    {
        long long elapsed_ms = t_first_post ? (mstime() - t_first_post) : 0;
        double gbps = (elapsed_ms > 0)
            ? ((double) length * 8.0) / ((double) elapsed_ms / 1000.0) / 1e9 : 0.0;
        serverLog(LL_NOTICE,
            "CHAIN: sess=%lld wrote %zu bytes (n_slots=%d, pipelined per-slot) "
            "leader → F1 (%s) — first-post→done %lldms, %.1f Gbps",
            src_mig_id, length, n_slots, f1_host, elapsed_ms, gbps);
    }

    chainMarkForwarded(src_mig_id);

    /* Single CHAIN-FORWARDED to F1 (the "one DONE") — F1 cascades to F2. */
    int rc = leaderSendForwardedWithRecipe(src_mig_id, f1_host, f1_port,
                                           slots, n_slots, errbuf, errbuf_len);
    sdsfree(f1_host);
    return rc;
}

long long rdmaLeaderChainAckCount(long long src_mig_id) {
    pthread_mutex_lock(&g_chain_state_mu);
    rdmaLeaderChainState *ls = findLeaderState(src_mig_id);
    long long count = (ls == NULL) ? -1 : ls->ack_count;
    pthread_mutex_unlock(&g_chain_state_mu);
    return count;
}

/* ====================================================================== *
 *  RDMA DEBUG-CHAIN-FORWARD <src_mig_id> <length>                       *
 *                                                                       *
 *  Phase B test helper — thin wrapper over rdmaLeaderChainForward with  *
 *  fill_pattern=1 so CHAIN-PING can verify byte equality at each hop.   *
 * ====================================================================== */
void rdmaDebugChainForwardCommand(client *c) {
    /* DEBUG-CHAIN-FORWARD was a Phase B test helper for fill_pattern byte
     * verification. The pass-through model no longer maintains a separate
     * fill_pattern path (chain payloads now come from the donor's RDMA-WRITE
     * blocks via r_allocator). The command is preserved for backward
     * compatibility but is now a no-op that returns an error. The end-to-end
     * migration smoke test exercises the same path implicitly. */
    UNUSED(c);
    addReplyError(c, "DEBUG-CHAIN-FORWARD: deprecated — use DEBUG-CHAIN-APPLY-SLOT");
}

/* ====================================================================== *
 *  RDMA DEBUG-CHAIN-STATUS <src_mig_id>                                  *
 *                                                                        *
 *  Phase B.4 verification. On the leader: returns a 3-element array      *
 *  [ack_count, last_acked_length, last_acked_at_ms] so the test can      *
 *  confirm CHAIN-ACK from the tail arrived. ack_count==0 means no tail   *
 *  ack received yet for this session.                                    *
 * ====================================================================== */
/* ====================================================================== *
 *  RDMA DEBUG-CHAIN-APPLY-SLOT <src_mig_id> <slot>                        *
 *                                                                         *
 *  Phase C test helper. On the leader: encode the kvstore contents of    *
 *  <slot> via rdmaChainEncodeBatch into the chain src_pool, RDMA-WRITE    *
 *  it down the chain, send CHAIN-FORWARDED. Each follower (including     *
 *  the tail) decodes via rdmaApplyChainBatch and installs the entries    *
 *  into its own kvstore. Lets us validate Phase C without driving a full *
 *  YCSB migration. After running, GET against any follower should return *
 *  the keys that were in the leader's slot.                              *
 * ====================================================================== */
void rdmaDebugChainApplySlotCommand(client *c) {
    UNUSED(c);
    /* DEBUG-CHAIN-APPLY-SLOT was a Phase C test helper. The pass-through
     * model now requires a pre-captured snapshot pool (filled in the
     * backpatch worker), which this debug entry point does not have
     * access to. The end-to-end migration smoke test exercises the same
     * code path in production. */
    addReplyError(c, "DEBUG-CHAIN-APPLY-SLOT: deprecated — exercise via real migration");
}

void rdmaDebugChainStatusCommand(client *c) {
    long long src_mig_id;
    if (getLongLongFromObjectOrReply(c, c->argv[2], &src_mig_id, NULL) != C_OK) return;

    pthread_mutex_lock(&g_chain_state_mu);
    rdmaLeaderChainState *ls = findLeaderState(src_mig_id);
    if (ls == NULL) {
        pthread_mutex_unlock(&g_chain_state_mu);
        addReplyErrorFormat(c,
            "DEBUG-CHAIN-STATUS: no leader state for sess=%lld", src_mig_id);
        return;
    }
    long long count = ls->ack_count;
    long long len   = (long long) ls->last_acked_length;
    long long ts_ms = ls->last_acked_at_ms;
    pthread_mutex_unlock(&g_chain_state_mu);

    addReplyArrayLen(c, 3);
    addReplyLongLong(c, count);
    addReplyLongLong(c, len);
    addReplyLongLong(c, ts_ms);
}

/* RDMA CHAIN-STATUS <sess> <slot_lo> <slot_hi>
 *
 * AqRaft B#1 recipient-leader recovery: the newly-promoted sg4 leader broadcasts
 * this to every surviving replica to discover WHICH one holds a registered landing
 * block for each slot in [lo,hi] (a committed INDX_UPD guarantees a majority of sg4
 * physically holds the blocks). Replies with the list of held slots in the range,
 * so the new leader can have that holder forward the gap slots inward (reusing the
 * chain-forward push + rdmaApplySlotBlock — no new transfer primitive).
 *
 * "Held" == the r_allocator has >=1 registered foreign landing block for the slot
 * (the chain-forward apply path registers each received block there). */
/* RDMA CHAIN-STATUS <sess> <lo> <hi> [MERGED]
 *
 * Report which slots in [lo,hi] this sg4 replica holds for the given session,
 * answered from the node-LOCAL slot inventory (bookkeeping without Raft: bits
 * are set apply-then-mark, so a claimed slot is genuinely present). Default
 * reports RECEIVED (raw landing block held — what a gap-pull needs); the
 * optional MERGED mode reports slots already applied to the live keyspace
 * (what the S2 donor-status "remaining" computation needs).
 *
 * Compat fallback: if this node has NO inventory entry for the session (e.g.
 * blocks landed before this feature, or a warm sentinel), fall back to the
 * session-blind r_allocator probe the B#1 increment shipped with. */
void rdmaChainStatusCommand(client *c) {
    long long sess, lo, hi;
    if (getLongLongFromObjectOrReply(c, c->argv[2], &sess, NULL) != C_OK) return;
    if (getLongLongFromObjectOrReply(c, c->argv[3], &lo, NULL) != C_OK) return;
    if (getLongLongFromObjectOrReply(c, c->argv[4], &hi, NULL) != C_OK) return;
    /* CHAIN-STATUS <sess> <lo> <hi> SEND <attempt> (<host> <port>)+
     *
     * Asked by a newly elected recipient leader that lacks the batch covering
     * slots [lo,hi] of a replica that holds it: send it down this recipe (the
     * leader first, then the other replicas that lack it). Same hand-off as a
     * chain repair — see "Chain recipes": this node becomes the source, off the
     * main thread, once its own apply is over. The batch is found by slot range
     * (the new leader does not know the old leader's chain session id). */
    if (c->argc >= 9 && strcasecmp(c->argv[5]->ptr, "SEND") == 0) {
        long long attempt;
        int k = (c->argc - 7) / 2;
        if ((c->argc - 7) % 2 != 0 || k < 1 || k > CLUSTER_NAMELEN ||
            getLongLongFromObject(c->argv[6], &attempt) != C_OK || attempt < 0) {
            addReplyError(c, "syntax: RDMA CHAIN-STATUS <sess> <lo> <hi> SEND <attempt> (<host> <port>)+");
            return;
        }
        chainWorkItem *item = NULL;
        pthread_mutex_lock(&g_chain_state_mu);
        for (int i = 0; i < RDMA_CHAIN_MAX_SESSIONS && item == NULL; i++) {
            rdmaFollowerChainState *fs = g_follower_chains[i];
            if (fs == NULL || !fs->applied || fs->slots == NULL || fs->n_slots <= 0) continue;
            int has_lo = 0, has_hi = 0;
            for (int j = 0; j < fs->n_slots; j++) {
                if (fs->slots[j] == (int) lo) has_lo = 1;
                if (fs->slots[j] == (int) hi) has_hi = 1;
            }
            if (!has_lo || !has_hi) continue;
            item = zcalloc(sizeof(*item));
            item->kind = CHAIN_WORK_FORWARD;
            item->src_mig_id = fs->src_mig_id;
            item->n_slots = fs->n_slots;
            item->slots = zmalloc((size_t) fs->n_slots * sizeof(int));
            memcpy(item->slots, fs->slots, (size_t) fs->n_slots * sizeof(int));
            item->length = (size_t) fs->n_slots * (size_t) RDMAMIG_BLOCK_SIZE_BYTES;
        }
        pthread_mutex_unlock(&g_chain_state_mu);
        if (item == NULL) {
            addReplyErrorFormat(c, "CHAIN-STATUS SEND: no held batch covers slots %lld-%lld", lo, hi);
            return;
        }
        item->has_recipe = 1;
        item->attempt = (int) attempt;
        item->n_recipe = k;
        item->recipe_hosts = zmalloc((size_t) k * sizeof(sds));
        item->recipe_ports = zmalloc((size_t) k * sizeof(int));
        for (int i = 0; i < k; i++) {
            long long p = 0;
            item->recipe_hosts[i] = sdsdup(c->argv[7 + 2 * i]->ptr);
            getLongLongFromObject(c->argv[7 + 2 * i + 1], &p);
            item->recipe_ports[i] = (int) p;
        }
        serverLog(LL_NOTICE, "RDMA CHAIN-STATUS SEND: sess=%lld slots=%lld-%lld — serving the "
                  "held batch (%d blocks) to %s:%d (+%d more), attempt=%lld",
                  item->src_mig_id, lo, hi, item->n_slots, item->recipe_hosts[0],
                  item->recipe_ports[0], k - 1, attempt);
        ensureChainWorker();
        chainWorkPush(item);
        addReply(c, shared.ok);
        return;
    }
    int kind = 0;   /* 0 = received (default), 1 = merged */
    if (c->argc >= 6) {
        const char *m = (const char *) c->argv[5]->ptr;
        if (strcasecmp(m, "MERGED") == 0) kind = 1;
        else if (strcasecmp(m, "RECEIVED") != 0) {
            addReplyError(c, "CHAIN-STATUS: expected RECEIVED or MERGED");
            return;
        }
    }
    if (lo < 0) lo = 0;
    if (hi >= CLUSTER_SLOTS) hi = CLUSTER_SLOTS - 1;
    int cap = (hi >= lo) ? (int) (hi - lo + 1) : 0;
    long long *held = zmalloc((size_t) (cap > 0 ? cap : 1) * sizeof(long long));
    int n = rdmaInvSlotsInRange(sess, kind, (int) lo, (int) hi, held, cap);
    const char *src = "inventory";
    if (n < 0) {
        /* No inventory for this session on this node — legacy fallback
         * (session-blind, received-equivalent only). */
        src = "allocator-fallback";
        n = 0;
        if (kind == 0) {
            for (int slot = (int) lo; slot <= (int) hi; slot++)
                if (r_allocator_get_landing_blocks_for_slot(slot, NULL, 0) > 0)
                    held[n++] = slot;
        }
    }
    long long nr = 0, nm = 0; int ex = 0;
    (void) rdmaInvSummary(sess, &nr, &nm, &ex);
    serverLog(LL_NOTICE,
        "RDMA CHAIN-STATUS: sess=%lld range=%lld-%lld %s — holds %d/%d slots [%s; "
        "session totals received=%lld merged=%lld executed=%d]",
        sess, lo, hi, kind ? "MERGED" : "RECEIVED", n, cap, src, nr, nm, ex);
    addReplyArrayLen(c, n);
    for (int i = 0; i < n; i++) addReplyLongLong(c, held[i]);
    zfree(held);
}
