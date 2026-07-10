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
static pthread_mutex_t g_rdma_server_bootstrap_mu = PTHREAD_MUTEX_INITIALIZER;

static int ensureLocalRdmamigServer(void) {
    pthread_mutex_lock(&g_rdma_server_bootstrap_mu);
    if (server.rdma_server != NULL) {
        pthread_mutex_unlock(&g_rdma_server_bootstrap_mu);
        return C_OK;
    }
    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", server.rdma_migration_port);
    struct rdmamig_server *s = rdmamig_server_create(port_str);
    if (s == NULL) {
        pthread_mutex_unlock(&g_rdma_server_bootstrap_mu);
        serverLog(LL_WARNING,
            "CHAIN: rdmamig_server_create(port=%s) failed during bootstrap",
            port_str);
        return C_ERR;
    }
    server.rdma_server = s;
    serverLog(LL_NOTICE,
        "CHAIN: rdmamig_server bootstrapped on port %s (lazy)", port_str);
    pthread_mutex_unlock(&g_rdma_server_bootstrap_mu);
    return C_OK;
}

/* ====================================================================== *
 *  Data structures (Phase A.2)                                          *
 * ====================================================================== */

/* One per chain link the recipient LEADER tracks (one entry per follower). */
typedef struct rdmaChainPeer {
    sds  host;                 /* follower's host */
    int  port;                 /* follower's RDMA migration port */
    int  chain_position;       /* 1 = first follower, ... n = chain tail */
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

/* Forward decl — definition is further down in the leader-side section. */
static rdmaLeaderChainState *findLeaderState(long long src_mig_id);

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
    struct chainWorkItem *next;
} chainWorkItem;

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

/* Forward declaration (FOLLOWER → SUCCESSOR forward, runs on chain worker). */
static void chainWorkerHandleForward(chainWorkItem *item);

static void chainWorkerHandleOpenSuccQp(chainWorkItem *item) {
    char rdma_port_str[16];
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
            serverLog(LL_WARNING,
                "CHAIN worker: forward sess=%lld post_write failed slot_idx=%d",
                item->src_mig_id, i);
            sdsfree(succ_host);
            return;
        }
        if (rdmamig_client_wait_send(cli) < 0) {
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
    redisContext *ctx = redisConnect(succ_host, succ_port);
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
                chainWorkerHandleForward(item);
                break;
            case CHAIN_WORK_ACK_LEADER:
                chainWorkerHandleAckLeader(item);
                break;
            default:
                serverLog(LL_WARNING,
                    "CHAIN worker: unknown work kind %d", item->kind);
        }
        if (item->host) sdsfree(item->host);
        if (item->slots) zfree(item->slots);
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
        server.rdma_migration_port);

    addReplyArrayLen(c, 2);
    addReplyBulkCString(c,
        bootstrapped ? "CHAIN-INIT-QP-OK" : "CHAIN-INIT-QP-DEGRADED");
    addReplyLongLong(c, server.rdma_migration_port);
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
static struct rdma_cm_id     *g_flp_cm[N_FOLLOWER_LANDING_POOLS]    = {0};
static int                    g_flp_next      = 0;
static int                    g_flp_prewarmed = 0;
static pthread_mutex_t        g_flp_mu = PTHREAD_MUTEX_INITIALIZER;

/* Ensure ring slot idx is registered against cm with capacity >= bytes.
 * Idempotent (reuses a compatible existing registration). The mmap + ibv_reg_mr
 * (the slow ~340 ms part) runs OUTSIDE g_flp_mu. Returns 0 on success. */
static int followerEnsurePool(int idx, struct rdma_cm_id *cm, size_t bytes) {
    if (cm == NULL) return -1;
    size_t cap = (bytes + FLP_GRAIN - 1) & ~(FLP_GRAIN - 1);   /* round up to grain */
    pthread_mutex_lock(&g_flp_mu);
    int ok = (g_flp_buf[idx] != NULL && g_flp_cm[idx] == cm && g_flp_bytes[idx] >= bytes);
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
    g_flp_bytes[idx] = cap; g_flp_cm[idx] = cm;
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

        if (cm != NULL && followerEnsurePool(idx, cm, bytes) == 0) {
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
        if (reuse != NULL || succ_rdma_port > 0) {
            ensureChainWorker();
            /* zcalloc so the new ->slots / ->n_slots / ->reuse_client fields
             * default to NULL/0; the worker frees ->slots only when non-NULL. */
            chainWorkItem *item = zcalloc(sizeof(*item));
            item->kind = CHAIN_WORK_OPEN_SUCC_QP;
            item->src_mig_id = src_mig_id;
            item->host = sdsdup(succ_host);
            item->port = (int) succ_rdma_port;
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

static void *chainApplyWorker(void *arg) {
    chainApplyJob *job = arg;
    size_t length = (size_t) job->n_slots * (size_t) RDMAMIG_BLOCK_SIZE_BYTES;
    int total_staged = 0;
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
                if (r_allocator_register_existing_block(slot, sub) == NULL) {
                    serverLog(LL_WARNING,
                        "CHAIN apply: r_allocator_register_existing_block failed "
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
                total_staged += rdmaFollowerEnqueueSlotMerge(job->db, slot);
            }
            i += run;
        }
        serverLog(LL_NOTICE,
            "CHAIN apply: sess=%lld n_slots=%d staged=%d "
            "consumed=%zu/%zu bytes [register+main-merge, Patch 29]",
            job->src_mig_id, job->n_slots, total_staged, length, job->pool_bytes);
    }
    zfree(job->slots);
    zfree(job);
    return NULL;
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
    if (c->argc != 4 + n_slots) {
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
    /* AqRaft majority-commit: EVERY follower acks the leader once it has the
     * batch in its landing pool (not just the tail). The leader's chainPendingTick
     * proceeds on the FIRST ack that crosses the batch baseline (count > base),
     * so with leader + F1 acking = majority of a 3-node sg4, the leader unblocks
     * as soon as F1 has the data — without waiting for the F1->tail hop. Non-tail
     * followers ALSO still forward down the chain so the tail eventually gets it.
     * leader_host/leader_port are set for all followers in CHAIN-WIRE. */
    sds leader_host_dup = st->leader_host ? sdsdup(st->leader_host) : NULL;
    int leader_port_snap = st->leader_port;
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
    pthread_mutex_unlock(&g_chain_state_mu);

    /* AqRaft Patch 16(E): per-slot apply (1365 slots × walk_used_segments
     * + kvstoreDictAddRaw) moves OFF the follower main thread into a
     * one-shot detached pthread chainApplyWorker. Main thread proceeds
     * straight to enqueueing the forward to the next chain peer (or the
     * tail ack), then replies OK. The follower's event loop stays free
     * to send AppendEntries acks to the sg4 leader. */
    int apply_spawned = 0;
    if (already_applied) {
        serverLog(LL_NOTICE,
            "CHAIN-FORWARDED: sess=%lld already applied — skipping re-apply "
            "(retry); forwarding/ack only", src_mig_id);
    } else if (local_pool != NULL && length <= pool_bytes && n_slots > 0) {
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
        pthread_t tid;
        if (pthread_create(&tid, NULL, chainApplyWorker, job) != 0) {
            serverLog(LL_WARNING,
                "CHAIN apply: pthread_create(chainApplyWorker) failed for "
                "sess=%lld — running inline on main thread", src_mig_id);
            chainApplyWorker(job);   /* frees job */
        } else {
            pthread_detach(tid);
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
     * so ack the leader NOW — regardless of tail position. The leader counts
     * acks and proceeds at the first one past the baseline (= majority for a
     * 3-node sg4: leader + this follower). */
    if (leader_host_dup != NULL && leader_port_snap > 0) {
        ensureChainWorker();
        chainWorkItem *ack = zcalloc(sizeof(*ack));
        ack->kind = CHAIN_WORK_ACK_LEADER;
        ack->src_mig_id = src_mig_id;
        ack->host = leader_host_dup;   /* worker frees */
        ack->port = leader_port_snap;
        ack->length = (size_t) length;
        chainWorkPush(ack);
        leader_host_dup = NULL;
        ack_enqueued = 1;
    } else if (leader_host_dup != NULL) {
        sdsfree(leader_host_dup);
    }

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
        slots = NULL;
        chainWorkPush(item);
    }
    if (is_tail) {
        serverLog(LL_NOTICE,
            "RDMA CHAIN-FORWARDED: sess=%lld tail ack_enqueued=%d",
            src_mig_id, ack_enqueued);
    }
    /* If `slots` wasn't taken by a forward worker item (tail path), free it. */
    if (slots) zfree(slots);

    addReply(c, shared.ok);
}

/*
 * RDMA CHAIN-ACK <src_mig_id> <length>
 *
 * Sent by the chain TAIL to the leader after its landing pool has the
 * <length> bytes that originated from the leader. Leader records this
 * into its rdmaLeaderChainState for the session — Phase B.5+ will use
 * this to gate firing MGN_INDX_UPD until the chain has committed.
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
    redisContext *ctx = redisConnect(host, port);
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
    redisContext *ctx = redisConnect(host, port);
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
    redisContext *ctx = redisConnect(host, port);
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
int rdmaLeaderChainEstablish(long long src_mig_id, long long pool_bytes,
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
    for (int i = 0; i < n_followers; i++) {
        st->peers[i].host = sdsnew(hosts[i]);
        st->peers[i].port = ports[i];
        st->peers[i].chain_position = i + 1;
        st->peers[i].established = 0;

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
    /* Serialize against any other session's forward (shared QP+CQ) — see mutex. */
    pthread_mutex_lock(&g_chain_forward_mu);
    {
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
                pthread_mutex_unlock(&g_chain_forward_mu);
                sdsfree(f1_host);
                snprintf(errbuf, errbuf_len,
                         "poll_send for F1 failed after %d/%d reaped", reaped, n_slots);
                return C_ERR;
            }
            reaped += n;
        }
    }
    pthread_mutex_unlock(&g_chain_forward_mu);
    serverLog(LL_NOTICE,
        "CHAIN: sess=%lld wrote %zu bytes (n_slots=%d, %d × 2 MiB WRs) leader → F1 (%s)",
        src_mig_id, length, n_slots, n_slots, f1_host);

    /* Send CHAIN-FORWARDED to F1 with the per-slot list. F1 will cascade
     * via its chain worker carrying the same slot list. */
    redisContext *ctx = redisConnect(f1_host, f1_port);
    if (ctx == NULL || ctx->err) {
        snprintf(errbuf, errbuf_len,
                 "connect(%s:%d) failed: %s",
                 f1_host, f1_port, ctx ? ctx->errstr : "(null)");
        if (ctx) redisFree(ctx);
        sdsfree(f1_host);
        return C_ERR;
    }

    int argc = 4 + n_slots;
    const char **argv = zmalloc((size_t) argc * sizeof(*argv));
    size_t *argvlen = zmalloc((size_t) argc * sizeof(*argvlen));
    char sess_arg[32];
    char nslots_arg[16];
    int sess_arg_len = snprintf(sess_arg, sizeof(sess_arg), "%lld", src_mig_id);
    int nslots_arg_len = snprintf(nslots_arg, sizeof(nslots_arg), "%d", n_slots);
    char (*slot_bufs)[16] = zmalloc((size_t) n_slots * sizeof(*slot_bufs));
    argv[0] = "RDMA";              argvlen[0] = 4;
    argv[1] = "CHAIN-FORWARDED";   argvlen[1] = 15;
    argv[2] = sess_arg;            argvlen[2] = (size_t) sess_arg_len;
    argv[3] = nslots_arg;          argvlen[3] = (size_t) nslots_arg_len;
    for (int i = 0; i < n_slots; i++) {
        argvlen[4 + i] = (size_t) snprintf(slot_bufs[i], 16, "%d", slots[i]);
        argv[4 + i]    = slot_bufs[i];
    }
    redisReply *r = redisCommandArgv(ctx, argc, argv, argvlen);
    int rc = C_OK;
    if (r == NULL) {
        snprintf(errbuf, errbuf_len,
                 "CHAIN-FORWARDED to F1 failed: %s", ctx->errstr);
        rc = C_ERR;
    } else if (r->type == REDIS_REPLY_ERROR) {
        snprintf(errbuf, errbuf_len, "F1 errored: %s", r->str);
        rc = C_ERR;
    }
    if (r) freeReplyObject(r);
    zfree(argv);
    zfree(argvlen);
    zfree(slot_bufs);
    redisFree(ctx);
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
int rdmaLeaderChainForwardPipelined(long long src_mig_id,
                                    const int *slots, int n_slots,
                                    void *const *landing_va,
                                    void *landing_buf,
                                    const _Atomic unsigned char *snapshot_ready,
                                    const int *chunk_slots, _Atomic uint64_t *ch_chunk_logged,
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

    /* Scan-post loop: forward any captured-but-unposted block; reap completions.
     * Out-of-order capture (4 concurrent pool workers) is fine — each WR targets
     * remote_addr + idx*BLOCK independently. */
    /* Serialize this session's forward against any other session's (shared QP+CQ). */
    long long t_first_post = 0;   /* REAL forward start (first WR on the wire) */
    pthread_mutex_lock(&g_chain_forward_mu);
    {
        const int INFLIGHT = RDMA_FWD_INFLIGHT;
        struct ibv_wc wc[64];
        unsigned char *posted = zcalloc((size_t) n_slots);
        int n_posted = 0, reaped = 0;
        long long stall = 0;
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
                    zfree(posted); sdsfree(f1_host);
                    snprintf(errbuf, errbuf_len,
                             "landing_va[%d] NULL despite ready (pipelined)", idx);
                    return C_ERR;
                }
                uint64_t remote = remote_addr + (uint64_t) idx * RDMAMIG_BLOCK_SIZE_BYTES;
                if (rdmamig_client_post_write(fwd_buf, local, remote, remote_rkey,
                                              RDMAMIG_BLOCK_SIZE_BYTES) != 0) {
                    pthread_mutex_unlock(&g_chain_forward_mu);
                    zfree(posted); sdsfree(f1_host);
                    snprintf(errbuf, errbuf_len,
                             "post_write to F1 failed at idx=%d (pipelined)", idx);
                    return C_ERR;
                }
                posted[idx] = 1; n_posted++; progressed = 1;
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
                pthread_mutex_unlock(&g_chain_forward_mu);
                zfree(posted); sdsfree(f1_host);
                snprintf(errbuf, errbuf_len,
                         "poll_send for F1 failed after %d/%d reaped (pipelined)",
                         reaped, n_slots);
                return C_ERR;
            }
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
                    pthread_mutex_unlock(&g_chain_forward_mu);
                    serverLog(LL_WARNING,
                        "CHAIN: sess=%lld forward STALL-ABORT at %d/%d posted "
                        "(donor likely dead) — releasing forward mutex so a "
                        "recovery session can proceed; batch left un-finalized",
                        src_mig_id, n_posted, n_slots);
                    zfree(posted); sdsfree(f1_host);
                    snprintf(errbuf, errbuf_len,
                        "STALL-ABORT: forward stalled at %d/%d posted (donor dead)",
                        n_posted, n_slots);
                    return C_ERR;
                }
                usleep(200);
            }
        }
        zfree(posted);
    }
    pthread_mutex_unlock(&g_chain_forward_mu);
    {
        long long elapsed_ms = t_first_post ? (mstime() - t_first_post) : 0;
        double gbps = (elapsed_ms > 0)
            ? ((double) length * 8.0) / ((double) elapsed_ms / 1000.0) / 1e9 : 0.0;
        serverLog(LL_NOTICE,
            "CHAIN: sess=%lld wrote %zu bytes (n_slots=%d, pipelined per-slot) "
            "leader → F1 (%s) — first-post→done %lldms, %.1f Gbps",
            src_mig_id, length, n_slots, f1_host, elapsed_ms, gbps);
    }

    /* Single CHAIN-FORWARDED to F1 (the "one DONE") — F1 cascades to F2. */
    redisContext *ctx = redisConnect(f1_host, f1_port);
    if (ctx == NULL || ctx->err) {
        snprintf(errbuf, errbuf_len, "connect(%s:%d) failed: %s",
                 f1_host, f1_port, ctx ? ctx->errstr : "(null)");
        if (ctx) redisFree(ctx);
        sdsfree(f1_host);
        return C_ERR;
    }
    int argc = 4 + n_slots;
    const char **argv = zmalloc((size_t) argc * sizeof(*argv));
    size_t *argvlen = zmalloc((size_t) argc * sizeof(*argvlen));
    char sess_arg[32], nslots_arg[16];
    int sess_arg_len = snprintf(sess_arg, sizeof(sess_arg), "%lld", src_mig_id);
    int nslots_arg_len = snprintf(nslots_arg, sizeof(nslots_arg), "%d", n_slots);
    char (*slot_bufs)[16] = zmalloc((size_t) n_slots * sizeof(*slot_bufs));
    argv[0] = "RDMA";            argvlen[0] = 4;
    argv[1] = "CHAIN-FORWARDED"; argvlen[1] = 15;
    argv[2] = sess_arg;          argvlen[2] = (size_t) sess_arg_len;
    argv[3] = nslots_arg;        argvlen[3] = (size_t) nslots_arg_len;
    for (int i = 0; i < n_slots; i++) {
        argvlen[4 + i] = (size_t) snprintf(slot_bufs[i], 16, "%d", slots[i]);
        argv[4 + i]    = slot_bufs[i];
    }
    redisReply *r = redisCommandArgv(ctx, argc, argv, argvlen);
    int rc = C_OK;
    if (r == NULL) {
        snprintf(errbuf, errbuf_len, "CHAIN-FORWARDED to F1 failed: %s", ctx->errstr);
        rc = C_ERR;
    } else if (r->type == REDIS_REPLY_ERROR) {
        snprintf(errbuf, errbuf_len, "F1 errored: %s", r->str);
        rc = C_ERR;
    }
    if (r) freeReplyObject(r);
    zfree(argv); zfree(argvlen); zfree(slot_bufs);
    redisFree(ctx); sdsfree(f1_host);
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
