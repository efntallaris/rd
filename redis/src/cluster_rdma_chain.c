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
} rdmaChainPeer;

/* Per-session chain state on the recipient LEADER. Keyed in g_leader_chains
 * by src_mig_id (long long). Lives for the session lifetime. */
typedef struct rdmaLeaderChainState {
    long long src_mig_id;
    int n_peers;
    rdmaChainPeer *peers;       /* heap array, len = n_peers */
    pthread_mutex_t mu;
    /* Phase B: source buffer for chain forward. Lazy-allocated on first
     * RDMA WRITE; registered against peers[0].client's PD so the WRITE
     * to peers[0]'s pool has matching local + remote PDs. */
    void *src_pool;             /* mmap'd test/source bytes */
    size_t src_pool_bytes;
    void *src_buf;              /* struct rdmamig_buffer * */
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

#define RDMA_CHAIN_MAX_SESSIONS 8

static rdmaLeaderChainState   *g_leader_chains[RDMA_CHAIN_MAX_SESSIONS]   = {0};
static rdmaFollowerChainState *g_follower_chains[RDMA_CHAIN_MAX_SESSIONS] = {0};
static pthread_mutex_t g_chain_state_mu = PTHREAD_MUTEX_INITIALIZER;

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

    struct rdmamig_client *cl =
        rdmamig_client_create((const char *) item->host, rdma_port_str);
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

/*
 * RDMA CHAIN-PREP <src_mig_id> <pool_bytes>
 *
 * Issued by the recipient leader to each follower at session start.
 *
 * Phase A skeleton behavior:
 *   - Parses + validates args.
 *   - Creates a follower-side state entry if one does not yet exist for
 *     this src_mig_id.
 *   - Replies with (addr=0, rkey=0, bytes=requested) — placeholder values
 *     until Phase A.full wires real mmap + rdmamig_buffer_create.
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
        /* Allocate the landing pool. Anonymous mmap so it's page-aligned,
         * which ibv_reg_mr requires. Phase A.full: also call
         * rdmamig_buffer_create on this pool to register it with the local
         * PD and obtain a real rkey. For now we leave rkey = 0; the actual
         * RDMA-WRITE will only work once Phase A.full lands. */
        size_t bytes = (size_t) pool_bytes;
        void *pool = mmap(NULL, bytes, PROT_READ | PROT_WRITE,
                          MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
        if (pool == MAP_FAILED) {
            pthread_mutex_unlock(&g_chain_state_mu);
            addReplyErrorFormat(c, "CHAIN-PREP: mmap(%lld) failed: %s",
                                pool_bytes, strerror(errno));
            return;
        }

        st = zcalloc(sizeof(*st));
        st->src_mig_id = src_mig_id;
        st->landing_pool = pool;
        st->landing_pool_bytes = bytes;
        st->landing_pool_addr = (uint64_t) (uintptr_t) pool;
        st->landing_pool_rkey = 0;
        st->landing_pool_buf = NULL;

        /* Phase A.full: register the landing pool against the local
         * rdmamig_server's accepted cm_id. Requires that the upstream peer
         * (leader for F1; F1 for F2; etc.) has already CONNECTed to our
         * rdmamig_server — that's what makes server_cm_id non-NULL. The
         * orchestrator sequences CHAIN-INIT-QP → leader-side connect →
         * CHAIN-PREP so this precondition holds.
         *
         * On hardware-less dev machines server.rdma_server is NULL → we
         * leave landing_pool_buf = NULL and rkey = 0; the chain transport
         * won't work but the control-plane RPCs and pytest verification
         * still pass. */
        if (server.rdma_server != NULL) {
            struct rdma_cm_id *cm = rdmamig_server_cm_id(server.rdma_server);
            if (cm != NULL) {
                struct rdmamig_buffer *buf =
                    rdmamig_buffer_create(cm, (char *) pool, bytes, 0);
                if (buf != NULL) {
                    st->landing_pool_buf = buf;
                    st->landing_pool_rkey = rdmamig_buffer_rkey(buf);
                    serverLog(LL_NOTICE,
                        "CHAIN-PREP: sess=%lld registered pool @ %p (%zu B) rkey=0x%x",
                        src_mig_id, pool, bytes, st->landing_pool_rkey);
                } else {
                    serverLog(LL_WARNING,
                        "CHAIN-PREP: sess=%lld rdmamig_buffer_create failed "
                        "(pool=%p bytes=%zu cm=%p)",
                        src_mig_id, pool, bytes, (void *) cm);
                }
            } else {
                serverLog(LL_VERBOSE,
                    "CHAIN-PREP: sess=%lld no peer connected yet "
                    "(server_cm_id NULL) — leaving rkey=0",
                    src_mig_id);
            }
        }

        if (insertFollowerState(st) != C_OK) {
            if (st->landing_pool_buf == NULL) munmap(pool, bytes);
            pthread_mutex_unlock(&g_chain_state_mu);
            zfree(st);
            addReplyError(c, "CHAIN-PREP: too many concurrent chain sessions");
            return;
        }
    } else if (st->landing_pool_bytes != (size_t) pool_bytes) {
        pthread_mutex_unlock(&g_chain_state_mu);
        addReplyErrorFormat(c,
            "CHAIN-PREP: repeated call for sess=%lld with mismatched pool_bytes "
            "(have=%zu, asked=%lld)",
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
    if (!st->is_tail && succ_rdma_port > 0 && st->successor_client == NULL) {
        ensureChainWorker();
        /* zcalloc so the new ->slots / ->n_slots fields default to NULL/0;
         * the worker frees ->slots only when non-NULL. */
        chainWorkItem *item = zcalloc(sizeof(*item));
        item->kind = CHAIN_WORK_OPEN_SUCC_QP;
        item->src_mig_id = src_mig_id;
        item->host = sdsdup(succ_host);
        item->port = (int) succ_rdma_port;
        item->next = NULL;
        chainWorkPush(item);
        enqueued_qp_work = 1;
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
        for (int i = 0; i < job->n_slots; i++) {
            int slot = job->slots[i];
            void *sub = (char *) job->local_pool
                      + (size_t) i * RDMAMIG_BLOCK_SIZE_BYTES;
            /* AqRaft fix: never register a SECOND landing block for a slot that
             * already received one this migration. In multi-round migration the
             * chain forward can re-deliver an earlier round's slots (observed:
             * round-2 forwarding round-1 slots with empty/garbage data,
             * staged=0). Linking a 2nd block per slot left a stale block whose
             * later walk/sanitize/free corrupted the heap → intermittent
             * recipient-follower SIGSEGV. Skip the duplicate (it carries no
             * valid kvobjs anyway). The flag is set ONLY by landing-block
             * registration, so a round-2 slot that merely picked up a managed
             * block from a raft-applied client write is still applied. */
            if (slot < 0 || slot >= CLUSTER_SLOTS) continue;
            if (g_chain_landing_registered[slot]) {
                serverLog(LL_WARNING,
                    "CHAIN apply: slot=%d landing block already registered — "
                    "skipping duplicate cross-round delivery (sess=%lld)",
                    slot, job->src_mig_id);
                continue;
            }
            if (r_allocator_register_existing_block(slot, sub) == NULL) {
                serverLog(LL_WARNING,
                    "CHAIN apply: r_allocator_register_existing_block failed "
                    "(sess=%lld slot=%d) — skipping slot", job->src_mig_id, slot);
                continue;
            }
            g_chain_landing_registered[slot] = 1;
            total_staged += rdmaFollowerEnqueueSlotMerge(job->db, slot);
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
    sds leader_host_dup = (is_tail && st->leader_host)
        ? sdsdup(st->leader_host) : NULL;
    int leader_port_snap = is_tail ? st->leader_port : 0;
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
    } else if (leader_host_dup != NULL && leader_port_snap > 0) {
        /* Classic chain-replication "tail commit": once the tail has the
         * bytes in its landing pool, ack the leader directly so the leader
         * can finalize the chain step (e.g., fire MGN_INDX_UPD). */
        ensureChainWorker();
        chainWorkItem *item = zcalloc(sizeof(*item));
        item->kind = CHAIN_WORK_ACK_LEADER;
        item->src_mig_id = src_mig_id;
        item->host = leader_host_dup;   /* worker frees */
        item->port = leader_port_snap;
        item->length = (size_t) length;
        chainWorkPush(item);
        leader_host_dup = NULL;
        ack_enqueued = 1;
    } else if (leader_host_dup != NULL) {
        sdsfree(leader_host_dup);
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
    if (r == NULL) {
        snprintf(errbuf, errbuf_len, "CHAIN-PREP %s:%d: %s",
                 host, port, ctx->errstr);
        redisFree(ctx);
        return C_ERR;
    }
    if (r->type == REDIS_REPLY_ERROR) {
        snprintf(errbuf, errbuf_len, "CHAIN-PREP %s:%d: %s",
                 host, port, r->str);
        freeReplyObject(r);
        redisFree(ctx);
        return C_ERR;
    }
    if (r->type != REDIS_REPLY_ARRAY || r->elements != 4 ||
        r->element[0]->type != REDIS_REPLY_STRING ||
        strcmp(r->element[0]->str, "CHAIN-PREP-OK") != 0) {
        snprintf(errbuf, errbuf_len, "CHAIN-PREP %s:%d: bad reply",
                 host, port);
        freeReplyObject(r);
        redisFree(ctx);
        return C_ERR;
    }

    peer->peer_pool_addr  = (uint64_t) r->element[1]->integer;
    peer->peer_pool_rkey  = (uint32_t) r->element[2]->integer;
    peer->peer_pool_bytes = (size_t)   r->element[3]->integer;
    freeReplyObject(r);
    redisFree(ctx);
    return C_OK;
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
        snprintf(errbuf, errbuf_len, "CHAIN-WIRE %s:%d: %s",
                 host, port, ctx->errstr);
        rc = C_ERR;
    } else if (r->type == REDIS_REPLY_ERROR) {
        snprintf(errbuf, errbuf_len, "CHAIN-WIRE %s:%d: %s",
                 host, port, r->str);
        rc = C_ERR;
    } else if (r->type != REDIS_REPLY_STATUS ||
               r->len != 2 || memcmp(r->str, "OK", 2) != 0) {
        snprintf(errbuf, errbuf_len, "CHAIN-WIRE %s:%d: bad reply",
                 host, port);
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
    for (int i = 0; i < n_followers; i++) {
        st->peers[i].host = sdsnew(hosts[i]);
        st->peers[i].port = ports[i];
        st->peers[i].chain_position = i + 1;

        if (sendChainInitQp(hosts[i], ports[i], src_mig_id,
                            &peer_rdma_ports[i],
                            errbuf, errbuf_len) != C_OK) {
            return C_ERR;
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
        if (sendChainPrep(hosts[i], ports[i], src_mig_id, pool_bytes,
                          &st->peers[i], errbuf, errbuf_len) != C_OK) {
            return C_ERR;
        }
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
    for (int i = 0; i < n_followers; i++) {
        const char *pred_host = (i == 0) ? "-" : hosts[i - 1];
        int pred_port = (i == 0) ? 0 : ports[i - 1];
        const char *succ_host = (i == n_followers - 1) ? "-" : hosts[i + 1];
        int succ_port = (i == n_followers - 1) ? 0 : ports[i + 1];
        int succ_rdma_port = (i == n_followers - 1) ? 0 : peer_rdma_ports[i + 1];
        uint64_t succ_addr = (i == n_followers - 1) ? 0 : st->peers[i + 1].peer_pool_addr;
        uint32_t succ_rkey = (i == n_followers - 1) ? 0 : st->peers[i + 1].peer_pool_rkey;
        if (sendChainWire(hosts[i], ports[i], src_mig_id,
                          i + 1, n_followers,
                          pred_host, pred_port,
                          succ_host, succ_port,
                          succ_rdma_port,
                          succ_addr, succ_rkey,
                          leader_host, leader_port,
                          errbuf, errbuf_len) != C_OK) {
            return C_ERR;
        }
    }

    serverLog(LL_NOTICE,
        "RDMA chain established: sess=%lld n_followers=%d pool_bytes=%lld",
        src_mig_id, n_followers, pool_bytes);
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
    if (st->src_pool != NULL) {
        pthread_mutex_unlock(&g_chain_state_mu);
        return C_OK;   /* already registered */
    }

    size_t bytes = st->peers[0].peer_pool_bytes;
    /* Capture cm_id under the lock; rdmamig_buffer_create may take a while
     * (it's the ibv_reg_mr that we're trying to keep off the main thread). */
    struct rdma_cm_id *cm = rdmamig_client_cm_id(st->peers[0].client);
    pthread_mutex_unlock(&g_chain_state_mu);

    void *pool = mmap(NULL, bytes, PROT_READ | PROT_WRITE,
                      MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (pool == MAP_FAILED) {
        snprintf(errbuf, errbuf_len,
                 "mmap(%zu) failed: %s", bytes, strerror(errno));
        return C_ERR;
    }
    struct rdmamig_buffer *buf =
        rdmamig_buffer_create(cm, (char *) pool, bytes, 0);
    if (buf == NULL) {
        munmap(pool, bytes);
        snprintf(errbuf, errbuf_len,
                 "rdmamig_buffer_create on leader src failed");
        return C_ERR;
    }

    pthread_mutex_lock(&g_chain_state_mu);
    /* Re-find state in case it was torn down while we were registering. */
    st = findLeaderState(src_mig_id);
    if (st == NULL) {
        pthread_mutex_unlock(&g_chain_state_mu);
        /* Leak the MR + pool (no rdmamig_buffer_destroy helper exists; the
         * existing recipient pool register has the same leak comment). */
        snprintf(errbuf, errbuf_len, "chain state torn down during register");
        return C_ERR;
    }
    if (st->src_pool != NULL) {
        /* Lost the race with another caller — leak our buffer (same as above). */
        pthread_mutex_unlock(&g_chain_state_mu);
        return C_OK;
    }
    st->src_pool = pool;
    st->src_pool_bytes = bytes;
    st->src_buf = buf;
    pthread_mutex_unlock(&g_chain_state_mu);

    serverLog(LL_NOTICE,
        "CHAIN: sess=%lld leader src pool registered @ %p (%zu B) "
        "[off-main-thread, AqRaft Patch 15]",
        src_mig_id, pool, bytes);
    return C_OK;
}

int rdmaLeaderChainForwardPerSlot(long long src_mig_id,
                                  const int *slots, int n_slots,
                                  const char *snapshot_pool,
                                  size_t snapshot_pool_bytes,
                                  char *errbuf, size_t errbuf_len) {
    if (n_slots <= 0) {
        snprintf(errbuf, errbuf_len, "n_slots must be positive");
        return C_ERR;
    }
    size_t length = (size_t) n_slots * (size_t) RDMAMIG_BLOCK_SIZE_BYTES;
    if (snapshot_pool == NULL || snapshot_pool_bytes < length) {
        snprintf(errbuf, errbuf_len,
                 "snapshot_pool too small (%zu < %zu)",
                 snapshot_pool_bytes, length);
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

    /* AqRaft Patch 15: src pool is now pre-registered by chainEstablishThread
     * (via rdmaLeaderChainEnsureSrcPool below) so the heavy ibv_reg_mr happens
     * off the main thread. If for some reason that didn't run (e.g., the
     * chain establish thread hasn't completed yet), fall back to lazy-init
     * here — but log a warning since this means we'll block the main thread. */
    if (st->src_pool == NULL) {
        pthread_mutex_unlock(&g_chain_state_mu);
        serverLog(LL_WARNING,
            "CHAIN: sess=%lld src pool not pre-registered — falling back to "
            "main-thread ibv_reg_mr (may stall raft heartbeats)",
            src_mig_id);
        char fallback_err[256] = {0};
        if (rdmaLeaderChainEnsureSrcPool(src_mig_id, fallback_err,
                                         sizeof(fallback_err)) != C_OK) {
            snprintf(errbuf, errbuf_len,
                     "src pool fallback init failed: %s", fallback_err);
            return C_ERR;
        }
        pthread_mutex_lock(&g_chain_state_mu);
        st = findLeaderState(src_mig_id);
        if (st == NULL || st->src_pool == NULL) {
            pthread_mutex_unlock(&g_chain_state_mu);
            snprintf(errbuf, errbuf_len,
                     "src pool fallback init: state vanished");
            return C_ERR;
        }
    }

    /* Snapshot what we need to do the WRITE outside the lock. */
    void *src_buf = st->src_buf;
    void *src_pool = st->src_pool;
    void *cli = st->peers[0].client;
    uint64_t remote_addr = st->peers[0].peer_pool_addr;
    uint32_t remote_rkey = st->peers[0].peer_pool_rkey;
    sds f1_host = sdsdup(st->peers[0].host);
    int f1_port = st->peers[0].port;
    pthread_mutex_unlock(&g_chain_state_mu);

    /* Pass-through fill: bulk memcpy the caller's pre-captured snapshot
     * (donor's raw 2 MiB blocks, sequenced by slot index) into the
     * RDMA-registered src_pool. The snapshot was taken in
     * rdmaBackpatchSlotFillShadow BEFORE r_allocator's kvobj insert
     * corrupted the donor's encoded data. */
    (void) slots;  /* slot list only used for the CHAIN-FORWARDED RPC below */
    memcpy(src_pool, snapshot_pool, length);

    /* RDMA-WRITE per-slot: post each 2 MiB chunk individually.
     * A single 2.67 GiB write may exceed IB HCA's max_msg_sz (typically 2 GiB).
     * Pipelined posts back-to-back with a single drain at the end keeps
     * throughput high while staying within per-WR size limits. */
    for (int i = 0; i < n_slots; i++) {
        char *local = (char *) src_pool + (size_t) i * RDMAMIG_BLOCK_SIZE_BYTES;
        uint64_t remote = remote_addr + (uint64_t) i * RDMAMIG_BLOCK_SIZE_BYTES;
        if (rdmamig_client_post_write(src_buf, local,
                                      remote, remote_rkey,
                                      RDMAMIG_BLOCK_SIZE_BYTES) != 0) {
            sdsfree(f1_host);
            snprintf(errbuf, errbuf_len, "post_write to F1 failed at slot_idx=%d", i);
            return C_ERR;
        }
        /* Drain after every WR (simple back-pressure; with a deep send queue
         * we could batch waits, but 1365 posts at ~10 µs each is fine). */
        if (rdmamig_client_wait_send(cli) < 0) {
            sdsfree(f1_host);
            snprintf(errbuf, errbuf_len, "wait_send for F1 failed at slot_idx=%d", i);
            return C_ERR;
        }
    }
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
