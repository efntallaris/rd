/*
 * cluster_rdma.c
 *
 * Minimal RDMA slot-migration data-transfer path. Five RPCs handled here:
 *
 *   DONOR                                          RECIPIENT
 *     | --- RDMA INIT-SERVER 6479 -------------->  |   (recipient listens)
 *     | --- RDMA INIT-CLIENT <recip-ip> 6479 --->  |   (donor connects, QP up)
 *     |                                            |
 *     | --- RDMA REGISTER-BLOCK-SLOTS              |
 *     |         SLOTS <slot> <nblocks> ... ----->  |   (recipient allocates,
 *     |  <-- bulk reply: array of (VA, rkey) ---   |    registers, returns metadata)
 *     |                                            |
 *     | === RDMA WRITES (no RPC, pure verbs) ===>  |   (donor pushes blocks
 *     |                                            |    using the rkeys above)
 *     |                                            |
 *     | --- RDMA DONE-SLOTS <slot> ... -------->   |   (recipient iterates
 *     | <-- +OK ----------------------------       |    landed segments, dbAdds
 *     |                                            |    into kvstore)
 *
 * Block payload format used in this port (replaces the source's complex
 * segment-iterator format from the 6.2.4 allocator):
 *
 *   [uint32 n_entries]
 *   for each entry:
 *     [uint32 key_len][key bytes]
 *     [uint32 val_len][val bytes]
 *
 * Out of scope for this slice: cluster ownership flip, lock-free Queue + batch
 * processor, shadow-write, experimental toggles. Each TRANSFER-SLOTS targets
 * a single 1 MB block per slot, which limits the per-call payload but keeps
 * the protocol simple. Multi-block / multi-round flows are deferred.
 */

#include "server.h"
#include "cluster.h"
#include "cluster_legacy.h"
#include "cluster_rdma_chain.h"   /* rdmaLeaderChain* — Phase B.5 wiring */
#include "rdma_migration/include/rdma_migration.h"
#include "rdma_migration/allocator.h"   /* r_allocator_log_slot_stats */
#include "kvstore.h"
#include "hiredis.h"
#include "async.h"
#include <arpa/inet.h>
#include <sys/mman.h>   /* mmap, munmap, MAP_ANONYMOUS — Aqueduct big-MR pool */

/* ---- Async hiredis adapter for the main event loop ----------------------- *
 * Mirrors the static adapter sentinel.c carries (sentinel.c:278-366). Used
 * only by rdmaMgnLogAsync below for the main-thread sites that cannot use
 * the sync rdmaMgnLogSync (would deadlock — main thread is what processes
 * RAFT.MGN-LOG). Prefixed mgn_ to avoid colliding with sentinel's symbols. */

typedef struct mgnRedisAeEvents {
    redisAsyncContext *context;
    aeEventLoop *loop;
    int fd;
    int reading, writing;
} mgnRedisAeEvents;

static void mgnRedisAeReadEvent(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el); UNUSED(fd); UNUSED(mask);
    mgnRedisAeEvents *e = (mgnRedisAeEvents*) privdata;
    redisAsyncHandleRead(e->context);
}

static void mgnRedisAeWriteEvent(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el); UNUSED(fd); UNUSED(mask);
    mgnRedisAeEvents *e = (mgnRedisAeEvents*) privdata;
    redisAsyncHandleWrite(e->context);
}

static void mgnRedisAeAddRead(void *privdata) {
    mgnRedisAeEvents *e = (mgnRedisAeEvents*) privdata;
    if (!e->reading) {
        e->reading = 1;
        aeCreateFileEvent(e->loop, e->fd, AE_READABLE, mgnRedisAeReadEvent, e);
    }
}

static void mgnRedisAeDelRead(void *privdata) {
    mgnRedisAeEvents *e = (mgnRedisAeEvents*) privdata;
    if (e->reading) {
        e->reading = 0;
        aeDeleteFileEvent(e->loop, e->fd, AE_READABLE);
    }
}

static void mgnRedisAeAddWrite(void *privdata) {
    mgnRedisAeEvents *e = (mgnRedisAeEvents*) privdata;
    if (!e->writing) {
        e->writing = 1;
        aeCreateFileEvent(e->loop, e->fd, AE_WRITABLE, mgnRedisAeWriteEvent, e);
    }
}

static void mgnRedisAeDelWrite(void *privdata) {
    mgnRedisAeEvents *e = (mgnRedisAeEvents*) privdata;
    if (e->writing) {
        e->writing = 0;
        aeDeleteFileEvent(e->loop, e->fd, AE_WRITABLE);
    }
}

static void mgnRedisAeCleanup(void *privdata) {
    mgnRedisAeEvents *e = (mgnRedisAeEvents*) privdata;
    mgnRedisAeDelRead(privdata);
    mgnRedisAeDelWrite(privdata);
    zfree(e);
}

static int mgnRedisAeAttach(aeEventLoop *loop, redisAsyncContext *ac) {
    if (ac->ev.data != NULL) return C_ERR;
    mgnRedisAeEvents *e = zmalloc(sizeof(*e));
    e->context = ac;
    e->loop = loop;
    e->fd = ac->c.fd;
    e->reading = e->writing = 0;
    ac->ev.addRead   = mgnRedisAeAddRead;
    ac->ev.delRead   = mgnRedisAeDelRead;
    ac->ev.addWrite  = mgnRedisAeAddWrite;
    ac->ev.delWrite  = mgnRedisAeDelWrite;
    ac->ev.cleanup   = mgnRedisAeCleanup;
    ac->ev.data      = e;
    return C_OK;
}

/* ---- Migration protocol log (RAFT.MGN-LOG) ------------------------------- */

/* Async send target — lazy-initialized loopback connection to our own
 * Redis port, attached to the main event loop. Lives for the process. */
static redisAsyncContext *mgn_async_ctx = NULL;

static void mgnAsyncDisconnectCb(const redisAsyncContext *c, int status) {
    UNUSED(c); UNUSED(status);
    /* hiredis frees the context on disconnect; null our global so the next
     * call reconnects. */
    mgn_async_ctx = NULL;
}

static void mgnAsyncReplyCb(redisAsyncContext *c, void *r, void *privdata) {
    UNUSED(c);
    char *type = privdata;
    redisReply *reply = r;
    if (!reply) {
        serverLog(LL_WARNING, "RAFT.MGN-LOG %s (async): null reply (disconnected)", type);
    } else if (reply->type == REDIS_REPLY_ERROR) {
        serverLog(LL_WARNING, "RAFT.MGN-LOG %s (async): error: %s", type, reply->str);
    } else {
        serverLog(LL_NOTICE, "RAFT.MGN-LOG %s (async) acked", type);
    }
    zfree(type);
}

/* Fire-and-forget log of a migration protocol event from a MAIN-THREAD site
 * (e.g., mergeBackpatchTick). Lazy-attaches one shared async connection to
 * 127.0.0.1:<server.port>; sends RAFT.MGN-LOG without blocking. The reply
 * is logged from the callback. Safe ONLY from the main thread (event-loop
 * thread); worker threads must use rdmaMgnLogSync below. */
static void rdmaMgnLogAsync(const char *type, const char *payload) {
    if (mgn_async_ctx == NULL) {
        redisAsyncContext *ac = redisAsyncConnect("127.0.0.1", server.port);
        if (ac == NULL || ac->err) {
            serverLog(LL_WARNING,
                "RAFT.MGN-LOG %s (async): connect failed: %s",
                type, ac ? ac->errstr : "(null ctx)");
            if (ac) redisAsyncFree(ac);
            return;
        }
        if (mgnRedisAeAttach(server.el, ac) != C_OK) {
            serverLog(LL_WARNING,
                "RAFT.MGN-LOG %s (async): event-loop attach failed", type);
            redisAsyncFree(ac);
            return;
        }
        redisAsyncSetDisconnectCallback(ac, mgnAsyncDisconnectCb);
        mgn_async_ctx = ac;
    }

    int ret = redisAsyncCommand(mgn_async_ctx, mgnAsyncReplyCb,
                                zstrdup(type),
                                "RAFT.MGN-LOG %s %s", type, payload);
    if (ret != REDIS_OK) {
        serverLog(LL_WARNING,
            "RAFT.MGN-LOG %s (async): redisAsyncCommand failed", type);
    }
}

/* Synchronously append a migration protocol event into the local redisraft
 * Raft log. Issued by migrationWorker (donor side) and registerWorkerThread
 * (recipient side) at phase boundaries, so all replicas in the local replica
 * group durably record the session's progress.
 *
 * Sync hiredis to 127.0.0.1:<this server's port> — same pattern the existing
 * RDMA control RPCs already use. Safe from worker threads; NOT safe from the
 * main Redis thread (would deadlock — main thread is the one that processes
 * RAFT.MGN-LOG). The two main-thread sites (mergeBackpatchTick INDX_UPD /
 * RECP_TXN_DONE) need a different mechanism — out of scope for 1c.1. */
static void rdmaMgnLogSync(const char *type, const char *payload) {
    redisContext *ctx = redisConnect("127.0.0.1", server.port);
    if (ctx == NULL || ctx->err) {
        serverLog(LL_WARNING,
            "RAFT.MGN-LOG %s: redisConnect(127.0.0.1:%d) failed: %s",
            type, server.port, ctx ? ctx->errstr : "(null ctx)");
        if (ctx) redisFree(ctx);
        return;
    }

    redisReply *r = redisCommand(ctx, "RAFT.MGN-LOG %s %s", type, payload);
    if (r == NULL) {
        serverLog(LL_WARNING,
            "RAFT.MGN-LOG %s: command failed: %s",
            type, ctx->errstr);
    } else if (r->type == REDIS_REPLY_ERROR) {
        serverLog(LL_WARNING,
            "RAFT.MGN-LOG %s: error reply: %s", type, r->str);
    } else if (r->type == REDIS_REPLY_STATUS &&
               r->len == 2 && memcmp(r->str, "OK", 2) == 0) {
        serverLog(LL_NOTICE,
            "RAFT.MGN-LOG %s logged: %s", type, payload);
    } else {
        serverLog(LL_WARNING,
            "RAFT.MGN-LOG %s: unexpected reply (type=%d)", type, r->type);
    }
    if (r) freeReplyObject(r);
    redisFree(ctx);
}

/* ---- AqRaft helpers (cluster_enabled bypass for RedisRaft donors) ------- *
 *
 * RedisRaft refuses to load with cluster_enabled=yes, but the migration
 * path needs slot ownership + node identity. These helpers consult
 * server.rdma_migration_redisraft_slots ("low:high") when server.cluster
 * is NULL — i.e. when RedisRaft is loaded and we're in redisraft-mode
 * for migration purposes.
 *
 * rdmaMigrationGuard: returns C_OK if this node can participate in a
 *   migration as a donor; C_ERR if not (sends client error). Replaces
 *   the legacy "server.cluster == NULL || server.cluster->myself == NULL"
 *   check.
 *
 * rdmaMigrationOwnsSlot: true if this node owns slot, via either
 *   server.cluster->slots[] (vanilla cluster) or rdma_migration_redisraft_slots.
 *
 * rdmaMigrationParseSlotRange: parse "low:high" into ints. -1 ranges
 *   on failure. Side-effect-free.
 *
 * rdmaMigrationSelfName: writes a stable 40-char identifier for this
 *   node into buf. Uses cluster->myself->name if available, else
 *   synthesizes from rdma_addr / port. */
static void rdmaMigrationParseSlotRange(const char *spec, int *out_lo, int *out_hi) {
    *out_lo = -1; *out_hi = -1;
    if (spec == NULL || *spec == '\0') return;
    const char *colon = strchr(spec, ':');
    if (colon == NULL) {
        char *end;
        long v = strtol(spec, &end, 10);
        if (*end == '\0' && v >= 0 && v < CLUSTER_SLOTS) { *out_lo = (int) v; *out_hi = (int) v; }
        return;
    }
    char *end;
    long lo = strtol(spec, &end, 10);
    if (end != colon || lo < 0 || lo >= CLUSTER_SLOTS) return;
    long hi = strtol(colon + 1, &end, 10);
    if (*end != '\0' || hi < lo || hi >= CLUSTER_SLOTS) return;
    *out_lo = (int) lo; *out_hi = (int) hi;
}

static int rdmaMigrationGuard(client *c) {
    if (server.cluster != NULL && server.cluster->myself != NULL) return C_OK;
    if (server.rdma_migration_redisraft_mode &&
        server.rdma_migration_redisraft_slots != NULL &&
        sdslen(server.rdma_migration_redisraft_slots) > 0) {
        int lo, hi;
        rdmaMigrationParseSlotRange(server.rdma_migration_redisraft_slots, &lo, &hi);
        if (lo >= 0 && hi >= lo) return C_OK;
        addReplyError(c, "rdma-migration-redisraft-slots is set but malformed (expected 'low:high')");
        return C_ERR;
    }
    addReplyError(c, "cluster mode not enabled on this node");
    return C_ERR;
}

static int rdmaMigrationOwnsSlot(int slot) {
    if (server.cluster != NULL && server.cluster->myself != NULL) {
        return server.cluster->slots[slot] == server.cluster->myself;
    }
    if (server.rdma_migration_redisraft_mode &&
        server.rdma_migration_redisraft_slots != NULL &&
        sdslen(server.rdma_migration_redisraft_slots) > 0) {
        int lo, hi;
        rdmaMigrationParseSlotRange(server.rdma_migration_redisraft_slots, &lo, &hi);
        return (lo >= 0 && slot >= lo && slot <= hi);
    }
    return 0;
}

static void rdmaMigrationSelfName(char *buf /* >= CLUSTER_NAMELEN+1 */) {
    if (server.cluster != NULL && server.cluster->myself != NULL) {
        memcpy(buf, server.cluster->myself->name, CLUSTER_NAMELEN);
        buf[CLUSTER_NAMELEN] = '\0';
        return;
    }
    /* Synthesize: "AQRAFT_" + 33 ascii of "<port>_<rand>". Stable for a
     * given instance lifetime; doesn't survive restart but that's fine
     * for in-flight migration accounting which is per-session. */
    static char synth[CLUSTER_NAMELEN + 1];
    static int initialized = 0;
    if (!initialized) {
        snprintf(synth, sizeof(synth), "AQRAFT_%05d_%08x_padddddd_padddddd_pa",
                 server.port, (unsigned int) (mstime() & 0xFFFFFFFFu));
        synth[CLUSTER_NAMELEN] = '\0';
        initialized = 1;
    }
    memcpy(buf, synth, CLUSTER_NAMELEN);
    buf[CLUSTER_NAMELEN] = '\0';
}

/* ---- Phase C: chain batch encode / apply -------------------------------- *
 *
 * Wire format (leader → followers, written into the chain landing pool):
 *
 *   [u32 magic = CHAIN_BATCH_MAGIC]
 *   [u32 n_slots]
 *   per-slot:
 *     [u32 slot_id]
 *     [u32 n_entries]
 *     per-entry:
 *       [u32 klen][key bytes]
 *       [u32 vlen][val bytes]
 *
 * This is a packed equivalent of what rdmaEncodeSlotEntries produces for
 * one slot, repeated across the batch's covered slots. Smaller than the
 * RDMAMIG-format per-slot block (which always reserves RDMAMIG_BLOCK_SIZE_BYTES
 * = 2 MiB) because the chain pool is shared across slots and sized for
 * actual content. */

#define CHAIN_BATCH_MAGIC 0xC4A1F00DU

/* Encode the kvstore entries for the given slots into buf. Returns the
 * number of bytes written, or 0 on failure (e.g. buf too small). Callable
 * from main thread; iterates via kvstoreDictIterator the same way
 * rdmaEncodeSlotEntries does. */
/* rdmaApplySlotBlock — raw r_allocator block installer (Patch 9).
 *
 * The donor's TRANSFER step now ships its r_allocator block byte-for-byte.
 * Each kvobj segment in the block is fully self-contained:
 *  - kvobj struct (robj header with iskvobj=1, encoding=OBJ_ENCODING_R_ALLOCATOR,
 *    data_offset = (slot << 18) | value_offset_within_segment).
 *  - embedded key sds at (kv+1)+1.
 *  - embedded value sds at (kv+1)+1+key_sds_size.
 *
 * Only kv->ptr (absolute pointer to the embedded value sds) needs fixup
 * after RDMA-WRITE — recompute it from kv->data_offset which stores the
 * in-segment value offset. Then kvstoreDictAddRaw the kvobj directly,
 * skipping the per-key r_allocator_insert_kvobj + sdsnewlen allocations
 * the old wire-format path required. */

typedef struct {
    redisDb *db;
    int slot;
    int installed;
    int skipped_existing;
    int skipped_invalid;
    dict *shadow;  /* optional: when non-NULL, install into this dict (shadow path); */
                   /* when NULL, install directly into db->keys (chain follower path). */
} applySlotCtx;

static void applySlotCb(void *seg_payload, size_t seg_payload_size, void *user) {
    applySlotCtx *c = (applySlotCtx *) user;
    kvobj *kv = (kvobj *) seg_payload;
    /* Sanity #1: an r_allocator-format segment carries a self-describing
     * kvobj. iskvobj is 1 bit, encoding is 4 bits — together 5 bits of
     * filtering. With thousands of segments scanned, garbage that happens
     * to satisfy these two checks will sneak past at ~1/32 rate; further
     * validation below catches the rest. */
    if (kv->iskvobj != 1 || kv->encoding != OBJ_ENCODING_R_ALLOCATOR) {
        c->skipped_invalid++;
        return;
    }
    unsigned val_off = R_ALLOC_GET_OFFSET(kv);
    if (val_off == 0 || val_off >= seg_payload_size) {
        c->skipped_invalid++;
        return;
    }
    /* Sanity #2 (AqRaft Patch 18): validate the embedded key sds layout
     * BEFORE we let dictAddOrFind hash it. kvobjGetKey reads hdr_size from
     * the byte right after the robj header, then advances 1+hdr_size to
     * reach the sds char data; siphash then calls sdslen which reads more
     * metadata. If hdr_size is garbage (random byte 0..255) the key ptr
     * lands off in space and siphash dereferences invalid memory -> SEGV.
     * Valid sds hdr_size is one of {0, 2, 4, 8, 16} for sdshdr5/8/16/32/64,
     * and the resulting key sds end must fit inside the segment. */
    unsigned char *kv_data = (unsigned char *)(kv + 1);
    /* Bounds: kv_data must be inside the segment payload. */
    if ((char *)kv_data >= (char *)kv + seg_payload_size) {
        c->skipped_invalid++;
        return;
    }
    uint8_t hdr_size = *kv_data;
    /* Strict allowlist of sds header sizes. sdsHdrSize() returns the byte
     * count of the sds header INCLUDING the 1-byte flags/type field, which
     * is one of {1, 3, 5, 9, 17} for sdshdr5/sdshdr8/sdshdr16/sdshdr32/sdshdr64
     * respectively (see redis/src/sds.h). Anything else = garbage. */
    if (hdr_size != 1 && hdr_size != 3 && hdr_size != 5 &&
        hdr_size != 9 && hdr_size != 17) {
        c->skipped_invalid++;
        return;
    }
    /* The key sds char* lives at kv_data + 1 + hdr_size. It must be
     * inside the segment payload, and the implied value-offset must come
     * after the key. */
    size_t key_meta_end_off = (size_t)((char *)kv_data - (char *)kv) + 1 + hdr_size;
    if (key_meta_end_off >= seg_payload_size || key_meta_end_off >= val_off) {
        c->skipped_invalid++;
        return;
    }
    /* Repoint kv->ptr to the embedded value sds at its in-segment offset.
     * The donor's address space is irrelevant — the offset is stable. */
    kv->ptr = (char *) kv + val_off;
    /* Slot field in data_offset is the DONOR's slot id; recipient uses the
     * same slot ids during migration so no re-pack is needed. */
    if (c->shadow != NULL) {
        /* Shadow path: dictAdd into shadow dict (key = kvobj, no_value=1). */
        dictAddOrFind(c->shadow, kv);
        c->installed++;
    } else {
        dictEntry *existing = NULL;
        dictEntry *added = kvstoreDictAddRaw(c->db->keys, c->slot, kv, &existing);
        if (added != NULL) {
            c->installed++;
        } else {
            /* Don't-clobber: live kvstore already has this key (client wrote
             * during migration window). Leave the kvobj orphaned in the
             * received block (block is registered to r_allocator and never
             * freed for the lifetime of the session). */
            c->skipped_existing++;
        }
    }
}

int rdmaApplySlotBlock(redisDb *db, int slot, const char *buf, size_t buf_size) {
    if (slot < 0 || slot >= CLUSTER_SLOTS) return 0;
    if (buf == NULL || buf_size < 2 * sizeof(uint32_t)) return 0;
    applySlotCtx c = { db, slot, 0, 0, 0, NULL };
    /* The block was DMA'd into a region already prologued by
     * r_allocator_register_existing_block / init_bloc_layout, so the segment
     * walk respects the same layout the donor emits. */
    r_allocator_walk_used_segments((char *) buf, applySlotCb, &c);
    /* AqRaft Patch 17: sanitize donor-shipped block. Flips every free
     * segment's alloc-bit to 1 so future coalesce-on-free never tries to
     * merge with a "free" neighbor whose freelist pointers refer to the
     * donor's address space (would SIGSEGV in freelist_remove_segment).
     * Done AFTER the walker so applySlotCb sees the real alloc bits. */
    r_allocator_sanitize_imported_block((char *) buf);
    /* The freelist for this slot is stale (init_bloc_layout said "whole block
     * is one free segment", but now donor's used segments occupy it). Reset
     * so any subsequent client write to this slot allocates a fresh block. */
    r_allocator_reset_freelist_for_slot(slot);
    if (c.installed > 0 || c.skipped_invalid > 0) {
        serverLog(LL_VERBOSE,
            "CHAIN apply: slot=%d installed=%d skipped_existing=%d "
            "skipped_invalid=%d",
            slot, c.installed, c.skipped_existing, c.skipped_invalid);
    }
    return c.installed;
}

/* ---- Phase B.5 chain establishment helper ------------------------------- */

/* One-shot detached pthread arg: chain-establish job. Owns all fields. */
typedef struct {
    long long src_mig_id;
    long long pool_bytes;
    int n_peers;
    sds *hosts;
    int *ports;
} chainEstablishJob;

static void *chainEstablishThread(void *arg) {
    chainEstablishJob *job = arg;
    char errbuf[256] = {0};
    const char **host_arr = zmalloc((size_t) job->n_peers * sizeof(char *));
    for (int i = 0; i < job->n_peers; i++) host_arr[i] = (const char *) job->hosts[i];
    int rc = rdmaLeaderChainEstablish(job->src_mig_id, job->pool_bytes,
                                      job->n_peers, host_arr, job->ports,
                                      errbuf, sizeof(errbuf));
    if (rc != C_OK) {
        serverLog(LL_WARNING,
            "CHAIN: leaderEstablish sess=%lld failed: %s",
            job->src_mig_id, errbuf);
    } else {
        serverLog(LL_NOTICE,
            "CHAIN: leaderEstablish sess=%lld OK (%d followers)",
            job->src_mig_id, job->n_peers);

        /* AqRaft Patch 15: pre-register the leader's src pool HERE, off the
         * main thread. The ibv_reg_mr for 2.86 GB takes ~3 sec — if we let
         * rdmaLeaderChainForwardPerSlot lazy-init it on the main thread the
         * first time a backpatch batch finishes, the event loop stalls past
         * the raft check-quorum window (election_timeout * 2 = 2000ms) and
         * the leader steps down. */
        char ensure_err[256] = {0};
        if (rdmaLeaderChainEnsureSrcPool(job->src_mig_id, ensure_err,
                                         sizeof(ensure_err)) != C_OK) {
            serverLog(LL_WARNING,
                "CHAIN: sess=%lld src pool pre-register failed: %s "
                "(will fall back to main-thread lazy-init)",
                job->src_mig_id, ensure_err);
        }
    }
    zfree(host_arr);
    for (int i = 0; i < job->n_peers; i++) sdsfree(job->hosts[i]);
    zfree(job->hosts);
    zfree(job->ports);
    zfree(job);
    return NULL;
}

/* AqRaft Patch 16(B/C/D) chainForwardJob + chainForwardWorker definitions live
 * further down, after `backpatchBatch` is fully declared (around line 1410)
 * and after `backpatchFinalize` is defined. Forward-declared here so
 * mergeBackpatchTick can spawn the worker. */
struct chainForwardJob;
static void *chainForwardWorker(void *arg);

/* Parse "host1:port1 host2:port2 ..." from rdma_chain_followers, spawn one
 * detached pthread to drive rdmaLeaderChainEstablish. Returns immediately;
 * the main thread is never blocked by RDMA QP setup. Called from
 * DONE-SLOTS-INIT (main thread). */
static void rdmaChainSpawnEstablish(long long src_mig_id, long long pool_bytes,
                                    sds followers_str) {
    /* Skip if a chain is already established for this session (e.g., a
     * retry of DONE-SLOTS-INIT). rdmaLeaderChainAckCount returns -1 when
     * no state exists, ≥ 0 once leaderEstablishChain has stored state. */
    if (rdmaLeaderChainAckCount(src_mig_id) >= 0) {
        serverLog(LL_VERBOSE,
            "CHAIN: leaderEstablish sess=%lld already in progress/done",
            src_mig_id);
        return;
    }

    int count;
    sds *tokens = sdssplitlen(followers_str, sdslen(followers_str),
                              " ", 1, &count);
    if (tokens == NULL || count == 0) {
        if (tokens) sdsfreesplitres(tokens, count);
        serverLog(LL_WARNING,
            "CHAIN: rdma-chain-followers empty after split — skipping");
        return;
    }

    /* Filter out empty tokens (multiple spaces in config). */
    chainEstablishJob *job = zcalloc(sizeof(*job));
    job->src_mig_id = src_mig_id;
    job->pool_bytes = pool_bytes;
    job->hosts = zmalloc((size_t) count * sizeof(sds));
    job->ports = zmalloc((size_t) count * sizeof(int));
    for (int i = 0; i < count; i++) {
        if (sdslen(tokens[i]) == 0) continue;
        int sub;
        sds *hp = sdssplitlen(tokens[i], sdslen(tokens[i]), ":", 1, &sub);
        if (hp == NULL || sub != 2) {
            serverLog(LL_WARNING,
                "CHAIN: rdma-chain-followers token '%s' is not host:port — skipping",
                tokens[i]);
            if (hp) sdsfreesplitres(hp, sub);
            continue;
        }
        long long port_ll;
        if (string2ll(hp[1], sdslen(hp[1]), &port_ll) == 0 ||
            port_ll <= 0 || port_ll > 65535) {
            serverLog(LL_WARNING,
                "CHAIN: rdma-chain-followers '%s' has invalid port — skipping",
                tokens[i]);
            sdsfreesplitres(hp, sub);
            continue;
        }
        job->hosts[job->n_peers] = sdsdup(hp[0]);
        job->ports[job->n_peers] = (int) port_ll;
        job->n_peers++;
        sdsfreesplitres(hp, sub);
    }
    sdsfreesplitres(tokens, count);

    if (job->n_peers == 0) {
        zfree(job->hosts);
        zfree(job->ports);
        zfree(job);
        serverLog(LL_WARNING,
            "CHAIN: rdma-chain-followers had no valid entries");
        return;
    }

    pthread_t tid;
    if (pthread_create(&tid, NULL, chainEstablishThread, job) != 0) {
        serverLog(LL_WARNING,
            "CHAIN: pthread_create for chainEstablishThread failed");
        for (int i = 0; i < job->n_peers; i++) sdsfree(job->hosts[i]);
        zfree(job->hosts);
        zfree(job->ports);
        zfree(job);
        return;
    }
    pthread_detach(tid);
    serverLog(LL_NOTICE,
        "CHAIN: spawned chainEstablishThread sess=%lld n_followers=%d "
        "pool_bytes=%lld",
        src_mig_id, job->n_peers, pool_bytes);
}

/* ---- Connection cache ---------------------------------------------------- */

/* Cache an RDMA connection keyed by client id, so subsequent RPCs from the
 * same control client can recover the cm_id and slot-buffer metadata. */
static void rdmaAddConnection(client *c, struct rdmamig_server *s_rdma) {
    rdmaCachedConnection *cs = zmalloc(sizeof(*cs));
    cs->s = s_rdma;
    cs->db = c->db;
    cs->c = c;
    cs->number_of_assigned_slots = 0;

    sds key = sdsfromlonglong(c->id);
    dictAdd(server.rdma_cached_connections, key, cs);
}

static rdmaCachedConnection *rdmaGetConnection(client *c) {
    sds key = sdsfromlonglong(c->id);
    rdmaCachedConnection *cs = dictFetchValue(server.rdma_cached_connections, key);
    sdsfree(key);
    return cs;
}

/* ---- INIT-CLIENT / INIT-SERVER ------------------------------------------ */

void rdmaInitClientCommand(client *c) {
    /* argv[0]=RDMA argv[1]=INIT-CLIENT argv[2]=ip argv[3]=port */
    char *ip = c->argv[2]->ptr;
    char *port = c->argv[3]->ptr;

    struct rdmamig_client *ci = rdmamig_client_create(ip, port);
    if (ci == NULL) {
        addReplyError(c, "rdmamig_client_create failed");
        return;
    }
    if (rdmamig_client_connect(ci) != 0) {
        addReplyError(c, "RDMA connect failed");
        zfree(ci);
        return;
    }

    if (server.rdma_client != NULL) {
        zfree(server.rdma_client);
    }
    server.rdma_client = ci;
    addReply(c, shared.ok);
}

void rdmaInitServerCommand(client *c) {
    /* argv[0]=RDMA argv[1]=INIT-SERVER argv[2]=port */
    char *rdma_server_port = c->argv[2]->ptr;
    serverLog(LL_NOTICE, "RDMA INIT-SERVER: client_id=%llu requesting RDMA listener on port %s",
              (unsigned long long) c->id, rdma_server_port);

    struct rdmamig_server *s = rdmamig_server_create(rdma_server_port);
    if (s == NULL) {
        serverLog(LL_WARNING, "RDMA INIT-SERVER: rdmamig_server_create(%s) failed", rdma_server_port);
        addReplyError(c, "rdmamig_server_create failed");
        return;
    }
    /* The accepted-connection cm_id is set by the listening thread once a
     * peer connects. NULL here just means we haven't seen a donor yet, which
     * is fine -- REGISTER-BLOCK-SLOTS will fail later with a clearer error
     * if the caller proceeds before the connection is established. */

    if (server.rdma_server == NULL) {
        server.rdma_server = s;
        serverLog(LL_NOTICE, "RDMA INIT-SERVER: bound RDMA listener on port %s (server.rdma_server set)",
                  rdma_server_port);
    } else {
        serverLog(LL_NOTICE, "RDMA INIT-SERVER: server.rdma_server already set; using new server for this client only");
    }
    rdmaAddConnection(c, s);
    serverLog(LL_NOTICE, "RDMA INIT-SERVER: client_id=%llu cached connection, awaiting donor",
              (unsigned long long) c->id);
    addReply(c, shared.ok);
}

/* ---- REGISTER-BLOCK-SLOTS ----------------------------------------------- */

/* Phase 4d (callback-RPC variant): registerJob carries the source's call-back
 * host:port + a register_id so the recipient's worker thread, after finishing
 * the heavy ibv_reg_mr loop, can dial back to the source and deliver the
 * results via a fresh hiredis connection issuing `RDMA REGISTER-RESULT`.
 * No blockClient on the recipient — the recipient just replies +OK
 * immediately. */
typedef struct registerJob {
    rdmaCachedConnection *conn;
    int        n_pairs;
    int       *slot_ids;
    int       *n_blocks_per_slot;
    rdmaRemoteBufferInfo *result;
    int        total_buffers;
    int        has_error;
    char       err_msg[256];
    /* Callback target on the source side (parsed from REGISTER-BLOCK-SLOTS). */
    char       register_id[64];     /* unique correlation id */
    char       src_host[128];       /* source's host */
    int        src_port;            /* source's master Redis port */
} registerJob;

static void *registerWorkerThread(void *arg);

/* RDMA REGISTER-BLOCK-SLOTS <register_id> <src-host> <src-port>
 *                            SLOTS <slot> <nblocks> [<slot> <nblocks> ...]
 *
 * Recipient side (Phase 4d callback-RPC variant): records the callback
 * coordinates (register_id + src-host + src-port), enqueues a worker job
 * to do the heavy ibv_reg_mr loop, and replies +OK immediately. The
 * worker thread, when done, opens a fresh TCP connection back to
 * src-host:src-port and issues `RDMA REGISTER-RESULT <register_id>
 * <binary-VA-rkey-tuples>` to deliver the result. */
void rdmaRegisterBlockSlotsCommand(client *c) {
    rdmaCachedConnection *cs = rdmaGetConnection(c);
    if (cs == NULL) {
        addReplyError(c, "no RDMA connection cached for this client; call RDMA INIT-SERVER first");
        return;
    }

    /* Phase 4d wire format:
     *   argv[0]=RDMA argv[1]=REGISTER-BLOCK-SLOTS
     *   argv[2]=<register_id> argv[3]=<src-host> argv[4]=<src-port>
     *   argv[5]=SLOTS argv[6..]=<slot> <nblocks> ... */
    if (c->argc < 7 || strcasecmp(c->argv[5]->ptr, "SLOTS") != 0) {
        addReplyError(c,
            "syntax: RDMA REGISTER-BLOCK-SLOTS register_id src_host src_port SLOTS slot nblocks ...");
        return;
    }
    long long src_port_ll;
    if (getLongLongFromObject(c->argv[4], &src_port_ll) != C_OK ||
        src_port_ll <= 0 || src_port_ll > 65535) {
        addReplyError(c, "src_port out of range");
        return;
    }
    int start_idx = 6;
    if (((c->argc - start_idx) % 2) != 0) {
        addReplyError(c, "expected an even number of arguments after SLOTS");
        return;
    }

    int total_requested_blocks = 0;
    for (int idx = start_idx; idx < c->argc; idx += 2) {
        long long n;
        if (getLongLongFromObject(c->argv[idx + 1], &n) != C_OK || n < 0) {
            addReplyError(c, "n-blocks must be a non-negative integer");
            return;
        }
        total_requested_blocks += (int) n;
    }

    int n_pairs = (c->argc - start_idx) / 2;
    const char *register_id = c->argv[2]->ptr;
    const char *src_host    = c->argv[3]->ptr;
    serverLog(LL_NOTICE,
        "RDMA REGISTER-BLOCK-SLOTS: register_id=%s src=%s:%lld enqueueing %d blocks across %d pairs",
        register_id, src_host, src_port_ll, total_requested_blocks, n_pairs);

    /* Build the job. */
    registerJob *job = zmalloc(sizeof(*job));
    job->conn = cs;
    job->n_pairs = n_pairs;
    job->slot_ids = zmalloc((size_t) n_pairs * sizeof(int));
    job->n_blocks_per_slot = zmalloc((size_t) n_pairs * sizeof(int));
    job->result = zmalloc((size_t) total_requested_blocks * sizeof(rdmaRemoteBufferInfo));
    job->total_buffers = 0;
    job->has_error = 0;
    job->err_msg[0] = '\0';
    snprintf(job->register_id, sizeof(job->register_id), "%s", register_id);
    snprintf(job->src_host, sizeof(job->src_host), "%s", src_host);
    job->src_port = (int) src_port_ll;

    int pi = 0;
    for (int idx = start_idx; idx + 1 < c->argc; idx += 2, pi++) {
        long long slot_id, n_blocks;
        if (getLongLongFromObject(c->argv[idx], &slot_id) != C_OK ||
            slot_id < 0 || slot_id >= CLUSTER_SLOTS) {
            zfree(job->slot_ids); zfree(job->n_blocks_per_slot);
            zfree(job->result); zfree(job);
            addReplyError(c, "slot id out of range");
            return;
        }
        if (getLongLongFromObject(c->argv[idx + 1], &n_blocks) != C_OK || n_blocks < 0) {
            zfree(job->slot_ids); zfree(job->n_blocks_per_slot);
            zfree(job->result); zfree(job);
            addReplyError(c, "n-blocks must be a non-negative integer");
            return;
        }
        job->slot_ids[pi] = (int) slot_id;
        job->n_blocks_per_slot[pi] = (int) n_blocks;
    }

    /* Aqueduct: pause databasesCron on this recipient while the backpatch is in
     * flight. databasesCron's expire/defrag/resize/rehash all iterate the
     * recipient's kvstore internals (kvs->rehashing list, per-slot dict
     * resize, allocator slot_blocks) that the backpatch thread is concurrently
     * mutating via dbAdd. Per-slot rwlock doesn't cover this cross-slot
     * shared state. Reset in rdmaReshardRecvFlipCommand once the backpatch
     * completes and ownership swaps. */
    atomicIncr(server.recipient_backpatch_in_progress, 1);

    /* Aqueduct: defer Fenwick-tree updates on the keys kvstore while backpatch
     * is in flight. Each dbAdd would otherwise hit the cross-slot Fenwick
     * mutex, serializing the backpatch worker against concurrent main-thread
     * inserts. The tree only feeds RANDOMKEY / SCAN-style weighted picks
     * (not used by YCSB), so transient staleness is fine. Cleared + rebuilt
     * once at BACKPATCH_DONE. */
    kvstoreSetDeferFenwickUpdates(server.db[0].keys, 1);

    /* Aqueduct slot-state: flip each slot to MIGRATING with the donor's
     * host:port as the peer endpoint *before* replying +OK, so the donor
     * sees the recipient already in MIGRATING when it sets its own state
     * (closes the PREP race). Also set importing_slots_from[slot] so
     * Path B narrow keyspace wraps in db.c kick in for the backpatch window —
     * main-thread reads/writes on the recipient take clusterSlotLockRead
     * (or Write) while the backpatch thread holds clusterSlotLockWrite for its
     * dbAdd, serializing kvstore mutations through the per-slot rwlock. */
    char donor_endpoint[NET_HOST_PORT_STR_LEN];
    snprintf(donor_endpoint, sizeof(donor_endpoint), "%s:%lld",
             src_host, src_port_ll);

    /* Find the donor clusterNode by addr — needed for importing_slots_from. */
    clusterNode *donor_node = NULL;
    if (server.cluster != NULL && server.cluster->nodes != NULL) {
        dictIterator *di = dictGetSafeIterator(server.cluster->nodes);
        dictEntry *de;
        while ((de = dictNext(di)) != NULL) {
            clusterNode *n = dictGetVal(de);
            if (n != NULL && strcmp(n->ip, src_host) == 0 &&
                (int) n->tcp_port == (int) src_port_ll) {
                donor_node = n;
                break;
            }
        }
        dictReleaseIterator(di);
    }
    for (int i = 0; i < n_pairs; i++) {
        int slot = job->slot_ids[i];
        slotMigStateSet(slot, SLOT_STATE_MIGRATING, donor_endpoint);
        if (donor_node != NULL) {
            clusterSlotLockWrite(slot);
            server.cluster->importing_slots_from[slot] = donor_node;
            clusterSlotUnlock(slot);
        }
    }

    /* Spawn the detached worker thread. */
    pthread_t tid;
    int err = pthread_create(&tid, NULL, registerWorkerThread, job);
    if (err != 0) {
        addReplyErrorFormat(c, "RDMA REGISTER-BLOCK-SLOTS: pthread_create failed: %s",
                            strerror(err));
        zfree(job->slot_ids); zfree(job->n_blocks_per_slot);
        zfree(job->result); zfree(job);
        return;
    }
    pthread_detach(tid);

    /* Reply immediately. The actual result lands on the source later via
     * RDMA REGISTER-RESULT delivered by the worker on a fresh connection. */
    addReplyStatusFormat(c, "OK register_id=%s enqueued n_pairs=%d total_blocks=%d",
                         register_id, n_pairs, total_requested_blocks);
}

/* ---- TRANSFER-SLOTS (donor) --------------------------------------------- */

/* Encode entries in the simple flat format described at the top of the file.
 * Returns the number of entries packed. Bails when the next entry would
 * overflow buf_size. Caller passes *cursor pointing at the start of the
 * buffer; on return *cursor points at the last written byte + 1 and the
 * leading uint32 count is filled in. */
static uint32_t rdmaEncodeSlotEntries(redisDb *db, int slot, char *buf, size_t buf_size) {
    if (buf_size < sizeof(uint32_t)) return 0;
    char *p = buf + sizeof(uint32_t);   /* leave room for n_entries header */
    char *end = buf + buf_size;
    uint32_t n_entries = 0;

    if (kvstoreDictSize(db->keys, slot) == 0) {
        memset(buf, 0, sizeof(uint32_t));
        return 0;
    }

    kvstoreDictIterator it;
    kvstoreInitDictIterator(&it, db->keys, slot);
    dictEntry *de;
    while ((de = kvstoreDictIteratorNext(&it)) != NULL) {
        kvobj *kv = (kvobj *) dictGetKey(de);  /* in 8.6.2 the kvobj is stored as the dict key */
        sds key_sds = (sds) kvobjGetKey(kv);
        size_t klen = sdslen(key_sds);

        /* Only OBJ_STRING values are supported in this slim port. */
        if (kv->type != OBJ_STRING) continue;
        size_t vlen;
        char *vptr;
        char vbuf[LONG_STR_SIZE];
        if (kv->encoding == OBJ_ENCODING_INT) {
            vlen = ll2string(vbuf, sizeof(vbuf), (long)(intptr_t)kv->ptr);
            vptr = vbuf;
        } else {
            vptr = kv->ptr;
            vlen = sdslen((sds) kv->ptr);
        }

        size_t need = sizeof(uint32_t) + klen + sizeof(uint32_t) + vlen;
        if (p + need > end) break;

        uint32_t klen32 = (uint32_t) klen;
        uint32_t vlen32 = (uint32_t) vlen;
        memcpy(p, &klen32, sizeof(klen32)); p += sizeof(klen32);
        memcpy(p, key_sds, klen);            p += klen;
        memcpy(p, &vlen32, sizeof(vlen32)); p += sizeof(vlen32);
        memcpy(p, vptr, vlen);                p += vlen;
        n_entries++;
    }
    kvstoreResetDictIterator(&it);

    memcpy(buf, &n_entries, sizeof(n_entries));
    return n_entries;
}

/* Send `cmd` (formatted as RESP protocol bytes in `buf`) to the recipient
 * and read a single line of reply into `reply` (caller-allocated). Returns
 * C_OK / C_ERR. */
static int rdmaSendAndReadLine(connection *conn, sds buf, char *reply, size_t reply_size) {
    if (connSyncWrite(conn, buf, sdslen(buf), 1000) != (ssize_t) sdslen(buf)) return C_ERR;
    if (connSyncReadLine(conn, reply, reply_size, 5000) <= 0) return C_ERR;
    if (reply[0] == '-') return C_ERR;
    return C_OK;
}

/* RDMA TRANSFER-SLOTS <recip-ip> <recip-port> <slot> [<slot> ...]
 *
 * Donor side. Slim 8.6.2 replacement for the source's 819-line
 * migrateRDMASlotsCommandThread. Steps:
 *   1. Open a regular Redis (cluster-bus) connection to the recipient.
 *   2. Allocate one local RDMA-registered staging block per slot, encode
 *      that slot's keys into it in the flat format.
 *   3. RPC: RDMA REGISTER-BLOCK-SLOTS SLOTS <slot> 1 ... to the recipient
 *      (one block per slot). Receive the (VA, rkey) array back.
 *   4. RDMA-write each staging block into the corresponding recipient buffer.
 *   5. RPC: RDMA DONE-SLOTS <slot> ... to trigger the backpatch.
 *   6. Reply +OK to the original caller.
 *
 * No ownership flip, no thread, no batching. */
void rdmaTransferSlotsCommand(client *c) {
    /* argv[0]=RDMA argv[1]=TRANSFER-SLOTS argv[2]=ip argv[3]=port argv[4..]=slots */
    if (server.rdma_client == NULL) {
        addReplyError(c, "RDMA client not initialized; call RDMA INIT-CLIENT first");
        return;
    }

    char *recip_ip = c->argv[2]->ptr;
    long long recip_port_ll;
    if (getLongLongFromObject(c->argv[3], &recip_port_ll) != C_OK || recip_port_ll <= 0 || recip_port_ll > 65535) {
        addReplyError(c, "invalid recipient port");
        return;
    }
    int recip_port = (int) recip_port_ll;

    int n_slots = c->argc - 4;
    if (n_slots <= 0) { addReplyError(c, "no slots specified"); return; }

    int *slots = zmalloc(n_slots * sizeof(int));
    for (int i = 0; i < n_slots; i++) {
        long long s;
        if (getLongLongFromObject(c->argv[4 + i], &s) != C_OK ||
            s < 0 || s >= CLUSTER_SLOTS) {
            zfree(slots);
            addReplyError(c, "slot id out of range");
            return;
        }
        slots[i] = (int) s;
    }

    /* Open a control connection to the recipient. */
    connection *conn = connCreate(server.el, connTypeOfCluster());
    if (connBlockingConnect(conn, recip_ip, recip_port, 1000) != C_OK) {
        zfree(slots);
        connClose(conn);
        addReplyError(c, "could not connect to recipient");
        return;
    }
    connEnableTcpNoDelay(conn);

    /* Build "RDMA REGISTER-BLOCK-SLOTS SLOTS s 1 s 1 ..." (one block per slot). */
    rio cmd;
    rioInitWithBuffer(&cmd, sdsempty());
    int n_args = 3 + 1 + (n_slots * 2);  /* RDMA REGISTER-BLOCK-SLOTS SLOTS [s 1]... */
    rioWriteBulkCount(&cmd, '*', n_args);
    rioWriteBulkString(&cmd, "RDMA", 4);
    rioWriteBulkString(&cmd, "REGISTER-BLOCK-SLOTS", 20);
    rioWriteBulkString(&cmd, "SLOTS", 5);
    for (int i = 0; i < n_slots; i++) {
        rioWriteBulkLongLong(&cmd, slots[i]);
        rioWriteBulkLongLong(&cmd, 1);
    }
    sds buf = cmd.io.buffer.ptr;

    if (connSyncWrite(conn, buf, sdslen(buf), 5000) != (ssize_t) sdslen(buf)) {
        sdsfree(buf); zfree(slots); connClose(conn);
        addReplyError(c, "could not write REGISTER-BLOCK-SLOTS to recipient");
        return;
    }
    sdsfree(buf);

    /* Read the bulk-string reply: $<len>\r\n<bytes>\r\n */
    char hdr[64];
    if (connSyncReadLine(conn, hdr, sizeof(hdr), 5000) <= 0 || hdr[0] != '$') {
        zfree(slots); connClose(conn);
        addReplyError(c, "unexpected reply header from REGISTER-BLOCK-SLOTS");
        return;
    }
    long long blen = strtoll(hdr + 1, NULL, 10);
    if (blen != (long long) (n_slots * sizeof(rdmaRemoteBufferInfo))) {
        zfree(slots); connClose(conn);
        addReplyError(c, "unexpected reply size from REGISTER-BLOCK-SLOTS");
        return;
    }
    rdmaRemoteBufferInfo *remote = zmalloc(blen);
    if (connSyncRead(conn, (char *) remote, blen, 5000) != blen) {
        zfree(remote); zfree(slots); connClose(conn);
        addReplyError(c, "short read on REGISTER-BLOCK-SLOTS reply body");
        return;
    }
    /* Drain trailing CRLF + the final +OK. */
    char ok_line[16];
    connSyncReadLine(conn, ok_line, sizeof(ok_line), 5000);  /* CRLF after bulk */
    connSyncReadLine(conn, ok_line, sizeof(ok_line), 5000);  /* +OK */

    /* Stage each slot's keys into a local RDMA-registered buffer and push. */
    int rdma_errors = 0;
    for (int i = 0; i < n_slots; i++) {
        char *staging = zmalloc(RDMAMIG_BLOCK_SIZE_BYTES);
        rdmaEncodeSlotEntries(c->db, slots[i], staging, RDMAMIG_BLOCK_SIZE_BYTES);

        struct rdmamig_buffer *rb = rdmamig_buffer_create(
            rdmamig_client_cm_id(server.rdma_client),
            staging, RDMAMIG_BLOCK_SIZE_BYTES, 0);
        if (rb == NULL) {
            zfree(staging);
            rdma_errors++;
            continue;
        }
        int rc = rdmamig_client_post_write(rb, staging, remote[i].ptr,
                                              remote[i].rkey, RDMAMIG_BLOCK_SIZE_BYTES);
        if (rc != 0) rdma_errors++;
        else rdmamig_client_wait_send(server.rdma_client);
        /* Note: not freeing staging or rb here -- the recipient may still be
         * reading. For a research prototype this leaks one buffer per slot
         * per call; acceptable until a teardown command is added. */
    }
    zfree(remote);

    if (rdma_errors > 0) {
        zfree(slots); connClose(conn);
        addReplyErrorFormat(c, "%d RDMA write error(s) during transfer", rdma_errors);
        return;
    }

    /* Send DONE-SLOTS to trigger backpatch. */
    rioInitWithBuffer(&cmd, sdsempty());
    rioWriteBulkCount(&cmd, '*', 2 + n_slots);
    rioWriteBulkString(&cmd, "RDMA", 4);
    rioWriteBulkString(&cmd, "DONE-SLOTS", 10);
    for (int i = 0; i < n_slots; i++) rioWriteBulkLongLong(&cmd, slots[i]);
    buf = cmd.io.buffer.ptr;
    if (rdmaSendAndReadLine(conn, buf, ok_line, sizeof(ok_line)) != C_OK) {
        sdsfree(buf); zfree(slots); connClose(conn);
        addReplyError(c, "DONE-SLOTS failed on recipient");
        return;
    }
    sdsfree(buf);

    zfree(slots);
    connClose(conn);
    addReply(c, shared.ok);
}

/* ---- DONE-SLOTS (recipient backpatch) --------------------------------------- */

/* Parse one entry from the buffer. Returns the number of bytes consumed,
 * or 0 if the buffer is malformed / truncated. On success *key_out and
 * *val_out are freshly created robjs the caller must decrRefCount. */
static size_t rdmaDecodeEntry(const char *buf, size_t remaining, robj **key_out, robj **val_out) {
    if (remaining < sizeof(uint32_t)) return 0;
    uint32_t klen;
    memcpy(&klen, buf, sizeof(klen));
    if (remaining < sizeof(uint32_t) + klen + sizeof(uint32_t)) return 0;
    const char *kptr = buf + sizeof(uint32_t);
    uint32_t vlen;
    memcpy(&vlen, kptr + klen, sizeof(vlen));
    if (remaining < sizeof(uint32_t) + klen + sizeof(uint32_t) + vlen) return 0;
    const char *vptr = kptr + klen + sizeof(uint32_t);

    *key_out = createStringObject(kptr, klen);
    *val_out = createStringObject(vptr, vlen);
    return sizeof(uint32_t) + klen + sizeof(uint32_t) + vlen;
}

/* Diagnostic byte dump for RDMA RESHARD-TRANSFER / rdmaBackpatchSlot. Logs first 32
 * and last 32 bytes of `buf` (length = RDMAMIG_BLOCK_SIZE_BYTES) at LL_NOTICE
 * along with the leading u32 entry count, so source/recipient log lines can
 * be cross-checked byte-for-byte. Gated on server.rdma_reshard_debug_bytes
 * (CONFIG SET cluster-rdma-reshard-debug-bytes yes to enable). `tag` is
 * "SRC" on the sender, "RCV" on the recipient. */
static void rdmaDebugDumpSlotBytes(const char *tag, int slot, const char *buf) {
    if (!server.rdma_reshard_debug_bytes) return;
    uint32_t n_entries;
    memcpy(&n_entries, buf, sizeof(n_entries));
    char hex0[3*32 + 1], hexN[3*32 + 1];
    for (int k = 0; k < 32; k++)
        snprintf(hex0 + 3*k, 4, "%02x ", (unsigned char) buf[k]);
    for (int k = 0; k < 32; k++)
        snprintf(hexN + 3*k, 4, "%02x ",
                 (unsigned char) buf[RDMAMIG_BLOCK_SIZE_BYTES - 32 + k]);
    serverLog(LL_NOTICE,
        "RDMA RESHARD-TRANSFER %s: slot=%d n_entries=%u first32=[%s] last32=[%s]",
        tag, slot, n_entries, hex0, hexN);
}

/* Iterate the donor-staged blocks for a single slot from the recipient's
 * slot-keyed allocator, decode the flat-format entries, and dbAdd new keys
 * into the keyspace. Returns the number of keys added. If out_clobbered is
 * non-NULL, also reports how many staged entries were discarded because the
 * key already lived in the dict (don't-clobber rule: post-FLIP client writes
 * are fresher than the staged TRANSFER value). */
static int rdmaBackpatchSlotWithStats(redisDb *db, int slot, int *out_clobbered) {
    int n_blocks = 0;
    char **block_buffers = r_allocator_get_block_buffers_for_slot(slot, &n_blocks);
    if (block_buffers == NULL || n_blocks == 0) return 0;

    /* Pre-size the per-slot dict to 4x the total key count across all blocks
     * BEFORE doing any dbAdd. The 4x factor gives a load factor of ~25%,
     * making bucket collisions extremely rare — important now that we run
     * backpatch dbAdds concurrently with main-thread inserts WITHOUT a
     * per-slot lock. With sparse buckets, the chance that backpatch and a
     * main-thread insert touch the same bucket chain at the same time is
     * tiny. Combined with dbAdd's skip-if-present semantics, this is the
     * fast/lockless path. */
    unsigned long total_entries = 0;
    for (int b = 0; b < n_blocks; b++) {
        char *buf = block_buffers[b];
        if (buf == NULL) continue;
        uint32_t n;
        memcpy(&n, buf, sizeof(n));
        total_entries += n;
    }

    /* Pre-size + pause MUST happen under the per-slot write lock. dictExpand
     * mutates the dict's table pointers / rehashidx, and the main thread's
     * Path B lookups (lookupKey → dictFindLink → _dictRehashStepIfNeeded)
     * also mutate rehashidx — if they interleave with the expand, the bucket
     * chain corrupts (cycle → lookup walks forever; or dict.c:423 assert).
     * Once paused, _dictRehashStepIfNeeded is a no-op for both threads, so
     * the per-key dbAdd loop below stays race-free with just the per-key
     * lock. dictPauseRehashing/AutoResize are not undone until the loop ends. */
    clusterSlotLockWriteNoTopology(slot);
    cluster_slot_lock_held_by_thread++;
    if (total_entries > 0) {
        kvstoreDictExpand(db->keys, slot, total_entries * 4);
    }
    dict *slot_dict = kvstoreGetDict(db->keys, slot);
    if (slot_dict) {
        dictPauseRehashing(slot_dict);
        dictPauseAutoResize(slot_dict);
    }
    cluster_slot_lock_held_by_thread--;
    clusterSlotUnlockNoTopology(slot);

    int total_added = 0;
    int cross_slot_skipped = 0;
    for (int b = 0; b < n_blocks; b++) {
        char *buf = block_buffers[b];
        if (buf == NULL) continue;
        if (b == 0) rdmaDebugDumpSlotBytes("RCV", slot, buf);
        uint32_t n_entries;
        memcpy(&n_entries, buf, sizeof(n_entries));
        if (n_entries == 0) continue;

        size_t cursor = sizeof(uint32_t);
        size_t cap = RDMAMIG_BLOCK_SIZE_BYTES;
        for (uint32_t i = 0; i < n_entries; i++) {
            robj *key = NULL, *val = NULL;
            size_t consumed = rdmaDecodeEntry(buf + cursor, cap - cursor, &key, &val);
            if (consumed == 0) {
                serverLog(LL_WARNING,
                    "RDMA backpatch: malformed entry at slot=%d block=%d entry=%u offset=%zu",
                    slot, b, i, cursor);
                break;
            }
            cursor += consumed;
            /* Per-key write lock on the slot. Without this, our lookup+dbAdd
             * races with main-thread Path B reads/writes (which take the same
             * lock at db.c:295 / db.c:448), corrupting the dict bucket chain
             * and leaving lookupKeyImpl walking a cycle forever. The lock is
             * held only for one lookup+insert (~microseconds), so the main
             * event loop is never starved. cluster_slot_lock_held_by_thread
             * is set so nested Path B wraps in dbAddInternal/lookupKey skip
             * the (non-recursive) self-lock. */
            /* Lock the KEY's actual slot, not the backpatch `slot` param.
             * A staged block fetched for slot S can hold a key that hashes
             * to a different slot (donor-side block classification ≠ CRC16
             * slot). dbAdd/lookupKeyWrite always act on getKeySlot(key)'s
             * dict, so the lock must be on that same slot or we'd mutate an
             * unlocked dict and race the main thread → corrupted bucket chain.
             * Use keyHashSlot (pure CRC16) rather than getKeySlot — the
             * latter reads server.current_client, which is unsafe off the
             * main thread. */
            int kslot = keyHashSlot(key->ptr, (int)sdslen(key->ptr));
            if (kslot != slot) {
                /* This staged entry doesn't hash to the slot being migrated.
                 * Adding it would touch a *different* slot's dict (kslot) that
                 * this backpatch neither pre-sized nor paused. Worse, if kslot
                 * is a stable (already-owned) slot, main-thread accessors skip
                 * the per-slot lock entirely (clusterSlotIsImporting is false),
                 * so our insert races them lock-free and corrupts the dict —
                 * the intermittent dict.c:423 crash. Skip it: if kslot is also
                 * migrating, its own block carries the key; otherwise it isn't
                 * part of this migration. */
                cross_slot_skipped++;
                decrRefCount(val);
                decrRefCount(key);
                continue;
            }
            clusterSlotLockWriteNoTopology(kslot);
            cluster_slot_lock_held_by_thread++;
            if (lookupKeyWrite(db, key) == NULL) {
                /* dbAdd takes the value by ref and consumes it. */
                dbAdd(db, key, &val);
                total_added++;
            } else {
                /* DON'T-CLOBBER RULE: key already exists in live dict — a
                 * post-FLIP client write was fresher than this staged value.
                 * Drop the staged value; the live one wins. */
                if (out_clobbered) (*out_clobbered)++;
                decrRefCount(val);
            }
            cluster_slot_lock_held_by_thread--;
            clusterSlotUnlockNoTopology(kslot);
            decrRefCount(key);
        }
    }
    if (slot_dict) {
        dictResumeRehashing(slot_dict);
        dictResumeAutoResize(slot_dict);
    }
    if (cross_slot_skipped > 0) {
        serverLog(LL_WARNING,
            "RDMA backpatch: slot=%d skipped %d staged entries that hash to "
            "other slots (donor block misclassification)", slot, cross_slot_skipped);
    }
    zfree(block_buffers);
    return total_added;
}

/* ====================================================================== *
 *  RDMA DONE-SLOTS  (recipient side, chunked main-thread backpatch)
 *
 *  Reverted from pthread workers because the kvstore isn't thread-safe at
 *  the dict-resize / bucket level vs main-thread READ paths.
 *
 *  Two attempts were made to thread this on branch aqueduct-thread-migration:
 *
 *    Attempt 1: worker started in initServerConfig — silently lost because
 *      that runs BEFORE daemonize() forks. Pthreads don't survive fork.
 *    Attempt 2: worker started in initServer (post-fork) with a
 *      `recipient_backpatch_mu` that processCommand acquires for commands
 *      hitting importing slots. The worker reached slot ~1120 on the first
 *      DONE-SLOTS batch, then segfaulted at address 0x48 (NULL field
 *      access) — a race against one of the many non-`processCommand`
 *      main-thread paths that also touch `db->keys` / `migrating_slots_to`
 *      / `importing_slots_from`: clusterCron, the RDMA RESHARD-RECV-FLIP
 *      RPC handler, AOF, expiration, replication. The mutex only covered
 *      the command-dispatch path.
 *
 *  Properly threading the backpatch requires either (a) a broad audit + lock
 *  on every keyspace mutation path, or (b) a kvstore redesign with per-
 *  slot mutexes. Both are out of scope for this PR. Sticking with the
 *  chunked-tick approach in the meantime.
 *
 *  Concretely:
 *    rdmaDoneSlotsCommand: parse slots → push (db, slot_list) onto a
 *      pending list → reply +OK immediately. If no timer is running, arm
 *      a 1 ms time event that calls migrationBackpatchTick.
 *    migrationBackpatchTick: take the head pending item, call rdmaBackpatchSlot
 *      for the next SLOTS_PER_TICK slots, advance the per-item index.
 *      When the item is exhausted, free it and try the next. When the list
 *      is empty, return AE_NOMORE to drop the timer.
 * ====================================================================== */

typedef struct pendingBackpatch {
    redisDb *db;
    int *slots;
    int n_slots;
    int idx;                       /* next slot index to process */
    long long applied;             /* running total added */
    long long clobber_skipped;     /* don't-clobber: staged keys discarded
                                    * because the live dict already had them
                                    * (post-FLIP client write was fresher). */
} pendingBackpatch;

#define MIGRATION_SLOTS_PER_TICK 8     /* small enough to yield often,
                                         large enough to make progress */
#define MIGRATION_TICK_DELAY_MS 1      /* 1 ms reschedule; ample for the
                                         event loop to drain client work */

static list *pending_applies = NULL;
static long long migration_backpatch_timer_id = -1;

static int migrationBackpatchTick(struct aeEventLoop *el, long long id, void *clientData) {
    UNUSED(el); UNUSED(id); UNUSED(clientData);

    if (pending_applies == NULL || listLength(pending_applies) == 0) {
        migration_backpatch_timer_id = -1;
        return AE_NOMORE;
    }

    listNode *ln = listFirst(pending_applies);
    pendingBackpatch *p = listNodeValue(ln);

    int processed = 0;
    while (processed < MIGRATION_SLOTS_PER_TICK && p->idx < p->n_slots) {
        int clobbered = 0;
        p->applied += rdmaBackpatchSlotWithStats(p->db, p->slots[p->idx],
                                                 &clobbered);
        p->clobber_skipped += clobbered;
        p->idx++;
        processed++;
    }

    if (p->idx >= p->n_slots) {
        /* NOTE: importing_slots_from[] cleanup reverted — see donor
         * MIGRATE-DONE site for rationale. */
        serverLog(LL_NOTICE,
            "RDMA DONE-SLOTS (chunked): finished %d slots, applied %lld keys total, "
            "clobber_skipped=%lld",
            p->n_slots, p->applied, p->clobber_skipped);
        zfree(p->slots);
        zfree(p);
        listDelNode(pending_applies, ln);
    }

    return MIGRATION_TICK_DELAY_MS;
}

/* ====================================================================== *
 *  Phase 4d types + statics (definitions used by rdmaDoneSlotsCommand
 *  below and the backpatch-thread / status code further down).               *
 * ====================================================================== */

#include <semaphore.h>
#include <fcntl.h>
#include <unistd.h>

/* Same value bio.c uses for its worker threads. */
#define BACKPATCH_THREAD_STACK_SIZE (1024*1024*4)

/* Phase 4d callback-RPC variant: no shared globals needed on the recipient
 * for the deferred REGISTER-BLOCK-SLOTS path — each worker thread owns its
 * own job struct end-to-end (parse → ibv_reg_mr → callback hiredis to source
 * → free) without main-thread coordination after enqueue. */

/* dictType for backpatch_batches_by_key: sds key, backpatchBatch* value (free
 * the key sds, leave the value to the dispose path). */
extern dictType sdsHashDictType;

typedef enum {
    BACKPATCH_QUEUED   = 0,
    BACKPATCH_RUNNING  = 1,
    BACKPATCH_DONE     = 2,
    BACKPATCH_FAILED   = 3
} backpatchBatchState;

typedef struct backpatchBatch {
    redisDb *db;
    int     *slots;
    int      n_slots;
    char     src_node_id[CLUSTER_NAMELEN + 1];
    long long src_mig_id;
    _Atomic int          idx;        /* Slots completed so far (pool path); was in-order
                                        position in the single-thread path. */
    _Atomic long long    applied;
    _Atomic long long    clobber_skipped;  /* don't-clobber: staged keys discarded
                                             * because the live dict already had them
                                             * (a post-FLIP client write was fresher).
                                             * Aggregated from per-merge-work tickers
                                             * and from the legacy dbAdd path. */
    _Atomic int          state;
    _Atomic int          remaining;  /* Slots still to complete. Init = n_slots; the
                                        pool worker that decrements to 0 transitions
                                        the batch to BACKPATCH_DONE and disposes. */
    sds                  err;
    pthread_mutex_t      err_mu;
    time_t               t_started;
    time_t               t_ended;
    /* Phase B.5: chain replication coordination. chain_forwarded=1 once we've
     * issued rdmaLeaderChainForward for this batch's data; gates MGN_INDX_UPD
     * on chain-majority CHAIN-ACK arriving. */
    int                  chain_forwarded;
    int                  chain_acked;
    long long            chain_baseline_ack_count; /* ack_count snapshot when forward was issued */
    /* AqRaft 3-flag DONE invariant: BACKPATCH-STATUS reports "done" only when
     * ALL THREE are true — leader merge complete (merge_done), chain replicated
     * to majority (chain_acked), and MGN_INDX_UPD raft log committed
     * (indx_applied). Otherwise the donor leader could release its slots while
     * the recipient followers haven't received the migrated bytes yet → a
     * recipient-leader crash would silently lose data. */
    _Atomic int          merge_done;
    _Atomic int          indx_applied;
    /* AqRaft parallel-chain: incremented by each backpatch pool worker after
     * it captures its slot's chain snapshot (before shadow-merge would
     * overwrite the landing block's segment headers). When this reaches
     * n_slots all snapshots are safe to chain-forward, so we spawn
     * chainForwardWorker IN PARALLEL with the remaining shadow build + merge
     * instead of waiting for mergeBackpatchTick. chain_spawn_initiated is a
     * CAS gate so we spawn exactly once across the racing pool-worker and
     * mergeBackpatchTick-fallback paths. */
    _Atomic int          snapshots_captured;
    _Atomic int          chain_spawn_initiated;
    /* Phase C: slots covered by this batch's DONE-SLOTS-CHUNK calls. The
     * leader iterates these at BACKPATCH_DONE to encode kvstore content
     * for chain forwarding. Allocated in DONE-SLOTS-INIT (size = n_slots
     * = total_slots), appended in DONE-SLOTS-CHUNK; ordering matches the
     * order chunks were delivered. */
    int                 *covered_slots;
    int                  covered_slot_count;
    pthread_mutex_t      covered_mu;
    /* Pass-through chain snapshot: r_allocator's per-slot blocks get
     * corrupted by rdmaBackpatchSlotFillShadow (kvobj segments are
     * written on top of the donor's encoded data). To preserve the
     * donor's raw 2 MiB blocks for chain forwarding, we snapshot each
     * slot's block[0] into this pool BEFORE shadow-merge runs. Layout:
     * slot at covered_slots[i] sits at offset i * RDMAMIG_BLOCK_SIZE_BYTES.
     * Allocated in DONE-SLOTS-INIT / legacy DONE-SLOTS (sized to
     * n_slots * RDMAMIG_BLOCK_SIZE_BYTES), freed in backpatch dispose. */
    char                *donor_snapshot_pool;
    size_t               donor_snapshot_pool_bytes;
    /* AqRaft pool-free: the donor's landing pool (mmap base + size) for this
     * session, copied from the cached connection at batch creation. Reclaimed
     * via madvise(MADV_DONTNEED) in backpatchFinalize, after the migrated
     * kvobjs have been copied into the recipient's own managed blocks. */
    void                *landing_pool_base;
    size_t               landing_pool_bytes;
    struct rdmamig_buffer *landing_pool_buf;   /* for dereg+madvise at finalize */
    /* AqRaft lever #4 fix: the landing pool must NOT be reclaimed until the
     * main-thread merge has fully drained — mergeBackpatchTick reads each
     * migrated value (v = src->ptr) directly out of the landing pool, so a
     * dereg+madvise(DONTNEED) while merges are still queued drops the pages
     * under the main thread → SIGSEGV in r_allocator_insert_kvobj. The
     * snapshot-capture-triggered chain spawn can reach backpatchFinalize
     * BEFORE merge_done, so the release is now driven by whichever of
     * {mergeBackpatchTick @ merge_done, backpatchFinalize} runs last. This CAS
     * guard ensures exactly one of them performs the release. */
    _Atomic int          landing_pool_released;
} backpatchBatch;

#define BACKPATCH_RING_CAPACITY 64u

static backpatchBatch *backpatch_ring[BACKPATCH_RING_CAPACITY];
static _Atomic uint64_t backpatch_ring_head = 0;
static _Atomic uint64_t backpatch_ring_tail = 0;
static sem_t backpatch_wake;
static pthread_t backpatch_thread_tid;
static _Atomic int backpatch_thread_shutdown = 0;
static int backpatch_thread_started = 0;

static int backpatch_dispose_pipe[2] = {-1, -1};
static pthread_mutex_t backpatch_dispose_mu = PTHREAD_MUTEX_INITIALIZER;
static list *backpatch_dispose_list = NULL;

/* Phase B.5: batches in BACKPATCH_DONE whose MGN_INDX_UPD log + dispose are
 * deferred pending CHAIN-ACK from the chain tail. chainPendingTick polls
 * rdmaLeaderChainAckCount; when it crosses the batch's baseline, we log
 * MGN_INDX_UPD + RECP_TXN_DONE and dispose. */
static pthread_mutex_t backpatch_chain_pending_mu = PTHREAD_MUTEX_INITIALIZER;
static list *backpatch_chain_pending = NULL;
static long long chain_pending_timer_id = -1;
#define CHAIN_PENDING_TICK_MS 25
#define CHAIN_PENDING_TIMEOUT_MS 5000   /* fire MGN_INDX_UPD anyway after timeout */

/* Aqueduct: per-slot work threadpool. The dispatcher (the single consumer of
 * the SPSC backpatch_ring) fans a batch's slots out across N pool workers;
 * the worker that decrements backpatchBatch.remaining to 0 transitions the
 * batch to DONE and hands it to the dispose pipe.
 *
 * Pool size is server.rdma_backpatch_pool_size, read at thread-start time
 * and stored in backpatch_pool_size (config range is 1..BACKPATCH_POOL_MAX,
 * enforced by the config layer). */
#define BACKPATCH_POOL_MAX 32

typedef struct backpatchSlotWork {
    backpatchBatch *batch;   /* parent batch (back-pointer for completion accounting) */
    redisDb        *db;      /* cached from batch->db (avoids load in tight loop) */
    int             slot;
} backpatchSlotWork;

static list            *backpatch_work_queue = NULL;          /* list of backpatchSlotWork* */
static pthread_mutex_t  backpatch_work_mu    = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t   backpatch_work_cv    = PTHREAD_COND_INITIALIZER;

static pthread_t        backpatch_pool_tids[BACKPATCH_POOL_MAX];
static int              backpatch_pool_size  = 0;     /* Snapshot of config at start. */
static int              backpatch_pool_started = 0;

static dict *backpatch_batches_by_key = NULL;
static pthread_mutex_t backpatch_batches_mu = PTHREAD_MUTEX_INITIALIZER;

/* ====================================================================== *
 *  Double-buffer backpatch (option 3).
 *
 *  Pool worker fills a per-slot SHADOW dict (worker-private) instead of
 *  touching db->keys. When a slot's shadow is full, the worker enqueues a
 *  merge job; the main thread's mergeBackpatchTick drains the queue
 *  MERGE_KEYS_PER_TICK keys per tick, moving entries from shadow into the
 *  live slot dict (or freeing them if live already has the key — live wins
 *  because that's a client write that landed during the migration window).
 *  A slot is counted as applied (batch->idx, ->applied, ->remaining) at
 *  MERGE completion, not at shadow-fill completion, so BACKPATCH-STATUS
 *  reports DONE only after the live dict has every staged key.
 *
 *  Concurrency invariants:
 *    - Worker writes only to its slot's shadow. No live access. No locks
 *      on live, no rehash pause, no cross-slot guard needed.
 *    - Main thread owns live entirely (client traffic + merge). Single
 *      writer to live → no dict races.
 *    - The shadow handoff is via backpatch_merge_queue under
 *      backpatch_merge_mu; the worker writes one byte to the existing
 *      backpatch_dispose_pipe to wake the main thread, which arms the
 *      merge timer if not already armed.
 * ====================================================================== */

typedef struct backpatchMergeWork {
    backpatchBatch *batch;     /* parent batch */
    redisDb        *db;        /* cached from batch->db */
    int             slot;
    dict           *shadow;    /* worker-filled, main-thread drains */
    dictIterator   *iter;      /* persistent across ticks; NULL until first tick */
    int             total;     /* shadow size at handoff (stats) */
    int             moved;     /* entries transferred into live so far */
    int             skipped_existing; /* don't-clobber: key already in live dict;
                                       * the staged value was discarded because a
                                       * post-FLIP client write is fresher. Aggregated
                                       * into batch->clobber_skipped at BACKPATCH_DONE. */
} backpatchMergeWork;

#define MERGE_KEYS_PER_TICK 512
#define MERGE_TICK_DELAY_MS 1

static list           *backpatch_merge_queue   = NULL; /* list of backpatchMergeWork* */
static pthread_mutex_t backpatch_merge_mu      = PTHREAD_MUTEX_INITIALIZER;
static long long       backpatch_merge_timer_id = -1;
extern dictType        dbDictType;             /* server.c — shadow uses same type as live */

static sds backpatchBatchKey(const char *src_node_id, long long src_mig_id) {
    return sdscatfmt(sdsempty(), "%s:%I", src_node_id, src_mig_id);
}

/* Phase 4d format: RDMA DONE-SLOTS <src_node_id> <src_mig_id> <s1> ... <sN>
 * Older format (no src_node_id/src_mig_id, just slots) is detected by trying
 * to interpret argv[2] as a slot number: if it parses as 0..CLUSTER_SLOTS-1
 * AND argv[3] (if present) ALSO parses similarly, we treat the entire tail
 * as slots (legacy). Otherwise we expect the new format. The legacy path
 * still enqueues onto the SPSC ring, just without tracking metadata. */
void rdmaDoneSlotsCommand(client *c) {
    if (c->argc < 3) {
        addReplyError(c, "DONE-SLOTS: at least one slot required");
        return;
    }

    /* Try to parse the new format: argv[2] = src_node_id (40 chars),
     * argv[3] = src_mig_id, argv[4..] = slots. */
    const char *src_node_id = "";
    long long src_mig_id = 0;
    int slot_arg_start = 2;
    if (c->argc >= 5) {
        sds maybe_id = c->argv[2]->ptr;
        long long maybe_mig;
        if (sdslen(maybe_id) == CLUSTER_NAMELEN &&
            getLongLongFromObject(c->argv[3], &maybe_mig) == C_OK)
        {
            src_node_id = maybe_id;
            src_mig_id = maybe_mig;
            slot_arg_start = 4;
        }
    }
    int n_slots = c->argc - slot_arg_start;
    if (n_slots <= 0) {
        addReplyError(c, "DONE-SLOTS: no slot ids");
        return;
    }

    /* Allocate the backpatchBatch up front; ownership transfers to the
     * backpatch thread on successful enqueue. */
    backpatchBatch *b = zmalloc(sizeof(*b));
    b->db = c->db;
    b->n_slots = n_slots;
    b->slots = zmalloc((size_t) n_slots * sizeof(int));
    memcpy(b->src_node_id, src_node_id, strlen(src_node_id));
    b->src_node_id[strlen(src_node_id)] = '\0';
    b->src_mig_id = src_mig_id;
    atomic_store(&b->idx, 0);
    atomic_store(&b->applied, 0);
    atomic_store(&b->clobber_skipped, 0);
    atomic_store(&b->state, BACKPATCH_QUEUED);
    atomic_store(&b->remaining, n_slots);
    b->err = NULL;
    pthread_mutex_init(&b->err_mu, NULL);
    /* AqRaft pool-free: capture this donor's landing-pool region (set at
     * REGISTER-BLOCK-SLOTS) so backpatchFinalize can reclaim it. */
    {
        rdmaCachedConnection *cc = rdmaGetConnection(c);
        b->landing_pool_base  = cc ? cc->landing_pool_base  : NULL;
        b->landing_pool_bytes = cc ? cc->landing_pool_bytes : 0;
        b->landing_pool_buf   = cc ? cc->aqueduct_pool_buf  : NULL;
    }
    b->t_started = time(NULL);
    b->t_ended = 0;
    for (int j = 0; j < n_slots; j++) {
        long long s;
        if (getLongLongFromObject(c->argv[slot_arg_start + j], &s) != C_OK ||
            s < 0 || s >= CLUSTER_SLOTS) {
            zfree(b->slots);
            pthread_mutex_destroy(&b->err_mu);
            zfree(b);
            addReplyError(c, "slot id out of range");
            return;
        }
        b->slots[j] = (int) s;
    }
    /* Phase C: legacy DONE-SLOTS knows all slots up front; mirror them
     * into covered_slots so the chain-encode path sees the same list. */
    b->chain_forwarded = 0;
    b->chain_acked = 0;
    b->chain_baseline_ack_count = 0;
    /* AqRaft 3-flag DONE + parallel chain spawn — see backpatchBatch struct. */
    atomic_store_explicit(&b->merge_done, 0, memory_order_relaxed);
    atomic_store_explicit(&b->indx_applied, 0, memory_order_relaxed);
    atomic_store_explicit(&b->snapshots_captured, 0, memory_order_relaxed);
    atomic_store_explicit(&b->chain_spawn_initiated, 0, memory_order_relaxed);
    atomic_store_explicit(&b->landing_pool_released, 0, memory_order_relaxed);
    b->covered_slots = zmalloc((size_t) n_slots * sizeof(int));
    memcpy(b->covered_slots, b->slots, (size_t) n_slots * sizeof(int));
    b->covered_slot_count = n_slots;
    pthread_mutex_init(&b->covered_mu, NULL);
    /* Pass-through chain snapshot pool: sized to n_slots * 2 MiB. Only
     * allocated when a chain is configured; otherwise stays NULL and the
     * shadow-merge snapshot step is a no-op. */
    if (server.rdma_chain_followers != NULL &&
        sdslen(server.rdma_chain_followers) > 0) {
        b->donor_snapshot_pool_bytes = (size_t) n_slots * RDMAMIG_BLOCK_SIZE_BYTES;
        b->donor_snapshot_pool = zmalloc(b->donor_snapshot_pool_bytes);
        if (b->donor_snapshot_pool == NULL) {
            serverLog(LL_WARNING,
                "DONE-SLOTS: donor_snapshot_pool zmalloc(%zu) failed — "
                "chain forwarding will use stale/empty data",
                b->donor_snapshot_pool_bytes);
            b->donor_snapshot_pool_bytes = 0;
        }
        /* AqRaft Patch 16: do NOT memset the 2.86 GB pool here. memset on
         * a freshly-allocated pool of this size triggers ~700K page faults
         * (kernel lazy allocation), which blocks the main thread for ~1-2
         * sec → raft check-quorum trips (election_timeout * 2 = 2 s) and
         * the leader steps down. The chain-forwarded RPC carries an
         * explicit slot list (covered_slots), and F1 only walks that list
         * via rdmaApplySlotBlock — uncovered offsets in the pool are
         * never read on the follower side. Per-slot memcpys in the
         * backpatch worker thread (off main thread) populate the covered
         * offsets as snapshots arrive. */
    } else {
        b->donor_snapshot_pool = NULL;
        b->donor_snapshot_pool_bytes = 0;
    }

    /* Index by (src_node_id, src_mig_id) for BACKPATCH-STATUS lookups. The dict
     * stores the batch under the key sds; we drop+re-add if a previous
     * batch with the same key is still hanging around (rare; admin retry). */
    if (backpatch_batches_by_key != NULL && strlen(src_node_id) == CLUSTER_NAMELEN) {
        sds key = backpatchBatchKey(src_node_id, src_mig_id);
        pthread_mutex_lock(&backpatch_batches_mu);
        dictReplace(backpatch_batches_by_key, key, b);
        pthread_mutex_unlock(&backpatch_batches_mu);
    }

    /* AqRaft chain: when rdma-chain-followers is set, kick off chain
     * establishment to those followers. Mirrors the DONE-SLOTS-INIT hook —
     * legacy DONE-SLOTS path needs it too since not every donor sends
     * DONE-SLOTS-INIT (depends on cluster-rdma-transfer-overlap on the donor).
     * Pool sized to n_slots * 2 MiB so the leader can pass-through each
     * raw block plus per-block padding to the follower's landing pool. */
    if (server.rdma_chain_followers != NULL &&
        sdslen(server.rdma_chain_followers) > 0) {
        long long pool_bytes = (long long) n_slots * RDMAMIG_BLOCK_SIZE_BYTES;
        rdmaChainSpawnEstablish(src_mig_id, pool_bytes,
                                server.rdma_chain_followers);
    }

    /* Try to enqueue on the SPSC ring. Safe to run on a dedicated backpatch
     * thread now that databasesCron is paused for the migration window
     * (see server.recipient_backpatch_in_progress in databasesCron). Main-
     * thread reads on importing slots still serialize against the backpatch
     * thread's dbAdd via the per-slot rwlock (Path B narrow wraps in db.c). */
    int enqueued = 0;
    if (backpatch_thread_started) {
        uint64_t head = atomic_load_explicit(&backpatch_ring_head, memory_order_relaxed);
        uint64_t tail = atomic_load_explicit(&backpatch_ring_tail, memory_order_acquire);
        if (head - tail < BACKPATCH_RING_CAPACITY) {
            backpatch_ring[head & (BACKPATCH_RING_CAPACITY - 1)] = b;
            atomic_store_explicit(&backpatch_ring_head, head + 1, memory_order_release);
            sem_post(&backpatch_wake);
            enqueued = 1;
        }
    }

    if (!enqueued) {
        /* Ring full or thread not running — fall back to the chunked main-
         * thread tick path. Convert backpatchBatch into pendingBackpatch and let
         * migrationBackpatchTick handle it. */
        pendingBackpatch *p = zmalloc(sizeof(*p));
        p->db = c->db;
        p->n_slots = n_slots;
        p->slots = zmalloc((size_t) n_slots * sizeof(int));
        memcpy(p->slots, b->slots, (size_t) n_slots * sizeof(int));
        p->idx = 0;
        p->applied = 0;
        if (pending_applies == NULL) pending_applies = listCreate();
        listAddNodeTail(pending_applies, p);
        if (migration_backpatch_timer_id == -1) {
            migration_backpatch_timer_id = aeCreateTimeEvent(server.el,
                MIGRATION_TICK_DELAY_MS, migrationBackpatchTick, NULL, NULL);
        }
        /* The backpatchBatch we allocated for tracking is left in the index
         * dict; mark it as BACKPATCH_DONE inline so BACKPATCH-STATUS reports it
         * correctly once the chunked tick finishes the work. Note: this
         * inline state transition is unsynchronized with the actual
         * chunked backpatch progress — it's a coarse "queued / done" view in
         * the fallback path. The fallback is taken only on overflow, so
         * this is best-effort. */
        atomic_store(&b->state, BACKPATCH_DONE);
        atomic_store(&b->remaining, 0);  /* No pool worker will touch this batch. */
        b->t_ended = time(NULL);
        /* Re-enable databasesCron — backpatch is done. Paired with the incr in
         * rdmaRegisterBlockSlotsCommand (moved out of RECV-FLIP for early-FLIP). */
        atomicDecr(server.recipient_backpatch_in_progress, 1);
        /* Re-enable + rebuild the Fenwick tree we let go stale during backpatch. */
        kvstoreSetDeferFenwickUpdates(server.db[0].keys, 0);
        kvstoreFenwickRebuild(server.db[0].keys);
        /* Note: recipient's slot meta is already STABLE (set in RECV-FLIP).
         * The MIGRATING→STABLE transition here was tried but caused
         * concurrent-mutation issues with main-thread Path B; reverted. */
        serverLog(LL_NOTICE,
            "RDMA DONE-SLOTS (chunked fallback): queued %d slots; %lu in-flight",
            n_slots, listLength(pending_applies));
    } else {
        serverLog(LL_NOTICE,
            "RDMA DONE-SLOTS (backpatch-thread): enqueued %d slots from %.*s mig_id=%lld",
            n_slots, CLUSTER_NAMELEN, src_node_id, src_mig_id);
    }
    addReply(c, shared.ok);
}

/* Aqueduct TRANSFER/BACKPATCH overlap path.
 *
 * RDMA DONE-SLOTS-INIT <src_node_id> <src_mig_id> <total_slots>
 *
 * Pre-allocate a backpatchBatch sized to total_slots so per-chunk DONE-SLOTS-CHUNK
 * RPCs can append per-slot work items onto the pool work queue without needing
 * to know the final batch size up front. State starts as BACKPATCH_RUNNING so
 * the donor's BACKPATCH-STATUS poll sees "running" immediately after INIT.
 * The pool worker that decrements remaining → 0 (cluster_rdma.c around :1258)
 * transitions to BACKPATCH_DONE and runs the existing dispose path. */
void rdmaDoneSlotsInitCommand(client *c) {
    if (c->argc != 5) {
        addReplyError(c, "DONE-SLOTS-INIT: expected <src_node_id> <src_mig_id> <total_slots>");
        return;
    }
    sds src_node_id = c->argv[2]->ptr;
    if (sdslen(src_node_id) != CLUSTER_NAMELEN) {
        addReplyError(c, "src_node_id must be 40 chars");
        return;
    }
    long long src_mig_id, total_slots_ll;
    if (getLongLongFromObject(c->argv[3], &src_mig_id) != C_OK) {
        addReplyError(c, "src_mig_id must be an integer");
        return;
    }
    if (getLongLongFromObject(c->argv[4], &total_slots_ll) != C_OK ||
        total_slots_ll <= 0 || total_slots_ll > CLUSTER_SLOTS) {
        addReplyError(c, "total_slots out of range");
        return;
    }
    int total_slots = (int) total_slots_ll;

    backpatchBatch *b = zmalloc(sizeof(*b));
    b->db = c->db;
    b->n_slots = total_slots;
    b->slots = NULL;   /* No eager slot list — CHUNK RPCs deliver work items directly. */
    memcpy(b->src_node_id, src_node_id, CLUSTER_NAMELEN);
    b->src_node_id[CLUSTER_NAMELEN] = '\0';
    b->src_mig_id = src_mig_id;
    atomic_store(&b->idx, 0);
    atomic_store(&b->applied, 0);
    atomic_store(&b->clobber_skipped, 0);
    atomic_store(&b->state, BACKPATCH_RUNNING);
    atomic_store(&b->remaining, total_slots);
    b->err = NULL;
    pthread_mutex_init(&b->err_mu, NULL);
    /* AqRaft pool-free: capture this donor's landing-pool region for reclaim. */
    {
        rdmaCachedConnection *cc = rdmaGetConnection(c);
        b->landing_pool_base  = cc ? cc->landing_pool_base  : NULL;
        b->landing_pool_bytes = cc ? cc->landing_pool_bytes : 0;
        b->landing_pool_buf   = cc ? cc->aqueduct_pool_buf  : NULL;
    }
    b->t_started = time(NULL);
    b->t_ended = 0;
    /* Phase C tracking. */
    b->chain_forwarded = 0;
    b->chain_acked = 0;
    b->chain_baseline_ack_count = 0;
    /* AqRaft 3-flag DONE + parallel chain spawn — see backpatchBatch struct. */
    atomic_store_explicit(&b->merge_done, 0, memory_order_relaxed);
    atomic_store_explicit(&b->indx_applied, 0, memory_order_relaxed);
    atomic_store_explicit(&b->snapshots_captured, 0, memory_order_relaxed);
    atomic_store_explicit(&b->chain_spawn_initiated, 0, memory_order_relaxed);
    atomic_store_explicit(&b->landing_pool_released, 0, memory_order_relaxed);
    b->covered_slots = zmalloc((size_t) total_slots * sizeof(int));
    b->covered_slot_count = 0;
    pthread_mutex_init(&b->covered_mu, NULL);
    /* Pass-through chain snapshot pool: sized to total_slots * 2 MiB.
     * Only allocated when a chain is configured. */
    if (server.rdma_chain_followers != NULL &&
        sdslen(server.rdma_chain_followers) > 0) {
        b->donor_snapshot_pool_bytes = (size_t) total_slots * RDMAMIG_BLOCK_SIZE_BYTES;
        b->donor_snapshot_pool = zmalloc(b->donor_snapshot_pool_bytes);
        if (b->donor_snapshot_pool == NULL) {
            serverLog(LL_WARNING,
                "DONE-SLOTS-INIT: donor_snapshot_pool zmalloc(%zu) failed",
                b->donor_snapshot_pool_bytes);
            b->donor_snapshot_pool_bytes = 0;
        }
        /* AqRaft Patch 16: see DONE-SLOTS path above — skip the 2.86 GB
         * memset to avoid blocking the main thread on ~700K page faults
         * (triggers raft check-quorum step-down). F1 reads only covered
         * slot offsets; uncovered offsets are never accessed. */
    } else {
        b->donor_snapshot_pool = NULL;
        b->donor_snapshot_pool_bytes = 0;
    }

    if (backpatch_batches_by_key != NULL) {
        sds key = backpatchBatchKey(src_node_id, src_mig_id);
        pthread_mutex_lock(&backpatch_batches_mu);
        dictReplace(backpatch_batches_by_key, key, b);
        pthread_mutex_unlock(&backpatch_batches_mu);
    } else {
        /* Backpatch infrastructure not initialized — caller is using the
         * legacy path or running outside cluster mode. Fail loudly. */
        zfree(b);
        addReplyError(c, "backpatch infrastructure not initialized");
        return;
    }

    serverLog(LL_NOTICE,
        "RDMA DONE-SLOTS-INIT: batch from %.*s mig_id=%lld total_slots=%d "
        "(TRANSFER/BACKPATCH overlap)",
        CLUSTER_NAMELEN, b->src_node_id, src_mig_id, total_slots);

    /* Pass-through chain: if rdma-chain-followers is configured, kick off
     * chain establishment to those followers in the background so the chain
     * landing pools + QPs are up by the time the leader's backpatch finishes
     * and wants to forward. Pool sized to total_slots * 2 MiB to fit all
     * per-slot blocks the leader will RDMA-WRITE through. Single detached
     * pthread per session; leaderEstablishChain is internally protected
     * against duplicate setup for the same src_mig_id. */
    if (server.rdma_chain_followers != NULL &&
        sdslen(server.rdma_chain_followers) > 0) {
        long long pool_bytes = (long long) total_slots * RDMAMIG_BLOCK_SIZE_BYTES;
        rdmaChainSpawnEstablish(src_mig_id, pool_bytes,
                                server.rdma_chain_followers);
    }
    addReply(c, shared.ok);
}

/* RDMA DONE-SLOTS-CHUNK <src_node_id> <src_mig_id> <chunk_seq> <s1> ... <sK>
 *
 * Append K per-slot work items onto backpatch_work_queue for the batch
 * created by a prior DONE-SLOTS-INIT. Pool workers pick them up immediately;
 * the worker that decrements remaining → 0 finalizes the batch (no change
 * from the existing single-batch flow). chunk_seq is currently advisory (for
 * logs/debugging); ordering is not enforced. */
void rdmaDoneSlotsChunkCommand(client *c) {
    if (c->argc < 6) {
        addReplyError(c, "DONE-SLOTS-CHUNK: expected <src_node_id> <src_mig_id> <chunk_seq> <slot> [<slot> ...]");
        return;
    }
    sds src_node_id = c->argv[2]->ptr;
    if (sdslen(src_node_id) != CLUSTER_NAMELEN) {
        addReplyError(c, "src_node_id must be 40 chars");
        return;
    }
    long long src_mig_id, chunk_seq;
    if (getLongLongFromObject(c->argv[3], &src_mig_id) != C_OK) {
        addReplyError(c, "src_mig_id must be an integer");
        return;
    }
    if (getLongLongFromObject(c->argv[4], &chunk_seq) != C_OK) {
        addReplyError(c, "chunk_seq must be an integer");
        return;
    }
    int n_slots = c->argc - 5;

    sds key = backpatchBatchKey(src_node_id, src_mig_id);
    pthread_mutex_lock(&backpatch_batches_mu);
    backpatchBatch *b = backpatch_batches_by_key
        ? dictFetchValue(backpatch_batches_by_key, key) : NULL;
    pthread_mutex_unlock(&backpatch_batches_mu);
    sdsfree(key);
    if (b == NULL) {
        addReplyError(c, "no such backpatch batch — DONE-SLOTS-INIT first");
        return;
    }

    /* Validate + build all work items before taking the queue lock, so we
     * hold the lock only long enough to splice the tail. */
    backpatchSlotWork **items = zmalloc((size_t) n_slots * sizeof(*items));
    for (int j = 0; j < n_slots; j++) {
        long long s;
        if (getLongLongFromObject(c->argv[5 + j], &s) != C_OK ||
            s < 0 || s >= CLUSTER_SLOTS) {
            for (int k = 0; k < j; k++) zfree(items[k]);
            zfree(items);
            addReplyError(c, "slot id out of range");
            return;
        }
        backpatchSlotWork *w = zmalloc(sizeof(*w));
        w->batch = b;
        w->db    = b->db;
        w->slot  = (int) s;
        items[j] = w;
    }

    /* Phase C: append this chunk's slots to covered_slots BEFORE enqueuing
     * the worker items. The pass-through chain snapshot in the backpatch
     * worker looks up the slot's index in covered_slots; if we enqueue
     * first, the worker can race and find the slot missing (snapshot
     * skipped → follower gets zero-filled blocks). */
    if (b->covered_slots != NULL) {
        pthread_mutex_lock(&b->covered_mu);
        for (int j = 0; j < n_slots; j++) {
            if (b->covered_slot_count < b->n_slots) {
                b->covered_slots[b->covered_slot_count++] = items[j]->slot;
            }
        }
        pthread_mutex_unlock(&b->covered_mu);
    }

    pthread_mutex_lock(&backpatch_work_mu);
    for (int j = 0; j < n_slots; j++) listAddNodeTail(backpatch_work_queue, items[j]);
    pthread_cond_broadcast(&backpatch_work_cv);
    pthread_mutex_unlock(&backpatch_work_mu);
    zfree(items);

    serverLog(LL_NOTICE,
        "RDMA DONE-SLOTS-CHUNK: from %.*s mig_id=%lld seq=%lld n_slots=%d",
        CLUSTER_NAMELEN, b->src_node_id, src_mig_id, chunk_seq, n_slots);
    addReply(c, shared.ok);
}

/* Legacy stubs kept for server.c init-time call; the new recipientBackpatch
 * thread API below replaces these in functionality. */
void recipientBackpatchWorkerStart(void)  {}
void recipientBackpatchWorkerStop(void)   {}
void recipientBackpatchMuLock(void)       {}
void recipientBackpatchMuUnlock(void)     {}

/* ====================================================================== *
 *  Phase 4d: Recipient backpatch thread + SPSC lock-free ring                 *
 *                                                                          *
 *  Replaces the main-thread chunked migrationBackpatchTick. Producer is the    *
 *  main event loop (rdmaDoneSlotsCommand) when a DONE-SLOTS RPC lands;     *
 *  consumer is a single dedicated pthread that walks each batch's slots    *
 *  under clusterSlotLockWrite(slot) and calls rdmaBackpatchSlot — the same     *
 *  backpatch primitive the chunked path uses, just on a different thread.     *
 *                                                                          *
 *  Concurrency invariants:                                                 *
 *    - Single producer (main thread) → single consumer (backpatch thread).     *
 *    - Ring slots are backpatchBatch* pointers; ownership transfers on enqueue.*
 *    - backpatch thread sets cluster_slot_lock_held_by_thread = 1 around the   *
 *      slot wrlock so Path B keyspace wraps in db.c skip their own lock    *
 *      (no same-thread recursive rwlock).                                  *
 *    - backpatchBatch is freed by the backpatch thread via a 1-byte pipe message   *
 *      to the main thread; main thread does the zfree to keep             *
 *      malloc/free pairs single-threaded (jemalloc arena friendliness).   *
 *    - On ring overflow the producer falls back to the existing chunked    *
 *      migrationBackpatchTick path so DONE-SLOTS never blocks.                 *
 * ====================================================================== */

/* Pool worker: pull per-slot work items from backpatch_work_queue and apply
 * them. The dispatcher (drainBackpatchRing) pushes one item per slot when a
 * batch lands on the SPSC ring. The worker that decrements b->remaining to 0
 * transitions the batch to BACKPATCH_DONE and pushes to the dispose pipe,
 * inheriting the bookkeeping that the single-thread dispatcher used to do. */
/* Worker-private: read the staged blocks for `slot`, allocate kvobjs into the
 * recipient's r_allocator (its per-slot mutex makes that thread-safe), and
 * insert each kvobj pointer into a fresh `shadow` dict. Returns the populated
 * shadow (caller transfers ownership to the merge queue). Touches *no* live
 * keyspace state, so no Path B locks, no rehash pause, no per-slot wrlock —
 * the shadow is private to this thread until it's enqueued for merge. */
/* Migration-shadow dictType (Patch 9): same as dbDictType but keyDestructor
 * is NULL so dictRelease on the shadow doesn't try to decrRefCount kvobjs
 * whose memory lives inside the donor's RDMA-WRITTEN block (zfree would
 * corrupt the migration block which is registered to r_allocator). Don't-
 * clobber kvobjs left in the shadow at merge end leak into the block; the
 * block itself is intentionally not freed for the session's lifetime
 * (matches the existing register_existing_block contract — see
 * allocator.h:73-75). */
static dictType migrationShadowDictType;
static int migrationShadowDictTypeInited = 0;

static void initMigrationShadowDictType(void) {
    if (migrationShadowDictTypeInited) return;
    migrationShadowDictType = dbDictType;
    migrationShadowDictType.keyDestructor = NULL;
    migrationShadowDictTypeInited = 1;
}

/* Patch 9: pass-through raw-block install on the recipient.
 *
 * The donor's TRANSFER step now ships its r_allocator block byte-for-byte
 * into the recipient's pre-registered block. Each used segment in the block
 * holds a self-contained kvobj (encoding=OBJ_ENCODING_R_ALLOCATOR,
 * data_offset has slot id + value sds in-segment offset). The recipient
 * walks segments, fixes kv->ptr from data_offset, and adds the kvobj to a
 * fresh `shadow` dict that the main thread later merges into the live
 * kvstore. No per-key sdsnewlen + r_allocator_insert_kvobj — the kvobjs
 * are reused in place inside the registered block. */
static dict *rdmaBackpatchSlotFillShadow(redisDb *db, int slot,
                                         int *out_total, int *out_skipped)
{
    UNUSED(db);
    if (out_total)   *out_total = 0;
    if (out_skipped) *out_skipped = 0;

    /* AqRaft fix (thread-safety): this runs OFF the main thread (chainApplyWorker
     * on a follower, backpatch pool workers on the leader). It reads and mutates
     * the slot's r_allocator state — block list (get_block_buffers), segment
     * headers (walk + sanitize_imported_block), and the freelist (reset) — which
     * the MAIN thread also mutates via r_allocator_insert_kv when it raft-applies
     * client SETs to the same migrating slot. Without a shared lock those
     * concurrent doubly-linked-list / freelist mutations corrupted the heap
     * (recursive SIGSEGV under heavy write load, reproduced at 400 YCSB threads).
     * Hold the slot's (recursive) mutex across the whole read-modify-reset so it
     * is mutually exclusive with insert_kv. reset_freelist_for_slot re-locks the
     * same mutex internally — safe because it is recursive. */
    r_allocator_lock_slot(slot);

    int n_blocks = 0;
    char **block_buffers = r_allocator_get_block_buffers_for_slot(slot, &n_blocks);
    if (block_buffers == NULL || n_blocks == 0) {
        if (block_buffers) zfree(block_buffers);
        r_allocator_unlock_slot(slot);
        return NULL;
    }

    initMigrationShadowDictType();
    dict *shadow = dictCreate(&migrationShadowDictType);

    applySlotCtx ctx = { db, slot, 0, 0, 0, shadow };
    for (int b = 0; b < n_blocks; b++) {
        char *buf = block_buffers[b];
        if (buf == NULL) continue;
        if (b == 0) rdmaDebugDumpSlotBytes("RCV-SHADOW", slot, buf);
        r_allocator_walk_used_segments(buf, applySlotCb, &ctx);
        /* AqRaft Patch 17: sanitize donor-shipped block AFTER the walker
         * has identified kvobj segments. Flips free-segment alloc-bits to 1
         * so a later coalesce-on-free for a migrated kvobj never tries to
         * merge with a "free" neighbor whose freelist pointers refer to
         * donor memory (would SIGSEGV in freelist_remove_segment). */
        r_allocator_sanitize_imported_block(buf);
    }

    /* Reset the per-slot freelist: init_bloc_layout said "whole block is one
     * free segment", but the donor's writes occupy that memory now. Future
     * client writes (post-FLIP) will allocate a fresh block. */
    r_allocator_reset_freelist_for_slot(slot);
    /* End of the slot's allocator critical section; the rest only reads local
     * state (ctx) and frees our private block_buffers copy. */
    r_allocator_unlock_slot(slot);

    /* AqRaft diagnostic: distinguish the two ways a slot can stage zero keys.
     * We reached here with n_blocks > 0 (donor blocks WERE registered for this
     * slot), so installed==0 means the segment walker rejected every used
     * segment as a non-kvobj — i.e. the recipient's r_allocator/registration
     * state was not clean for this migration (the suspected cause of the
     * applied=0 we see on the 2nd/3rd successive migration into one node).
     * Logs n_blocks + skipped so we can tell "saw segments, rejected all"
     * (skipped>0) from "walker found no used segments at all" (skipped==0). */
    if (n_blocks > 0 && ctx.installed == 0) {
        serverLog(LL_WARNING,
            "RDMA shadow-fill: slot=%d n_blocks=%d staged=0 skipped_invalid=%d "
            "— donor blocks present but NO valid kvobjs staged "
            "(allocator/registration state not reset for this migration?)",
            slot, n_blocks, ctx.skipped_invalid);
    }

    zfree(block_buffers);
    if (out_total)   *out_total = ctx.installed;
    if (out_skipped) *out_skipped = ctx.skipped_invalid;
    return shadow;
}

/* Main thread: arm the merge timer if there's queued work and no timer is
 * already running. Called from the dispose-pipe handler (worker writes the
 * pipe after enqueuing) and from mergeBackpatchTick itself when re-queueing
 * is needed. */
static int mergeBackpatchTick(struct aeEventLoop *el, long long id, void *clientData);
static int chainPendingTick(struct aeEventLoop *el, long long id, void *clientData);

/* Phase B.5: fire MGN_INDX_UPD + RECP_TXN_DONE for a finished batch and
 * push it onto the dispose list. Called both from the no-chain immediate
 * path and from chainPendingTick after chain-ack arrives. Caller must hold
 * NO mutex on b. */
/* AqRaft Patch 24: detached worker for the 2.86 GB zfree, so it doesn't
 * block the main thread when backpatchFinalize is called from
 * chainPendingTick. jemalloc's free of a 2.86 GB pool ~165 ms which
 * propagates into Jedis pool exhaustion on YCSB. */
static void *backpatchPoolFreeWorker(void *arg) {
    zfree(arg);
    return NULL;
}

/* AqRaft pool-free: off-main worker to reclaim the landing pool's resident
 * pages (ibv_dereg_mr unpins + madvise drops the pages). Kept off the event
 * loop because dereg+madvise on a multi-GB MR can take tens of ms. */
static void *landingPoolReleaseWorker(void *arg) {
    rdmamig_buffer_release_pages((struct rdmamig_buffer *) arg);
    return NULL;
}

/* AqRaft lever #4 fix: reclaim the batch's donor landing pool exactly once,
 * off-main, and ONLY after the main-thread merge has fully drained. Both
 * mergeBackpatchTick (when it sets merge_done) and backpatchFinalize (when it
 * observes merge_done already set) call this; the CAS on landing_pool_released
 * makes the dereg+madvise happen exactly once. Callers MUST have established
 * that merge_done==1 (no more mergeBackpatchTick reads of v=src->ptr into the
 * pool) before calling — otherwise the pages could be dropped under an
 * in-flight insert. */
static void backpatchReleaseLandingPool(backpatchBatch *b) {
    if (b->landing_pool_buf == NULL) return;
    int expected = 0;
    if (!atomic_compare_exchange_strong_explicit(
            &b->landing_pool_released, &expected, 1,
            memory_order_acq_rel, memory_order_relaxed)) {
        return; /* another thread already claimed the release */
    }
    struct rdmamig_buffer *lpb = b->landing_pool_buf;
    size_t lbytes = b->landing_pool_bytes;
    pthread_t ltid;
    if (pthread_create(&ltid, NULL, landingPoolReleaseWorker, lpb) == 0) {
        pthread_detach(ltid);
        serverLog(LL_NOTICE,
            "AqRaft pool-free: reclaiming landing pool (%zu bytes) off-main "
            "for sess=%lld (post merge-drain)", lbytes, b->src_mig_id);
    } else {
        rdmamig_buffer_release_pages(lpb); /* fallback: inline */
    }
}

static void backpatchFinalize(backpatchBatch *b) {
    /* AqRaft Patch 24: timing probe. backpatchFinalize is called from
     * chainPendingTick on the MAIN thread when chain-ack arrives. The 2.86
     * GB zfree might block the event loop long enough to exhaust YCSB
     * connection pool. Measure to confirm. */
    long long t0 = ustime();

    {
        char mgn_payload[160];
        snprintf(mgn_payload, sizeof(mgn_payload),
                 "sess=%lld n_slots=%d applied=%lld",
                 b->src_mig_id, b->n_slots,
                 (long long) atomic_load(&b->applied));
        rdmaMgnLogAsync("INDX_UPD", mgn_payload);
        /* AqRaft 3-flag DONE: this is condition (3) — MGN_INDX_UPD is now in
         * the recipient's local raft log (the async send returned). The
         * reply / commit completes within the typical raft round-trip
         * (~tens of ms on a 3-node group); BACKPATCH-STATUS will return
         * "done" the next time it's polled, since chain_acked (1) and
         * merge_done (2) are also set by the time we reach this finalize. */
        atomic_store_explicit(&b->indx_applied, 1, memory_order_release);
    }
    {
        char mgn_payload[160];
        snprintf(mgn_payload, sizeof(mgn_payload),
                 "sess=%lld applied=%lld clobber_skipped=%lld",
                 b->src_mig_id,
                 (long long) atomic_load(&b->applied),
                 (long long) atomic_load(&b->clobber_skipped));
        rdmaMgnLogAsync("RECP_TXN_DONE", mgn_payload);
    }
    long long t1 = ustime();
    size_t pool_size = b->donor_snapshot_pool_bytes;
    /* Free the donor snapshot pool — chain finalize is done, the bytes
     * have been RDMA-forwarded and applied on every follower. The batch
     * itself stays in backpatch_batches_by_key for BACKPATCH-STATUS
     * lookups (per existing dispose convention), but the 2.67 GiB pool
     * is no longer needed.
     *
     * AqRaft Patch 24: spawn a detached pthread for the zfree. Direct
     * zfree blocks the main thread ~165 ms (jemalloc munmaps 2.86 GB
     * across many arenas). Off-main means main thread keeps servicing
     * YCSB requests during the free. */
    if (b->donor_snapshot_pool != NULL) {
        void *pool = b->donor_snapshot_pool;
        b->donor_snapshot_pool = NULL;
        b->donor_snapshot_pool_bytes = 0;
        pthread_t tid;
        if (pthread_create(&tid, NULL, backpatchPoolFreeWorker, pool) == 0) {
            pthread_detach(tid);
        } else {
            zfree(pool); /* fallback: inline if pthread_create fails */
        }
    }
    /* AqRaft pool-free (lever #4 fix): reclaim this session's donor landing
     * pool — but ONLY if the main-thread merge has already drained
     * (merge_done==1). mergeBackpatchTick reads each migrated value straight
     * out of the landing pool (v = src->ptr), so freeing it while merges are
     * still queued drops the pages under the main thread → SIGSEGV in
     * r_allocator_insert_kvobj. backpatchFinalize can be reached BEFORE the
     * merge finishes (the chain forward is spawned on last-snapshot-captured,
     * not last-merge), so here we release only when merge_done is observed; if
     * it isn't set yet, the mergeBackpatchTick path that sets merge_done will
     * perform the release instead. The CAS in backpatchReleaseLandingPool makes
     * this exactly-once regardless of which side wins. */
    if (atomic_load_explicit(&b->merge_done, memory_order_acquire)) {
        backpatchReleaseLandingPool(b);
    }
    long long t2 = ustime();
    pthread_mutex_lock(&backpatch_dispose_mu);
    listAddNodeTail(backpatch_dispose_list, b);
    pthread_mutex_unlock(&backpatch_dispose_mu);
    char tick = 1;
    ssize_t wr = write(backpatch_dispose_pipe[1], &tick, 1);
    (void) wr;
    long long t3 = ustime();

    long long total_us = t3 - t0;
    if (total_us > 50000) {
        serverLog(LL_WARNING,
            "backpatchFinalize sess=%lld: total=%lld us "
            "(mgn_log=%lld us, zfree(%zu B)=%lld us, dispose=%lld us) — main-thread blocked",
            b->src_mig_id, total_us, t1 - t0, pool_size, t2 - t1, t3 - t2);
    }
}

/* AqRaft Patch 16(D): pthread-friendly wrapper for backpatchFinalize.
 * Casting a `void (*)(backpatchBatch*)` to `void *(*)(void *)` is undefined
 * behavior under strict C; this shim keeps the conversion well-defined. */
static void *backpatchFinalizeWorker(void *arg) {
    backpatchFinalize((backpatchBatch *) arg);
    return NULL;
}

/* AqRaft Patch 16(B/C/D): one-shot detached pthread that takes a batch from
 * BACKPATCH_DONE through chain-forward + finalize without touching the main
 * thread. Replaces the inline mergeBackpatchTick code path that previously
 * did kvstoreFenwickRebuild (~100 ms) + rdmaLeaderChainForwardPerSlot (2.86
 * GB memcpy + 1365 RDMA-WRITE WR posts, ~1+ sec). The raft check-quorum
 * mechanism trips at election_timeout * 2 = 2 s of zero AppendEntries acks,
 * so any sub-2-sec main-thread block could leave the leader without quorum
 * and force a step-down. Running this off-main keeps the event loop free
 * to send/receive heartbeats throughout the migration window. */
typedef struct chainForwardJob {
    backpatchBatch *batch;        /* not owned: lives in backpatch_batches_by_key */
    int            *slots_copy;   /* owned: zfree on exit */
    int             n_slots;
    int             chain_configured;
} chainForwardJob;

static void *chainForwardWorker(void *arg) {
    chainForwardJob *job = arg;
    backpatchBatch *b = job->batch;

    /* (C) Defer Fenwick rebuild: kvstoreFenwickRebuild takes kvs->shared_mu
     * so it's thread-safe vs main-thread reads. Run it off-main. */
    kvstoreSetDeferFenwickUpdates(server.db[0].keys, 0);
    kvstoreFenwickRebuild(server.db[0].keys);

    serverLog(LL_NOTICE,
        "RDMA backpatch-merge: batch DONE from %.*s mig_id=%lld "
        "n_slots=%d applied=%lld clobber_skipped=%lld [off-main]",
        CLUSTER_NAMELEN, b->src_node_id, b->src_mig_id, b->n_slots,
        (long long) atomic_load(&b->applied),
        (long long) atomic_load(&b->clobber_skipped));

    if (job->chain_configured) {
        /* (B) Chain forward off-main. rdmaLeaderChainForwardPerSlot is the
         * 2.86 GB memcpy + RDMA-WRITE WR posts that used to block main. */
        char errbuf[256] = {0};
        int frc = rdmaLeaderChainForwardPerSlot(b->src_mig_id,
                                                job->slots_copy, job->n_slots,
                                                b->donor_snapshot_pool,
                                                b->donor_snapshot_pool_bytes,
                                                errbuf, sizeof(errbuf));
        if (frc == C_OK) {
            b->chain_forwarded = 1;
            pthread_mutex_lock(&backpatch_chain_pending_mu);
            if (backpatch_chain_pending == NULL)
                backpatch_chain_pending = listCreate();
            listAddNodeTail(backpatch_chain_pending, b);
            pthread_mutex_unlock(&backpatch_chain_pending_mu);
            /* armChainPendingTimer() is main-thread-only (aeCreateTimeEvent
             * is not thread-safe). Poke the dispose pipe to wake main; the
             * dispose handler arms the timer. */
            char tick = 1;
            ssize_t wr = write(backpatch_dispose_pipe[1], &tick, 1);
            (void) wr;
            serverLog(LL_NOTICE,
                "CHAIN: sess=%lld batch BACKPATCH_DONE -> "
                "pass-through forwarded %d slots (%zu B), MGN_INDX_UPD deferred [off-main]",
                b->src_mig_id, job->n_slots,
                (size_t) job->n_slots * (size_t) RDMAMIG_BLOCK_SIZE_BYTES);
        } else {
            serverLog(LL_WARNING,
                "CHAIN: sess=%lld leader forward failed (%s) - "
                "firing MGN_INDX_UPD immediately as fallback",
                b->src_mig_id, errbuf);
            /* AqRaft 3-flag DONE: chain forward failed → there will be no
             * CHAIN-ACK. The migration falls back to "MGN_INDX_UPD raft
             * replication is the durability path" — which itself reaches
             * majority via the recipient's raft group. Set chain_acked=1 so
             * BACKPATCH-STATUS doesn't hang waiting for an ack that will
             * never come; the indx_applied flag (set inside backpatchFinalize)
             * remains the substantive durability proof. */
            b->chain_acked = 1;
            backpatchFinalize(b);  /* (D) finalize runs in this worker thread */
        }
    } else {
        /* No chain configured -> finalize directly (off-main). */
        /* AqRaft 3-flag DONE: with no chain configured, MGN_INDX_UPD raft
         * replication is the sole durability path. Set chain_acked=1 so the
         * BACKPATCH-STATUS handler doesn't gate on a chain that doesn't
         * exist. */
        b->chain_acked = 1;
        backpatchFinalize(b);
    }

    zfree(job->slots_copy);
    zfree(job);
    return NULL;
}

/* AqRaft: build a chainForwardJob for batch `b` and spawn chainForwardWorker
 * to do (Fenwick rebuild + chain-forward + finalize) off-main. Safe to call
 * from either the main thread or a backpatch pool worker — chainForwardWorker
 * is detached, takes ownership of the job, and internally hands the batch
 * off to chainPendingTick (chain-ack arrival) or backpatchFinalize (no-chain
 * / failed paths). Caller MUST hold the CAS on b->chain_spawn_initiated so
 * we spawn exactly once across the pool-worker (last-snapshot) and
 * mergeBackpatchTick (last-merge) racing paths. */
static void spawnChainForwardWorker(backpatchBatch *b) {
    int chain_configured = (server.rdma_chain_followers != NULL &&
                            sdslen(server.rdma_chain_followers) > 0);
    long long ack_count = chain_configured
        ? rdmaLeaderChainAckCount(b->src_mig_id) : -1;
    int chain_ready = (chain_configured && ack_count >= 0 &&
                       b->covered_slots != NULL &&
                       b->covered_slot_count > 0 &&
                       b->donor_snapshot_pool != NULL);

    chainForwardJob *job = zcalloc(sizeof(*job));
    job->batch = b;
    job->chain_configured = chain_ready;
    if (chain_ready) {
        b->chain_baseline_ack_count = ack_count;
        int n = b->covered_slot_count;
        job->slots_copy = zmalloc((size_t) n * sizeof(int));
        pthread_mutex_lock(&b->covered_mu);
        memcpy(job->slots_copy, b->covered_slots,
               (size_t) n * sizeof(int));
        pthread_mutex_unlock(&b->covered_mu);
        job->n_slots = n;
    }

    pthread_t tid;
    if (pthread_create(&tid, NULL, chainForwardWorker, job) != 0) {
        serverLog(LL_WARNING,
            "CHAIN: pthread_create(chainForwardWorker) failed for "
            "sess=%lld - falling back to inline finalize",
            b->src_mig_id);
        if (job->slots_copy) zfree(job->slots_copy);
        zfree(job);
        kvstoreSetDeferFenwickUpdates(server.db[0].keys, 0);
        kvstoreFenwickRebuild(server.db[0].keys);
        backpatchFinalize(b);
    } else {
        pthread_detach(tid);
    }
}

/* Main thread: poll the per-batch chain-ack state. For each pending batch:
 *   - if chain ack arrived (ack_count > baseline) → fire MGN_INDX_UPD and
 *     proceed with dispose;
 *   - if pending > CHAIN_PENDING_TIMEOUT_MS → log warning and finalize anyway
 *     so a stuck chain doesn't block protocol log entries forever. */
static int chainPendingTick(struct aeEventLoop *el, long long id, void *clientData) {
    UNUSED(el); UNUSED(id); UNUSED(clientData);
    pthread_mutex_lock(&backpatch_chain_pending_mu);
    if (backpatch_chain_pending == NULL ||
        listLength(backpatch_chain_pending) == 0) {
        pthread_mutex_unlock(&backpatch_chain_pending_mu);
        chain_pending_timer_id = -1;
        return AE_NOMORE;
    }
    /* Move ready batches to a local list under the lock; finalize outside. */
    list *ready = listCreate();
    long long now_ms = mstime();
    listIter li;
    listNode *ln;
    listRewind(backpatch_chain_pending, &li);
    while ((ln = listNext(&li)) != NULL) {
        backpatchBatch *b = listNodeValue(ln);
        long long count = rdmaLeaderChainAckCount(b->src_mig_id);
        int acked = (count > b->chain_baseline_ack_count);
        int timed_out = (now_ms - b->t_ended * 1000LL) > CHAIN_PENDING_TIMEOUT_MS;
        if (acked || timed_out) {
            if (timed_out && !acked) {
                serverLog(LL_WARNING,
                    "CHAIN: sess=%lld pending CHAIN-ACK timeout after %dms — "
                    "firing MGN_INDX_UPD anyway",
                    b->src_mig_id, CHAIN_PENDING_TIMEOUT_MS);
            } else {
                serverLog(LL_NOTICE,
                    "CHAIN: sess=%lld chain-ack observed (count %lld > base %lld) — "
                    "finalizing batch",
                    b->src_mig_id, count, b->chain_baseline_ack_count);
            }
            b->chain_acked = 1;
            listAddNodeTail(ready, b);
            listDelNode(backpatch_chain_pending, ln);
        }
    }
    int remaining = (int) listLength(backpatch_chain_pending);
    pthread_mutex_unlock(&backpatch_chain_pending_mu);

    /* AqRaft Patch 16(D): finalize off main. backpatchFinalize does a
     * zfree of the 2.86 GB donor_snapshot_pool which involves kernel
     * page-table teardown (tens of ms) plus rdmaMgnLogAsync calls. With
     * many batches ready in one tick, doing this inline can add up to a
     * noticeable main-thread stall. Spawn a tiny detached thread per
     * batch instead — main thread returns to the event loop immediately. */
    listRewind(ready, &li);
    while ((ln = listNext(&li)) != NULL) {
        backpatchBatch *b = (backpatchBatch *) listNodeValue(ln);
        pthread_t tid;
        if (pthread_create(&tid, NULL, backpatchFinalizeWorker, b) != 0) {
            /* Thread spawn failed — fall back to inline finalize so the
             * batch doesn't leak its 2.86 GB pool. */
            backpatchFinalize(b);
        } else {
            pthread_detach(tid);
        }
    }
    listRelease(ready);

    if (remaining == 0) {
        chain_pending_timer_id = -1;
        return AE_NOMORE;
    }
    return CHAIN_PENDING_TICK_MS;
}

static void armChainPendingTimer(void) {
    if (chain_pending_timer_id != -1) return;
    chain_pending_timer_id = aeCreateTimeEvent(server.el,
        CHAIN_PENDING_TICK_MS, chainPendingTick, NULL, NULL);
}
static void armMergeTimer(void) {
    if (backpatch_merge_timer_id != -1) return;
    pthread_mutex_lock(&backpatch_merge_mu);
    int empty = (backpatch_merge_queue == NULL || listLength(backpatch_merge_queue) == 0);
    pthread_mutex_unlock(&backpatch_merge_mu);
    if (empty) return;
    backpatch_merge_timer_id = aeCreateTimeEvent(server.el,
        MERGE_TICK_DELAY_MS, mergeBackpatchTick, NULL, NULL);
}

/* Main thread: drain the head merge work item up to MERGE_KEYS_PER_TICK
 * keys per tick. For each shadow entry, move the kvobj into the live slot
 * dict if absent (shadow wins); otherwise leave it in the shadow to be
 * freed at dictRelease (live wins — a client write landed during the
 * window). When the head item is exhausted, free its shadow, mark the slot
 * applied on the batch counter, and drop the item from the queue. */
static int mergeBackpatchTick(struct aeEventLoop *el, long long id, void *clientData) {
    UNUSED(el); UNUSED(id); UNUSED(clientData);

    /* AqRaft Patch 24: timing probe to measure how long each main-thread
     * tick takes. YCSB connection pool exhaustion correlates with finalize
     * — this tells us if individual ticks are blocking >10s (YCSB timeout)
     * or if cumulative tick churn is the cause. */
    long long t_enter = ustime();

    /* AqRaft Patch 26 probe: cumulative per-second accounting. Logs how many
     * ticks ran and how many microseconds of main-thread time they consumed
     * each wall-clock second, so we can confirm whether mergeBackpatchTick is
     * the consumer starving raft apply during the migration-window stall. */
    static long long mbt_window_start_us = 0;
    static long long mbt_accum_us = 0;
    static long long mbt_calls = 0;
    if (mbt_window_start_us == 0) mbt_window_start_us = t_enter;
    if (t_enter - mbt_window_start_us >= 1000000) {
        serverLog(LL_NOTICE,
            "mergeBackpatchTick 1s window: calls=%lld main_thread_us=%lld (%.1f%% of 1s)",
            mbt_calls, mbt_accum_us, (double) mbt_accum_us / 10000.0);
        mbt_window_start_us = t_enter;
        mbt_accum_us = 0;
        mbt_calls = 0;
    }
    mbt_calls++;

    pthread_mutex_lock(&backpatch_merge_mu);
    if (backpatch_merge_queue == NULL || listLength(backpatch_merge_queue) == 0) {
        pthread_mutex_unlock(&backpatch_merge_mu);
        backpatch_merge_timer_id = -1;
        return AE_NOMORE;
    }
    int queue_depth = (int) listLength(backpatch_merge_queue);
    listNode *ln = listFirst(backpatch_merge_queue);
    backpatchMergeWork *w = listNodeValue(ln);
    pthread_mutex_unlock(&backpatch_merge_mu);

    /* Lazy iterator init on the first tick that touches this work item. */
    if (w->iter == NULL && w->shadow != NULL) {
        w->iter = dictGetSafeIterator(w->shadow);
    }

    int budget = MERGE_KEYS_PER_TICK;
    if (w->iter != NULL) {
        dictEntry *de;
        while (budget > 0 && (de = dictNext(w->iter)) != NULL) {
            /* dbDictType is no_value=1 so the entry IS the kvobj; pull the
             * lookup sds via kvobjGetKey (used everywhere else: evict.c,
             * cluster.c, keymeta.c). */
            kvobj *src = (kvobj *) dictGetKV(de);
            sds    k   = kvobjGetKey(src);
            /* applySlotCb repointed src->ptr to the value sds embedded in the
             * donor landing pool, so this is the value to copy. */
            sds    v   = (sds) src->ptr;

            /* AqRaft pool-free: copy the migrated kvobj into the recipient's
             * OWN compact r_allocator block (same path a normal SET takes in
             * db.c), instead of installing the pool-resident kvobj. The live
             * keyspace then never references the donor landing pool, which lets
             * us reclaim that pool's physical pages (~11 GB on the recipient) —
             * removing the cache/memory-pressure overhead that made sg4 burn
             * ~40% more CPU per op and become the post-migration bottleneck.
             * The pool-resident src kvobj stays in the shadow (NOT unlinked) and
             * is freed by the dbDictType destructor at dictRelease. Runs on the
             * main thread, throttled by MERGE_KEYS_PER_TICK. */
            if (kvstoreDictFind(w->db->keys, w->slot, k) != NULL) {
                /* DON'T-CLOBBER: a post-FLIP client write already populated this
                 * key. Skip WITHOUT building a copy — checking existence first
                 * avoids creating a managed kvobj we'd then have to free (the
                 * direct free path crashed on the recipient). Single-threaded
                 * merge, so find-then-insert is race-free. */
                w->skipped_existing++;
            } else {
                int nb = 0;
                kvobj *kvc = r_allocator_insert_kvobj(w->slot, k, v, &nb);
                if (kvc != NULL) {
                    dictEntry *ex = NULL;
                    kvstoreDictAddRaw(w->db->keys, w->slot, kvc, &ex);
                    w->moved++;
                } else {
                    /* insert refused (defensive sds-length check) — drop key. */
                    w->skipped_existing++;
                }
            }
            budget--;
        }

        if (de == NULL) {
            /* Shadow exhausted. */
            dictReleaseIterator(w->iter);
            w->iter = NULL;
            dictRelease(w->shadow);
            w->shadow = NULL;
        }
    }

    if (w->shadow == NULL) {
        /* This slot is fully applied. Update the batch as the worker used
         * to do, then drop the work item. If this was the last slot of
         * the batch, transition to BACKPATCH_DONE and dispose. */
        backpatchBatch *b = w->batch;
        /* AqRaft Patch 29: recipient FOLLOWERS enqueue merge work with
         * batch == NULL (rdmaFollowerEnqueueSlotMerge). They reuse this same
         * main-thread shadow->live drain, but must NOT run the leader-only
         * batch accounting / BACKPATCH_DONE transition / chainForwardWorker
         * spawn below — the follower forwards downstream via the separate
         * g_chain_worker (CHAIN-FORWARDED) path, not via chainForwardWorker.
         * The dequeue + zfree(w) + timer logic after this block stays
         * unconditional so the follower's work item is still freed. */
        if (b != NULL) {
        atomic_fetch_add_explicit(&b->idx, 1, memory_order_release);
        atomic_fetch_add_explicit(&b->applied, w->moved, memory_order_relaxed);
        atomic_fetch_add_explicit(&b->clobber_skipped,
                                  (long long) w->skipped_existing,
                                  memory_order_relaxed);
        int prev = atomic_fetch_sub_explicit(&b->remaining, 1, memory_order_acq_rel);
        if (prev == 1) {
            /* AqRaft 3-flag DONE: this is condition (2) — leader merge done.
             * We do NOT flip b->state to BACKPATCH_DONE here anymore: the
             * BACKPATCH-STATUS handler reports "done" only when all three
             * flags (merge_done, chain_acked, indx_applied) are set, so the
             * donor's poll correctly waits until chain has replicated to
             * majority AND MGN_INDX_UPD has committed. */
            atomic_store_explicit(&b->merge_done, 1, memory_order_release);
            b->t_ended = time(NULL);
            atomicDecr(server.recipient_backpatch_in_progress, 1);

            /* AqRaft lever #4 fix: the merge has now fully drained — no more
             * mergeBackpatchTick reads of v=src->ptr into the landing pool. It
             * is finally safe to reclaim the landing pool. Do it here (rather
             * than only in backpatchFinalize) because the chain-forward spawn
             * fires on last-snapshot-captured and can run backpatchFinalize
             * BEFORE this point; in that case finalize saw merge_done==0 and
             * skipped the release, leaving it to us. CAS-guarded → exactly once. */
            backpatchReleaseLandingPool(b);

            /* Parallel chain: pool worker may already have spawned
             * chainForwardWorker when it captured the last snapshot
             * (snapshots_captured == n_slots). CAS-fallback here in case
             * it hasn't (e.g. no chain configured ⇒ pool workers don't
             * touch snapshots_captured) so the chain/finalize path still
             * runs exactly once. */
            int expected = 0;
            if (atomic_compare_exchange_strong_explicit(
                    &b->chain_spawn_initiated, &expected, 1,
                    memory_order_acq_rel, memory_order_relaxed)) {
                spawnChainForwardWorker(b);
            }
        }
        } /* end if (b != NULL) */

        pthread_mutex_lock(&backpatch_merge_mu);
        listDelNode(backpatch_merge_queue, ln);
        int more = listLength(backpatch_merge_queue) > 0;
        pthread_mutex_unlock(&backpatch_merge_mu);
        zfree(w);

        if (!more) {
            backpatch_merge_timer_id = -1;
            long long t_exit = ustime();
            long long us = t_exit - t_enter;
            mbt_accum_us += us;
            if (us > 50000) {
                serverLog(LL_WARNING,
                    "mergeBackpatchTick LAST: took %lld us (queue_depth=%d) — main-thread blocked",
                    us, queue_depth);
            }
            return AE_NOMORE;
        }
    }

    long long t_exit = ustime();
    long long us = t_exit - t_enter;
    mbt_accum_us += us;
    if (us > 50000) {
        serverLog(LL_WARNING,
            "mergeBackpatchTick: took %lld us (queue_depth=%d) — main-thread blocked",
            us, queue_depth);
    }
    return MERGE_TICK_DELAY_MS;
}

static void *backpatchPoolWorkerMain(void *arg) {
    long worker_idx = (long) arg;
    while (1) {
        pthread_mutex_lock(&backpatch_work_mu);
        while (listLength(backpatch_work_queue) == 0 &&
               !atomic_load_explicit(&backpatch_thread_shutdown, memory_order_acquire)) {
            pthread_cond_wait(&backpatch_work_cv, &backpatch_work_mu);
        }
        if (atomic_load_explicit(&backpatch_thread_shutdown, memory_order_acquire) &&
            listLength(backpatch_work_queue) == 0) {
            pthread_mutex_unlock(&backpatch_work_mu);
            break;
        }
        listNode *ln = listFirst(backpatch_work_queue);
        backpatchSlotWork *w = listNodeValue(ln);
        listDelNode(backpatch_work_queue, ln);
        pthread_mutex_unlock(&backpatch_work_mu);

        /* Pass-through chain snapshot: before the shadow-merge corrupts
         * r_allocator's block[0] for this slot (by allocating kvobj
         * segments INTO the same memory the donor RDMA-WROTE), capture
         * the raw 2 MiB block into the batch's donor_snapshot_pool. The
         * chain forwarder later RDMA-WRITEs this snapshot to followers
         * verbatim — no dense re-encode. */
        if (w->batch->donor_snapshot_pool != NULL) {
            int n_covered = 0;
            int *covered = NULL;
            pthread_mutex_lock(&w->batch->covered_mu);
            n_covered = w->batch->covered_slot_count;
            covered = w->batch->covered_slots;
            int found_idx = -1;
            for (int i = 0; i < n_covered; i++) {
                if (covered[i] == w->slot) { found_idx = i; break; }
            }
            pthread_mutex_unlock(&w->batch->covered_mu);
            if (found_idx >= 0) {
                int nb = 0;
                char **blocks = r_allocator_get_block_buffers_for_slot(w->slot, &nb);
                if (blocks != NULL && nb > 0 && blocks[0] != NULL) {
                    char *dst = w->batch->donor_snapshot_pool
                              + (size_t) found_idx * RDMAMIG_BLOCK_SIZE_BYTES;
                    if (dst + RDMAMIG_BLOCK_SIZE_BYTES
                        <= w->batch->donor_snapshot_pool + w->batch->donor_snapshot_pool_bytes) {
                        memcpy(dst, blocks[0], RDMAMIG_BLOCK_SIZE_BYTES);
                    }
                }
                if (blocks) zfree(blocks);
            }

            /* AqRaft parallel-chain: once ALL slot snapshots are captured,
             * spawn chainForwardWorker IMMEDIATELY so chain replication runs
             * in parallel with the remaining shadow build + main-thread
             * merge. The shadow build for this slot is about to overwrite
             * the donor's block segment headers, so by the time the last
             * worker reaches this point every slot's snapshot is safe to
             * forward. CAS-guard so we spawn exactly once (racing with
             * mergeBackpatchTick's fallback). */
            int captured = atomic_fetch_add_explicit(
                &w->batch->snapshots_captured, 1,
                memory_order_acq_rel) + 1;
            if (captured == w->batch->n_slots) {
                int expected = 0;
                if (atomic_compare_exchange_strong_explicit(
                        &w->batch->chain_spawn_initiated, &expected, 1,
                        memory_order_acq_rel, memory_order_relaxed)) {
                    spawnChainForwardWorker(w->batch);
                }
            }
        }

        /* Double-buffer backpatch (option 3): the worker fills a SHADOW
         * dict for this slot (no live keyspace access, no locks). When the
         * shadow is full, hand it off to the main thread via the merge
         * queue + dispose pipe. The merge tick (mergeBackpatchTick) is the
         * one that updates batch->idx/applied/remaining and signals DONE,
         * because the slot isn't truly "applied" until live has the data. */
        int total = 0, skipped = 0;
        dict *shadow = rdmaBackpatchSlotFillShadow(w->db, w->slot, &total, &skipped);
        if (skipped > 0) {
            serverLog(LL_WARNING,
                "RDMA backpatch-shadow: slot=%d skipped %d staged entries "
                "(donor block misclassification)", w->slot, skipped);
        }
        UNUSED(worker_idx);

        /* zcalloc, not zmalloc: skipped_existing is left unset by the field
         * assignments below but read+accumulated into batch->clobber_skipped
         * in mergeBackpatchTick. zmalloc leaked uninitialized stack garbage
         * into that counter (the 7e10 / negative clobber_skipped values in
         * the recipient DONE logs). The follower path already uses zcalloc. */
        backpatchMergeWork *mw = zcalloc(sizeof(*mw));
        mw->batch  = w->batch;
        mw->db     = w->db;
        mw->slot   = w->slot;
        mw->shadow = shadow;       /* may be NULL → merge tick treats as 0-key */
        mw->iter   = NULL;
        mw->total  = total;
        mw->moved  = 0;

        pthread_mutex_lock(&backpatch_merge_mu);
        if (backpatch_merge_queue == NULL) {
            backpatch_merge_queue = listCreate();
        }
        listAddNodeTail(backpatch_merge_queue, mw);
        pthread_mutex_unlock(&backpatch_merge_mu);

        /* Wake main thread (it arms the merge timer if not running). */
        char tick = 1;
        ssize_t wr = write(backpatch_dispose_pipe[1], &tick, 1);
        (void) wr;
        zfree(w);
    }
    return NULL;
}

/* AqRaft Patch 29: recipient-FOLLOWER per-slot merge enqueue.
 *
 * Converges the follower's chain-apply onto the proven leader design:
 * build the shadow dict OFF-main from the slot's (now r_allocator-registered)
 * blocks, then hand it to the SAME main-thread merge queue the leader uses so
 * the live-keyspace insert happens on the event-loop thread — serialized with
 * raft-apply of client writes by construction (no per-slot lock needed; those
 * locks are no-ops under redisraft anyway).
 *
 * Unlike the leader's backpatchPoolWorkerMain enqueue, the work item carries
 * batch == NULL: the follower has no backpatchBatch and must not trigger the
 * leader-only BACKPATCH_DONE / chainForwardWorker transition in
 * mergeBackpatchTick (guarded there by `if (b != NULL)`). zcalloc zero-inits
 * skipped_existing/moved/iter.
 *
 * Precondition: the slot's landing-pool slice(s) have already been registered
 * via r_allocator_register_existing_block (done in chainApplyWorker), so
 * rdmaBackpatchSlotFillShadow can find them through
 * r_allocator_get_block_buffers_for_slot. Returns the number of staged keys,
 * or 0 if the slot had no blocks. Safe to call from a worker thread OR the
 * main thread (it only appends + writes the wake pipe). */
int rdmaFollowerEnqueueSlotMerge(redisDb *db, int slot) {
    int total = 0, skipped = 0;
    dict *shadow = rdmaBackpatchSlotFillShadow(db, slot, &total, &skipped);
    if (skipped > 0) {
        serverLog(LL_WARNING,
            "RDMA follower-merge: slot=%d skipped %d staged entries "
            "(donor block misclassification)", slot, skipped);
    }
    if (shadow == NULL) return 0;

    backpatchMergeWork *mw = zcalloc(sizeof(*mw));
    mw->batch  = NULL;             /* follower: no batch accounting / no chain-forward */
    mw->db     = db;
    mw->slot   = slot;
    mw->shadow = shadow;
    mw->iter   = NULL;
    mw->total  = total;
    mw->moved  = 0;

    pthread_mutex_lock(&backpatch_merge_mu);
    if (backpatch_merge_queue == NULL) {
        backpatch_merge_queue = listCreate();
    }
    listAddNodeTail(backpatch_merge_queue, mw);
    pthread_mutex_unlock(&backpatch_merge_mu);

    /* Wake the main thread; backpatchDisposePipeHandler arms mergeBackpatchTick. */
    char tick = 1;
    ssize_t wr = write(backpatch_dispose_pipe[1], &tick, 1);
    (void) wr;
    return total;
}

/* Drain the SPSC ring; called by recipientBackpatchThreadMain after sem_wait.
 * For each batch pulled off the ring, fan its slots out to the pool work queue
 * and advance the tail immediately — the last pool worker to finish a slot
 * transitions the batch to DONE and disposes it. */
static void drainBackpatchRing(void) {
    uint64_t tail = atomic_load_explicit(&backpatch_ring_tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&backpatch_ring_head, memory_order_acquire);
    while (tail != head) {
        backpatchBatch *b = backpatch_ring[tail & (BACKPATCH_RING_CAPACITY - 1)];
        atomic_store_explicit(&b->state, BACKPATCH_RUNNING, memory_order_relaxed);
        serverLog(LL_NOTICE,
            "RDMA backpatch-thread: dispatching batch from %.*s mig_id=%lld "
            "n_slots=%d pool=%d",
            CLUSTER_NAMELEN, b->src_node_id, b->src_mig_id, b->n_slots,
            backpatch_pool_size);

        /* Build all items outside the queue lock so we hold the lock only long
         * enough to splice the tail. */
        backpatchSlotWork **items = zmalloc((size_t) b->n_slots * sizeof(*items));
        for (int i = 0; i < b->n_slots; i++) {
            backpatchSlotWork *w = zmalloc(sizeof(*w));
            w->batch = b;
            w->db    = b->db;
            w->slot  = b->slots[i];
            items[i] = w;
        }

        pthread_mutex_lock(&backpatch_work_mu);
        for (int i = 0; i < b->n_slots; i++)
            listAddNodeTail(backpatch_work_queue, items[i]);
        pthread_cond_broadcast(&backpatch_work_cv);
        pthread_mutex_unlock(&backpatch_work_mu);
        zfree(items);

        atomic_store_explicit(&backpatch_ring_tail, tail + 1, memory_order_release);
        tail++;
        head = atomic_load_explicit(&backpatch_ring_head, memory_order_acquire);
    }
}

static void *recipientBackpatchThreadMain(void *arg) {
    UNUSED(arg);
    while (!atomic_load_explicit(&backpatch_thread_shutdown, memory_order_acquire)) {
        sem_wait(&backpatch_wake);
        if (atomic_load_explicit(&backpatch_thread_shutdown, memory_order_acquire))
            break;
        drainBackpatchRing();
    }
    return NULL;
}

/* Main-thread pipe handler: free completed batches. The backpatchBatch records
 * are kept in backpatch_batches_by_key (the BACKPATCH-STATUS handler may still read
 * them); we only ack the pipe tickle here so the backpatch thread can write more.
 * Actual eviction of stale records is via a TTL sweep in clusterCron — kept
 * simple for now: keep last N completed batches indexed by key. */
static void backpatchDisposePipeHandler(aeEventLoop *el, int fd, void *priv, int mask) {
    UNUSED(el); UNUSED(priv); UNUSED(mask);
    char buf[64];
    while (read(fd, buf, sizeof(buf)) > 0) { /* drain */ }
    /* The pipe is shared by two producers:
     *   1. The pool worker after filling a slot's shadow → arms the merge timer.
     *   2. The dispatch tail (last slot of a batch DONE) → batch in dispose_list.
     * Both paths just write a byte; the handler does the cheap thing for each. */
    armMergeTimer();
    /* AqRaft Patch 16: chainForwardWorker (off-main) writes the pipe after
     * appending to backpatch_chain_pending. The chain-pending timer is
     * main-thread-only (aeCreateTimeEvent is not thread-safe), so arm it
     * here. Safe to call when there's nothing pending — armChainPendingTimer
     * is a no-op if a timer already exists. */
    armChainPendingTimer();
    /* The completed batches stay in backpatch_batches_by_key until cleanup.
     * For now we don't free here — BACKPATCH-STATUS needs them. */
}

void recipientBackpatchThreadStart(void) {
    if (backpatch_thread_started) return;

    sem_init(&backpatch_wake, 0, 0);

    if (pipe(backpatch_dispose_pipe) != 0) {
        serverLog(LL_WARNING, "recipientBackpatchThreadStart: pipe() failed: %s",
                  strerror(errno));
        return;
    }
    /* Non-blocking on the read end so the handler's drain loop terminates. */
    int flags = fcntl(backpatch_dispose_pipe[0], F_GETFL, 0);
    fcntl(backpatch_dispose_pipe[0], F_SETFL, flags | O_NONBLOCK);
    if (aeCreateFileEvent(server.el, backpatch_dispose_pipe[0], AE_READABLE,
                          backpatchDisposePipeHandler, NULL) == AE_ERR) {
        serverLog(LL_WARNING, "recipientBackpatchThreadStart: aeCreateFileEvent failed");
        close(backpatch_dispose_pipe[0]);
        close(backpatch_dispose_pipe[1]);
        backpatch_dispose_pipe[0] = backpatch_dispose_pipe[1] = -1;
        return;
    }

    backpatch_dispose_list  = listCreate();
    /* sdsHashDictType: sds key (we free it via dictSdsDestructor), opaque
     * value (we don't auto-free it — the dispose path owns the lifetime). */
    backpatch_batches_by_key = dictCreate(&sdsHashDictType);
    backpatch_work_queue     = listCreate();

    /* Register-job pipe + list for deferred REGISTER-BLOCK-SLOTS replies. */
    /* No infra init for the REGISTER-BLOCK-SLOTS path — see comment near the
     * registerJob struct: each worker is self-contained. */

    pthread_attr_t attr;
    size_t stacksize;
    pthread_attr_init(&attr);
    pthread_attr_getstacksize(&attr, &stacksize);
    if (!stacksize) stacksize = 1;
    while (stacksize < BACKPATCH_THREAD_STACK_SIZE) stacksize *= 2;
    pthread_attr_setstacksize(&attr, stacksize);

    /* Snapshot the config (clamped to the static array bound) so the rest of
     * this function and Stop see a consistent value. */
    backpatch_pool_size = server.rdma_backpatch_pool_size;
    if (backpatch_pool_size < 1) backpatch_pool_size = 1;
    if (backpatch_pool_size > BACKPATCH_POOL_MAX) backpatch_pool_size = BACKPATCH_POOL_MAX;

    /* Start the pool workers BEFORE the dispatcher so the work queue has
     * consumers before the dispatcher can begin enqueueing. */
    for (long i = 0; i < backpatch_pool_size; i++) {
        int perr = pthread_create(&backpatch_pool_tids[i], &attr,
                                  backpatchPoolWorkerMain, (void *)(long) i);
        if (perr) {
            serverLog(LL_WARNING,
                "recipientBackpatchThreadStart: pool worker %ld pthread_create failed: %s",
                i, strerror(perr));
            /* Tear down whatever pool workers did start. */
            atomic_store_explicit(&backpatch_thread_shutdown, 1, memory_order_release);
            pthread_mutex_lock(&backpatch_work_mu);
            pthread_cond_broadcast(&backpatch_work_cv);
            pthread_mutex_unlock(&backpatch_work_mu);
            for (long j = 0; j < i; j++) pthread_join(backpatch_pool_tids[j], NULL);
            pthread_attr_destroy(&attr);
            return;
        }
    }
    backpatch_pool_started = 1;

    int err = pthread_create(&backpatch_thread_tid, &attr, recipientBackpatchThreadMain, NULL);
    pthread_attr_destroy(&attr);
    if (err) {
        serverLog(LL_WARNING,
                  "recipientBackpatchThreadStart: pthread_create failed: %s",
                  strerror(err));
        /* Pool workers are already running; tear them down so the call site
         * doesn't end up with half-initialized state. */
        atomic_store_explicit(&backpatch_thread_shutdown, 1, memory_order_release);
        pthread_mutex_lock(&backpatch_work_mu);
        pthread_cond_broadcast(&backpatch_work_cv);
        pthread_mutex_unlock(&backpatch_work_mu);
        for (int i = 0; i < backpatch_pool_size; i++)
            pthread_join(backpatch_pool_tids[i], NULL);
        backpatch_pool_started = 0;
        return;
    }
    backpatch_thread_started = 1;
    serverLog(LL_NOTICE,
        "Recipient backpatch dispatcher + %d pool workers started (Phase 4d).",
        backpatch_pool_size);
}

void recipientBackpatchThreadStop(void) {
    if (!backpatch_thread_started) return;
    atomic_store_explicit(&backpatch_thread_shutdown, 1, memory_order_release);

    /* Stop dispatcher first so it stops enqueueing into the pool work queue. */
    sem_post(&backpatch_wake);
    pthread_join(backpatch_thread_tid, NULL);

    /* Now drain-and-exit the pool workers: with no more producer, their loop
     * condition (queue empty AND shutdown) eventually fires. */
    if (backpatch_pool_started) {
        pthread_mutex_lock(&backpatch_work_mu);
        pthread_cond_broadcast(&backpatch_work_cv);
        pthread_mutex_unlock(&backpatch_work_mu);
        for (int i = 0; i < backpatch_pool_size; i++)
            pthread_join(backpatch_pool_tids[i], NULL);
        backpatch_pool_started = 0;
    }
    backpatch_thread_started = 0;
}

/* ---- Recipient REGISTER-BLOCK-SLOTS deferred worker (Phase 4d) ---------- */

/* Recipient-side worker for the deferred REGISTER-BLOCK-SLOTS. Does the heavy
 * ibv_reg_mr loop, then opens a hiredis TCP connection BACK to the source
 * (src_host:src_port saved on the job) and delivers the result via:
 *
 *   RDMA REGISTER-RESULT <register_id> <"OK"|"ERR errmsg"> [<binary-payload>]
 *
 * Source-side rdmaRegisterResultCommand looks up the pending registration
 * by register_id, populates L->buffers[], and signals the source's worker
 * (which is condvar-waiting). */
static void *registerWorkerThread(void *arg) {
    registerJob *job = arg;

    /* Aqueduct big-MR PREP: instead of one zmalloc + one ibv_reg_mr per
     * slot (N ioctls into /dev/infiniband/uverbs*), allocate ONE contiguous
     * mmap pool sized for the whole job and register it with a SINGLE
     * ibv_reg_mr. Each slot gets a stride-sized sub-pointer; all slots
     * share the same rkey (standard RDMA semantic — rkey is valid for any
     * VA inside the registered range). The donor's RDMA WRITE path only
     * needs (remote_addr, remote_key) per slot, so the wire format is
     * unchanged. Pool is leaked, matching the existing per-block leak. */
    size_t total_blocks = 0;
    for (int i = 0; i < job->n_pairs; i++) {
        total_blocks += (size_t) job->n_blocks_per_slot[i];
    }
    if (total_blocks == 0) {
        /* Nothing to register; deliver an empty OK reply. */
        goto deliver;
    }

    const size_t stride = r_allocator_block_stride_bytes();
    const size_t pool_bytes = total_blocks * stride;

    void *pool = mmap(NULL, pool_bytes, PROT_READ | PROT_WRITE,
                      MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (pool == MAP_FAILED) {
        job->has_error = 1;
        snprintf(job->err_msg, sizeof(job->err_msg),
            "mmap(%zu bytes) failed: %s (check vm.overcommit / available RAM)",
            pool_bytes, strerror(errno));
        goto deliver;
    }

    struct rdmamig_buffer *pool_buf = rdmamig_buffer_create(
        rdmamig_server_cm_id(job->conn->s),
        (char *) pool, pool_bytes, 0);
    if (pool_buf == NULL) {
        job->has_error = 1;
        snprintf(job->err_msg, sizeof(job->err_msg),
            "ibv_reg_mr(%zu bytes) failed: check ulimit -l (RLIMIT_MEMLOCK)",
            pool_bytes);
        munmap(pool, pool_bytes);
        goto deliver;
    }
    job->conn->aqueduct_pool_buf = pool_buf;
    /* AqRaft pool-free: record the landing-pool region so the backpatch path
     * can madvise(MADV_DONTNEED) it once migrated kvobjs are copied out. */
    job->conn->landing_pool_base  = pool;
    job->conn->landing_pool_bytes = pool_bytes;
    const uint32_t shared_rkey = rdmamig_buffer_rkey(pool_buf);

    serverLog(LL_NOTICE,
        "RDMA REGISTER-BLOCK-SLOTS: aqueduct big-MR pool base=%p bytes=%zu "
        "stride=%zu total_blocks=%zu rkey=0x%x (replaces %zu ibv_reg_mr ioctls with 1)",
        pool, pool_bytes, stride, total_blocks, shared_rkey, total_blocks);

    size_t cursor = 0;
    for (int i = 0; i < job->n_pairs; i++) {
        int slot = job->slot_ids[i];
        int n = job->n_blocks_per_slot[i];
        for (int k = 0; k < n; k++) {
            void *sub = (char *) pool + cursor;
            cursor += stride;
            if (r_allocator_register_existing_block(slot, sub) == NULL) {
                job->has_error = 1;
                snprintf(job->err_msg, sizeof(job->err_msg),
                    "r_allocator_register_existing_block failed (slot=%d block=%d)",
                    slot, k);
                goto deliver;
            }
            job->result[job->total_buffers].ptr  = (uint64_t) sub;
            job->result[job->total_buffers].rkey = shared_rkey;
            job->total_buffers++;
        }
    }

deliver:
    {
        serverLog(LL_NOTICE,
            "RDMA REGISTER-RESULT: register_id=%s total_buffers=%d, dialing back to %s:%d",
            job->register_id, job->total_buffers, job->src_host, job->src_port);

        /* Protocol log: recipient is ready to receive (buffers registered).
         * Logged BEFORE the dial-back so all recipient replicas record the
         * start of the session before the donor is told to proceed. Skipped
         * on the error path so we don't log a phantom "ready" event. */
        if (!job->has_error) {
            int first_slot = (job->n_pairs > 0) ? job->slot_ids[0] : -1;
            int last_slot  = (job->n_pairs > 0) ? job->slot_ids[job->n_pairs - 1] : -1;
            char mgn_payload[256];
            snprintf(mgn_payload, sizeof(mgn_payload),
                     "register_id=%s slots=%d-%d n=%d donor=%s:%d",
                     job->register_id, first_slot, last_slot, job->n_pairs,
                     job->src_host, job->src_port);
            rdmaMgnLogSync("RECP_TXN_START", mgn_payload);
        }

        redisContext *ctx = redisConnect(job->src_host, job->src_port);
        if (ctx == NULL || ctx->err) {
            serverLog(LL_WARNING,
                "RDMA REGISTER-RESULT: redisConnect(%s:%d) failed: %s",
                job->src_host, job->src_port,
                ctx ? ctx->errstr : "(null ctx)");
            if (ctx) redisFree(ctx);
            goto cleanup;
        }

        const char *status = job->has_error ? job->err_msg : "OK";
        size_t bin_len = job->has_error ? 0 :
            (size_t) job->total_buffers * sizeof(rdmaRemoteBufferInfo);
        const void *bin_ptr = job->has_error ? "" : (const void *) job->result;

        /* 5-element form: RDMA REGISTER-RESULT <id> <status> <binary>. */
        const char *argv5[5];
        size_t      argvlen5[5];
        argv5[0] = "RDMA";              argvlen5[0] = 4;
        argv5[1] = "REGISTER-RESULT";   argvlen5[1] = 15;
        argv5[2] = job->register_id;    argvlen5[2] = strlen(job->register_id);
        argv5[3] = status;              argvlen5[3] = strlen(status);
        argv5[4] = bin_ptr;             argvlen5[4] = bin_len;

        redisReply *r = redisCommandArgv(ctx, 5, argv5, argvlen5);
        if (r == NULL || r->type == REDIS_REPLY_ERROR) {
            serverLog(LL_WARNING,
                "RDMA REGISTER-RESULT: bad reply from source %s:%d: %s (ctxerr=%s)",
                job->src_host, job->src_port,
                (r && r->str) ? r->str : "(null)",
                ctx->errstr[0] ? ctx->errstr : "(no ctxerr)");
        } else {
            serverLog(LL_NOTICE,
                "RDMA REGISTER-RESULT: register_id=%s delivered (%zu bytes)",
                job->register_id, bin_len);
        }
        if (r) freeReplyObject(r);
        redisFree(ctx);
    }

cleanup:
    zfree(job->slot_ids);
    zfree(job->n_blocks_per_slot);
    zfree(job->result);
    zfree(job);
    return NULL;
}

/* RDMA BACKPATCH-STATUS <src_node_id> <src_mig_id>
 *
 * Source-polled status of a recipient backpatch batch. Returns a 6-element
 * reply: state (string: queued|running|done|failed), idx (slots applied
 * so far), n_slots, applied_keys, elapsed_seconds, err (empty if none). */
static const char *backpatchStateName(backpatchBatchState s) {
    switch (s) {
    case BACKPATCH_QUEUED:  return "queued";
    case BACKPATCH_RUNNING: return "running";
    case BACKPATCH_DONE:    return "done";
    case BACKPATCH_FAILED:  return "failed";
    }
    return "unknown";
}

void rdmaBackpatchStatusCommand(client *c) {
    if (c->argc != 4) {
        addReplyError(c, "syntax: RDMA BACKPATCH-STATUS src_node_id src_mig_id");
        return;
    }
    long long mig_id;
    if (getLongLongFromObject(c->argv[3], &mig_id) != C_OK) {
        addReplyError(c, "src_mig_id must be an integer");
        return;
    }
    sds src_id = c->argv[2]->ptr;
    if (sdslen(src_id) != CLUSTER_NAMELEN) {
        addReplyError(c, "src_node_id must be 40 chars");
        return;
    }

    sds key = backpatchBatchKey(src_id, mig_id);
    pthread_mutex_lock(&backpatch_batches_mu);
    backpatchBatch *b = backpatch_batches_by_key ? dictFetchValue(backpatch_batches_by_key, key) : NULL;
    pthread_mutex_unlock(&backpatch_batches_mu);
    sdsfree(key);

    if (b == NULL) {
        addReplyError(c, "no such backpatch batch");
        return;
    }

    int state    = atomic_load_explicit(&b->state, memory_order_acquire);
    int idx      = atomic_load_explicit(&b->idx,   memory_order_relaxed);
    long long ap = atomic_load_explicit(&b->applied, memory_order_relaxed);
    pthread_mutex_lock(&b->err_mu);
    sds err_copy = b->err ? sdsdup(b->err) : sdsempty();
    pthread_mutex_unlock(&b->err_mu);

    /* AqRaft 3-flag DONE invariant: only report "done" to the donor when
     * (1) the recipient leader's merge finished (merge_done),
     * (2) the chain has replicated to majority of recipient followers
     *     (chain_acked, set by chainPendingTick when CHAIN-ACK arrives), and
     * (3) MGN_INDX_UPD has been appended to the raft log (indx_applied,
     *     set by backpatchFinalize).
     * Without all three, the donor would release its slots while either
     * followers don't have the bytes or the migration metadata isn't durable
     * — a recipient-leader crash in that window silently loses data. */
    int md  = atomic_load_explicit(&b->merge_done,   memory_order_acquire);
    int ca  = b->chain_acked;
    int ia  = atomic_load_explicit(&b->indx_applied, memory_order_acquire);
    int reported_state = state;
    if (state == BACKPATCH_RUNNING && md && ca && ia) {
        /* All three durability conditions met — promote the reply to "done"
         * even though we no longer eagerly flip b->state to BACKPATCH_DONE
         * at merge end. */
        reported_state = BACKPATCH_DONE;
    } else if (state == BACKPATCH_DONE && !(md && ca && ia)) {
        /* Internal state has been flipped (legacy path) but at least one of
         * the three flags is still pending — downgrade the externally-
         * reported state so the donor waits. */
        reported_state = BACKPATCH_RUNNING;
    }

    long long elapsed = (long long) ((b->t_ended ? b->t_ended : time(NULL)) - b->t_started);

    /* AqRaft Round 2: element[6] is the chain-durable flag (merge_done &&
     * chain_acked). When set, the recipient leader has merged this donor's
     * data into its keyspace AND the chain has replicated to a majority of
     * followers; only the metadata-only MGN_INDX_UPD raft append (indx_applied)
     * is still outstanding. The donor's poll loop forwards this to the
     * orchestrator as an early CHAIN_DURABLE signal so the next donor can be
     * dispatched while this donor's INDX_UPD commits. */
    int chain_durable = (md && ca) ? 1 : 0;

    addReplyArrayLen(c, 7);
    addReplyBulkCString(c, backpatchStateName((backpatchBatchState) reported_state));
    addReplyLongLong(c, idx);
    addReplyLongLong(c, b->n_slots);
    addReplyLongLong(c, ap);
    addReplyLongLong(c, elapsed);
    addReplyBulkSds(c, err_copy);
    addReplyLongLong(c, chain_durable);
}


/* ====================================================================== *
 *  RDMA MIGRATE-PREP  (source-side bootstrap RPC)
 *
 *  Hides the existing recipient subcommands (INIT-SERVER, REGISTER-BLOCK-
 *  SLOTS) behind one operator-facing call:
 *
 *      RDMA MIGRATE-PREP <host> <port> <slot> [<slot> ...]
 *
 *  Caches a single RDMA control link per (source, recipient) pair in
 *  server.rdma_outbound_links so subsequent prep calls reuse the same
 *  RDMA QP and TCP control channel.
 * ====================================================================== */

/* Open a fresh outbound link to `host:port`. On success returns a link with
 * a connected hiredis context and a connected rdmamig client; the caller
 * is responsible for inserting it into the dict. Returns NULL on failure
 * (with all partial resources freed and a serverLog warning emitted).
 *
 * `key` is consumed by the caller — we do not free or take ownership of it
 * here; the dict owns the sds key after dictAdd. We still copy the addr
 * into `L->addr` (an independent sds) for diagnostic dumps. */
static rdmaOutboundLink *rdmaOutboundLinkOpen(const char *host, int port) {
    rdmaOutboundLink *L = zcalloc(sizeof(*L));
    L->addr = sdscatfmt(sdsempty(), "%s:%i", host, port);
    pthread_mutex_init(&L->mu, NULL);

    /* 1. Plain-TCP redis client connection to the recipient (for INIT-SERVER
     *    and REGISTER-BLOCK-SLOTS round-trips). Blocking — the prep RPC is
     *    a synchronous operator call. */
    L->ctrl = redisConnect(host, port);
    if (L->ctrl == NULL || L->ctrl->err != 0) {
        serverLog(LL_WARNING, "RDMA MIGRATE-PREP: redisConnect(%s:%d) failed: %s",
                  host, port, L->ctrl ? L->ctrl->errstr : "alloc failed");
        if (L->ctrl) redisFree(L->ctrl);
        sdsfree(L->addr);
        pthread_mutex_destroy(&L->mu);
        zfree(L);
        return NULL;
    }

    /* 2. Ask the recipient to bind its RDMA listener on server.rdma_migration_port.
     *    INIT-SERVER is idempotent on the recipient: if a server is already up,
     *    the existing handler keeps it and replies +OK. */
    redisReply *r = redisCommand(L->ctrl, "RDMA INIT-SERVER %d", server.rdma_migration_port);
    if (r == NULL || r->type == REDIS_REPLY_ERROR) {
        serverLog(LL_WARNING, "RDMA MIGRATE-PREP: INIT-SERVER on %s:%d failed: %s",
                  host, port, r ? r->str : "no reply");
        if (r) freeReplyObject(r);
        redisFree(L->ctrl);
        sdsfree(L->addr);
        pthread_mutex_destroy(&L->mu);
        zfree(L);
        return NULL;
    }
    freeReplyObject(r);

    /* 3. Bring up the source-side RDMA QP and connect to the recipient's
     *    just-bound RDMA listener. rdmamig_client_create takes the port as
     *    a string. */
    char rdma_port_str[16];
    snprintf(rdma_port_str, sizeof(rdma_port_str), "%d", server.rdma_migration_port);
    L->client = rdmamig_client_create((char *) host, rdma_port_str);
    if (L->client == NULL) {
        serverLog(LL_WARNING, "RDMA MIGRATE-PREP: rdmamig_client_create(%s:%s) failed",
                  host, rdma_port_str);
        redisFree(L->ctrl);
        sdsfree(L->addr);
        pthread_mutex_destroy(&L->mu);
        zfree(L);
        return NULL;
    }
    if (rdmamig_client_connect(L->client) != 0) {
        serverLog(LL_WARNING, "RDMA MIGRATE-PREP: rdmamig_client_connect(%s:%s) failed",
                  host, rdma_port_str);
        zfree(L->client);
        redisFree(L->ctrl);
        sdsfree(L->addr);
        pthread_mutex_destroy(&L->mu);
        zfree(L);
        return NULL;
    }

    serverLog(LL_NOTICE, "RDMA MIGRATE-PREP: new outbound link to %s (TCP %d, RDMA %d) up",
              host, port, server.rdma_migration_port);
    return L;
}

/* Dict valDestructor — invoked by dictRelease/dictDelete on the
 * server.rdma_outbound_links dict. */
void rdmaOutboundLinkFree(void *v) {
    rdmaOutboundLink *L = v;
    if (L == NULL) return;
    if (L->client) zfree(L->client);  /* rdmamig_client has no public disconnect/free */
    if (L->ctrl)   redisFree(L->ctrl);
    pthread_mutex_destroy(&L->mu);
    if (L->addr)   sdsfree(L->addr);
    zfree(L);
}

/* RDMA MIGRATE-PREP <host> <port> <slot> [<slot> ...]
 *
 *   argv[0] = "RDMA"
 *   argv[1] = "MIGRATE-PREP"
 *   argv[2] = host
 *   argv[3] = port
 *   argv[4..argc-1] = slots
 */
void rdmaMigratePrepCommand(client *c) {
    if (c->argc < 5) {
        addReplyError(c, "syntax: RDMA MIGRATE-PREP host port slot [slot ...]");
        return;
    }

    const char *host = c->argv[2]->ptr;
    long long port_ll;
    if (getLongLongFromObject(c->argv[3], &port_ll) != C_OK ||
        port_ll <= 0 || port_ll > 65535) {
        addReplyError(c, "port out of range");
        return;
    }
    int port = (int) port_ll;

    int n_slots = c->argc - 4;
    int *slots = zmalloc((size_t) n_slots * sizeof(int));
    for (int i = 0; i < n_slots; i++) {
        long long s_ll;
        if (getLongLongFromObject(c->argv[4 + i], &s_ll) != C_OK ||
            s_ll < 0 || s_ll >= CLUSTER_SLOTS) {
            zfree(slots);
            addReplyError(c, "slot id out of range");
            return;
        }
        slots[i] = (int) s_ll;
    }

    /* Cache lookup. The dict key is owned by the dict; if we dictAdd we hand
     * ownership over, otherwise we sdsfree below. */
    sds key = sdscatfmt(sdsempty(), "%s:%i", host, port);
    rdmaOutboundLink *L = dictFetchValue(server.rdma_outbound_links, key);
    int newly = 0;
    if (L == NULL) {
        L = rdmaOutboundLinkOpen(host, port);
        if (L == NULL) {
            sdsfree(key);
            zfree(slots);
            addReplyError(c, "could not establish outbound RDMA link (see server log)");
            return;
        }
        dictAdd(server.rdma_outbound_links, key, L);    /* dict now owns key */
        newly = 1;
    } else {
        sdsfree(key);                                   /* key not inserted */
    }

    /* Build "RDMA REGISTER-BLOCK-SLOTS SLOTS <s> 1 <s> 1 ..." via redisCommandArgv
     * (avoids RESP-formatting pitfalls of redisCommand %s expansion). */
    pthread_mutex_lock(&L->mu);

    int argc = 3 + 2 * n_slots;
    const char **argv = zmalloc((size_t) argc * sizeof(*argv));
    size_t *argvlen = zmalloc((size_t) argc * sizeof(*argvlen));
    argv[0] = "RDMA";                       argvlen[0] = 4;
    argv[1] = "REGISTER-BLOCK-SLOTS";       argvlen[1] = 20;
    argv[2] = "SLOTS";                      argvlen[2] = 5;

    /* Stable storage for slot/nblocks number strings. */
    char (*numbuf)[16] = zmalloc((size_t)(2 * n_slots) * sizeof(*numbuf));
    for (int i = 0; i < n_slots; i++) {
        argvlen[3 + 2*i]     = (size_t) snprintf(numbuf[2*i],     16, "%d", slots[i]);
        argv  [3 + 2*i]     = numbuf[2*i];
        argvlen[3 + 2*i + 1] = (size_t) snprintf(numbuf[2*i + 1], 16, "%d", 1);
        argv  [3 + 2*i + 1] = numbuf[2*i + 1];
    }
    redisReply *r = redisCommandArgv(L->ctrl, argc, argv, argvlen);
    zfree(argv);
    zfree(argvlen);
    zfree(numbuf);

    if (r == NULL || r->type != REDIS_REPLY_STRING ||
        r->len != (size_t) n_slots * sizeof(rdmaRemoteBufferInfo)) {
        if (r) freeReplyObject(r);
        pthread_mutex_unlock(&L->mu);
        zfree(slots);
        addReplyError(c, "RDMA REGISTER-BLOCK-SLOTS: malformed or error reply from recipient");
        return;
    }
    const rdmaRemoteBufferInfo *bufs = (const rdmaRemoteBufferInfo *) r->str;
    for (int i = 0; i < n_slots; i++) {
        L->buffers[slots[i]] = bufs[i];
    }
    freeReplyObject(r);
    pthread_mutex_unlock(&L->mu);

    serverLog(LL_NOTICE,
              "RDMA MIGRATE-PREP %s:%d -> %d slots registered (%s link)",
              host, port, n_slots, newly ? "new" : "cached");

    zfree(slots);
    addReplyStatusFormat(c, "OK %d slots, %d MiB (%s link)",
                         n_slots, n_slots, newly ? "new" : "cached");
}

/* ====================================================================== *
 *  RDMA RESHARD  (Phase 1: source-side MR registration only)
 *
 *  Mirrors `redis-cli --cluster reshard --cluster-slots N` UX from the
 *  perspective of a single source master. Phase 1: walk self's owned
 *  slots, allocate + ibv_reg_mr a 1 MiB staging buffer per slot, cache
 *  in the rdmaOutboundLink's source_buffers[]. No recipient contact,
 *  no data transfer, no CLUSTER SETSLOT — subsequent phases add those.
 *
 *  Precondition: RDMA MIGRATE-PREP must have already brought up the
 *  outbound link (the QP whose PD owns the new MRs).
 * ====================================================================== */

void rdmaReshardCommand(client *c) {
    if (c->argc != 5) {
        addReplyError(c, "syntax: RDMA RESHARD recipient-host recipient-port n-slots");
        return;
    }
    const char *host = c->argv[2]->ptr;
    long long port_ll, n_slots_ll;
    if (getLongLongFromObject(c->argv[3], &port_ll) != C_OK ||
        port_ll <= 0 || port_ll > 65535) {
        addReplyError(c, "port out of range");
        return;
    }
    if (getLongLongFromObject(c->argv[4], &n_slots_ll) != C_OK ||
        n_slots_ll <= 0 || n_slots_ll > CLUSTER_SLOTS) {
        addReplyError(c, "n-slots out of range");
        return;
    }
    int port = (int) port_ll;
    int n_slots = (int) n_slots_ll;

    /* Pick `n_slots` slots from self's owned set, lowest first. */
    if (rdmaMigrationGuard(c) != C_OK) return;
    int *chosen = zmalloc((size_t) n_slots * sizeof(int));
    int picked = 0;
    for (int i = 0; i < CLUSTER_SLOTS && picked < n_slots; i++) {
        if (rdmaMigrationOwnsSlot(i)) {
            chosen[picked++] = i;
        }
    }
    if (picked < n_slots) {
        zfree(chosen);
        addReplyErrorFormat(c, "self owns only %d slots, asked for %d", picked, n_slots);
        return;
    }

    /* Look up the outbound link cached by a prior MIGRATE-PREP. */
    sds key = sdscatfmt(sdsempty(), "%s:%i", host, port);
    rdmaOutboundLink *L = dictFetchValue(server.rdma_outbound_links, key);
    sdsfree(key);
    if (L == NULL) {
        zfree(chosen);
        addReplyError(c, "no outbound link cached; call RDMA MIGRATE-PREP first");
        return;
    }

    pthread_mutex_lock(&L->mu);

    int n_registered = 0, n_skipped = 0;
    for (int i = 0; i < n_slots; i++) {
        int slot = chosen[i];
        if (L->source_buffers[slot] != NULL) {
            n_skipped++;
            serverLog(LL_NOTICE,
                      "RDMA RESHARD: slot=%d already registered, skipping",
                      slot);
            continue;
        }
        void *staging = zmalloc(RDMAMIG_BLOCK_SIZE_BYTES);
        if (staging == NULL) {
            pthread_mutex_unlock(&L->mu);
            zfree(chosen);
            addReplyError(c, "zmalloc failed for staging buffer");
            return;
        }
        memset(staging, 0, RDMAMIG_BLOCK_SIZE_BYTES);
        struct rdmamig_buffer *rb = rdmamig_buffer_create(
            rdmamig_client_cm_id(L->client),
            staging, RDMAMIG_BLOCK_SIZE_BYTES, 0);
        if (rb == NULL) {
            serverLog(LL_WARNING,
                      "RDMA RESHARD: rdmamig_buffer_create failed for slot=%d",
                      slot);
            zfree(staging);
            pthread_mutex_unlock(&L->mu);
            zfree(chosen);
            addReplyError(c, "rdmamig_buffer_create failed (see server log)");
            return;
        }
        L->source_buffers[slot] = rb;
        n_registered++;
        serverLog(LL_NOTICE,
                  "RDMA RESHARD: registered slot=%d buf=%p rkey=0x%x size=%d",
                  slot, staging, rdmamig_buffer_rkey(rb), RDMAMIG_BLOCK_SIZE_BYTES);
    }

    pthread_mutex_unlock(&L->mu);

    /* Summarise. If the slot list is a contiguous range, render it as A..B;
     * otherwise just give first/last. */
    int first = chosen[0], last = chosen[n_slots - 1];
    int contiguous = (last - first + 1 == n_slots);
    zfree(chosen);

    if (contiguous) {
        addReplyStatusFormat(c,
            "OK %d source blocks registered (slots %d..%d, %d skipped)",
            n_registered, first, last, n_skipped);
    } else {
        addReplyStatusFormat(c,
            "OK %d source blocks registered (slots %d..%d non-contiguous, %d skipped)",
            n_registered, first, last, n_skipped);
    }
}

/* ====================================================================== *
 *  RDMA RESHARD-TRANSFER  (Phase 2: data-plane transfer)
 *
 *  Per-slot pipeline against the N slots a prior RDMA RESHARD registered:
 *    1. Encode the slot's keys into the source's pre-registered staging
 *       buffer (rdmaEncodeSlotEntries, leading u32 entry count).
 *    2. RDMA-WRITE that buffer to the recipient's landing buffer (the
 *       VA/rkey returned by RDMA MIGRATE-PREP).
 *    3. After all N slots are written, send "RDMA DONE-SLOTS <s1>..<sN>"
 *       over the cached hiredis ctrl channel; the recipient applies the
 *       landed bytes into its keyspace via rdmaDoneSlotsCommand.
 *
 *  Precondition: RDMA MIGRATE-PREP (recipient buffers cached in
 *  L->buffers[]) AND RDMA RESHARD (source MRs cached in L->source_buffers[])
 *  must have already run for these slots. Phase 2 does NOT flip slot
 *  ownership — that's Phase 3.
 *
 *  Reply: +OK <n> slots transferred (slots <first>..<last>, bytes B,
 *         errors E, done=<ok|fail>)
 * ====================================================================== */

/* Detached pthread worker that runs the actual RDMA WRITE loop + DONE-SLOTS
 * notification. rdmaReshardTransferCommand replies +OK immediately and this
 * thread does the network-bound work off the main event loop so the source
 * can keep serving traffic. */
struct reshardTransferArgs {
    rdmaOutboundLink *L;
    redisDb *db;
    int *chosen;       /* owned; freed by the worker */
    int n_slots;
};

static void *reshardTransferWorker(void *p) {
    struct reshardTransferArgs *a = (struct reshardTransferArgs *) p;

    pthread_mutex_lock(&a->L->mu);

    size_t total_bytes = 0;
    int errs = 0;
    for (int i = 0; i < a->n_slots; i++) {
        int slot = a->chosen[i];
        char *staging = rdmamig_buffer_data(a->L->source_buffers[slot]);
        uint32_t n_entries = rdmaEncodeSlotEntries(a->db, slot, staging,
                                                   RDMAMIG_BLOCK_SIZE_BYTES);
        rdmaDebugDumpSlotBytes("SRC", slot, staging);
        if (server.rdma_reshard_debug_bytes) {
            r_allocator_log_slot_stats(slot);
            serverLog(LL_NOTICE,
                "RDMA RESHARD-TRANSFER TX (thr): slot=%d n_entries=%u rdma_bytes=%d",
                slot, n_entries, RDMAMIG_BLOCK_SIZE_BYTES);
        }
        int rc = rdmamig_client_post_write(a->L->source_buffers[slot], staging,
                                           a->L->buffers[slot].ptr,
                                           a->L->buffers[slot].rkey,
                                           RDMAMIG_BLOCK_SIZE_BYTES);
        if (rc != 0) {
            errs++;
            serverLog(LL_WARNING,
                "RDMA RESHARD-TRANSFER (thr): slot=%d post_write failed rc=%d", slot, rc);
            continue;
        }
        int wc_rc = rdmamig_client_wait_send(a->L->client);
        if (wc_rc < 0) {
            errs++;
            serverLog(LL_WARNING,
                "RDMA RESHARD-TRANSFER (thr): slot=%d wait_send failed rc=%d", slot, wc_rc);
            continue;
        }
        total_bytes += RDMAMIG_BLOCK_SIZE_BYTES;
        serverLog(LL_NOTICE,
            "RDMA RESHARD-TRANSFER (thr): slot=%d entries=%u bytes=%d rc=0",
            slot, n_entries, RDMAMIG_BLOCK_SIZE_BYTES);
    }

    /* Notify recipient: "RDMA DONE-SLOTS <s1> <s2> ... <sN>". */
    int argc = 2 + a->n_slots;
    const char **argv = zmalloc((size_t) argc * sizeof(*argv));
    size_t *argvlen = zmalloc((size_t) argc * sizeof(*argvlen));
    argv[0] = "RDMA";       argvlen[0] = 4;
    argv[1] = "DONE-SLOTS"; argvlen[1] = 10;
    char (*numbuf)[16] = zmalloc((size_t) a->n_slots * sizeof(*numbuf));
    for (int i = 0; i < a->n_slots; i++) {
        argvlen[2 + i] = (size_t) snprintf(numbuf[i], 16, "%d", a->chosen[i]);
        argv  [2 + i] = numbuf[i];
    }
    redisReply *r = redisCommandArgv(a->L->ctrl, argc, argv, argvlen);
    zfree(argv);
    zfree(argvlen);
    zfree(numbuf);

    if (r == NULL || r->type != REDIS_REPLY_STATUS) {
        serverLog(LL_WARNING,
            "RDMA RESHARD-TRANSFER (thr): DONE-SLOTS reply not OK (data may still have landed)");
    }
    if (r) freeReplyObject(r);

    pthread_mutex_unlock(&a->L->mu);

    int first = a->chosen[0], last = a->chosen[a->n_slots - 1];
    serverLog(LL_NOTICE,
        "RDMA RESHARD-TRANSFER (thr): finished n=%d slots %d..%d bytes=%zu errs=%d",
        a->n_slots, first, last, total_bytes, errs);

    zfree(a->chosen);
    zfree(a);
    return NULL;
}

void rdmaReshardTransferCommand(client *c) {
    if (c->argc != 5) {
        addReplyError(c, "syntax: RDMA RESHARD-TRANSFER recipient-host recipient-port n-slots");
        return;
    }
    const char *host = c->argv[2]->ptr;
    long long port_ll, n_slots_ll;
    if (getLongLongFromObject(c->argv[3], &port_ll) != C_OK ||
        port_ll <= 0 || port_ll > 65535) {
        addReplyError(c, "port out of range");
        return;
    }
    if (getLongLongFromObject(c->argv[4], &n_slots_ll) != C_OK ||
        n_slots_ll <= 0 || n_slots_ll > CLUSTER_SLOTS) {
        addReplyError(c, "n-slots out of range");
        return;
    }
    int port = (int) port_ll;
    int n_slots = (int) n_slots_ll;

    if (rdmaMigrationGuard(c) != C_OK) return;

    sds key = sdscatfmt(sdsempty(), "%s:%i", host, port);
    rdmaOutboundLink *L = dictFetchValue(server.rdma_outbound_links, key);
    sdsfree(key);
    if (L == NULL) {
        addReplyError(c, "no outbound link cached; call RDMA MIGRATE-PREP + RDMA RESHARD first");
        return;
    }

    int *chosen = zmalloc((size_t) n_slots * sizeof(int));
    int picked = 0;
    for (int i = 0; i < CLUSTER_SLOTS && picked < n_slots; i++) {
        if (L->source_buffers[i] != NULL) {
            chosen[picked++] = i;
        }
    }
    if (picked < n_slots) {
        zfree(chosen);
        addReplyErrorFormat(c,
            "only %d slots pre-registered on this link, asked for %d; call RDMA RESHARD first",
            picked, n_slots);
        return;
    }

    for (int i = 0; i < n_slots; i++) {
        int slot = chosen[i];
        if (L->source_buffers[slot] == NULL) {
            zfree(chosen);
            addReplyErrorFormat(c, "slot %d not pre-registered; call RDMA RESHARD first", slot);
            return;
        }
        if (L->buffers[slot].ptr == 0) {
            zfree(chosen);
            addReplyErrorFormat(c,
                "slot %d has no recipient landing buffer; call RDMA MIGRATE-PREP first", slot);
            return;
        }
    }

    /* Spawn the detached worker. Main thread returns +OK immediately so the
     * source's event loop is free to serve client traffic (or ASKING reads)
     * during the RDMA-WRITE phase. */
    struct reshardTransferArgs *a = zmalloc(sizeof(*a));
    a->L = L;
    a->db = c->db;
    a->chosen = chosen;        /* worker owns it now */
    a->n_slots = n_slots;

    pthread_t t;
    if (pthread_create(&t, NULL, reshardTransferWorker, a) != 0) {
        zfree(a->chosen);
        zfree(a);
        addReplyError(c, "RESHARD-TRANSFER: pthread_create failed");
        return;
    }
    pthread_detach(t);

    addReplyStatusFormat(c,
        "OK %d slots scheduled (slots %d..%d) — RDMA-WRITE in background",
        n_slots, chosen[0], chosen[n_slots - 1]);
}


/* ====================================================================== *
 *  RDMA RESHARD-FLIP  (v0 — early ownership flip; parallel-reads NOT yet
 *                     implemented, tombstones NOT yet implemented)
 *
 *      RDMA RESHARD-FLIP <recipient-host> <recipient-port> <n-slots>
 *
 *  Source-side. Picks the same N slots a prior RDMA RESHARD registered,
 *  and for each:
 *    1. Locks the slot's r_allocator blocks (makes source-side data
 *       immutable for the upcoming RDMA-WRITE).
 *    2. Tells the recipient to claim ownership via CLUSTER SETSLOT NODE
 *       over the cached hiredis ctrl channel (L->ctrl).
 *    3. Locally releases ownership by calling clusterDelSlot +
 *       clusterAddSlot(recipient_node, slot) directly — bypassing the
 *       official CLUSTER SETSLOT NODE safety check that refuses to give
 *       up a slot while local keys still exist (which is precisely what
 *       this protocol wants: source keeps the keys until RESHARD-TRANSFER
 *       has RDMA-WRITE'd them to the recipient, then the source's copy
 *       becomes stale and can be released).
 *
 *  Known v0 limitations:
 *    - Reads on the recipient for keys not yet RDMA-WRITE'd from the
 *      source return NIL. (Parallel-read fallback is the next iteration.)
 *    - DELETE during the migration window may be resurrected by a later
 *      RDMA-WRITE. (Tombstone tracking is the next iteration.)
 *
 *  Reply: +OK <flipped> slots flipped (slots <first>..<last>)
 * ====================================================================== */

void rdmaReshardFlipCommand(client *c) {
    if (c->argc != 5) {
        addReplyError(c,
            "syntax: RDMA RESHARD-FLIP recipient-host recipient-port n-slots");
        return;
    }
    const char *host = c->argv[2]->ptr;
    long long port_ll, n_slots_ll;
    if (getLongLongFromObject(c->argv[3], &port_ll) != C_OK ||
        port_ll <= 0 || port_ll > 65535) {
        addReplyError(c, "port out of range");
        return;
    }
    if (getLongLongFromObject(c->argv[4], &n_slots_ll) != C_OK ||
        n_slots_ll <= 0 || n_slots_ll > CLUSTER_SLOTS) {
        addReplyError(c, "n-slots out of range");
        return;
    }
    int port = (int) port_ll;
    int n_slots = (int) n_slots_ll;

    if (rdmaMigrationGuard(c) != C_OK) return;

    /* Pick the same N slots as RESHARD: walk self-owned, lowest first. */
    int *chosen = zmalloc((size_t) n_slots * sizeof(int));
    int picked = 0;
    for (int i = 0; i < CLUSTER_SLOTS && picked < n_slots; i++) {
        if (rdmaMigrationOwnsSlot(i)) {
            chosen[picked++] = i;
        }
    }
    if (picked < n_slots) {
        zfree(chosen);
        addReplyErrorFormat(c, "self owns only %d slots, asked for %d",
                            picked, n_slots);
        return;
    }

    sds key = sdscatfmt(sdsempty(), "%s:%i", host, port);
    rdmaOutboundLink *L = dictFetchValue(server.rdma_outbound_links, key);
    sdsfree(key);
    if (L == NULL) {
        zfree(chosen);
        addReplyError(c, "no outbound link cached; call RDMA MIGRATE-PREP first");
        return;
    }

    pthread_mutex_lock(&L->mu);

    /* Get the recipient's node id via CLUSTER MYID over the ctrl channel.
     * CLUSTER MYID replies with a bulk string (REDIS_REPLY_STRING). */
    redisReply *r = redisCommand(L->ctrl, "CLUSTER MYID");
    if (r == NULL || r->type != REDIS_REPLY_STRING) {
        pthread_mutex_unlock(&L->mu);
        zfree(chosen);
        addReplyErrorFormat(c,
            "RESHARD-FLIP: CLUSTER MYID failed (type=%d, str=%s)",
            r ? r->type : -1, r ? r->str : "(null)");
        if (r) freeReplyObject(r);
        return;
    }
    /* CLUSTER NAMELEN-byte hex node id. */
    char recipient_id[CLUSTER_NAMELEN + 1];
    if ((int) strlen(r->str) < CLUSTER_NAMELEN) {
        pthread_mutex_unlock(&L->mu);
        zfree(chosen);
        addReplyErrorFormat(c, "RESHARD-FLIP: bad recipient node id: %s", r->str);
        freeReplyObject(r);
        return;
    }
    memcpy(recipient_id, r->str, CLUSTER_NAMELEN);
    recipient_id[CLUSTER_NAMELEN] = '\0';
    freeReplyObject(r);

    /* Look up the local clusterNode * for the recipient. In vanilla cluster
     * mode the recipient is known to us via cluster gossip (it joined via
     * CLUSTER MEET). In AqRaft mode (server.cluster == NULL on the donor),
     * we skip the lookup — the recipient is identified by its host:port
     * endpoint (already in L->host / L->port) for the control plane, and
     * by recipient_id for the RECV-FLIP RPC. The local slot table flip
     * (step 3) is also skipped in AqRaft mode. */
    clusterNode *recipient_node = NULL;
    if (server.cluster != NULL) {
        recipient_node = clusterLookupNode(recipient_id, CLUSTER_NAMELEN);
        if (recipient_node == NULL) {
            pthread_mutex_unlock(&L->mu);
            zfree(chosen);
            addReplyErrorFormat(c,
                "RESHARD-FLIP: recipient %s not in local cluster view "
                "(have you done CLUSTER MEET?)", recipient_id);
            return;
        }
    }

    /* Our own (source's) node id — recipient uses it for SETSLOT IMPORTING.
     * In AqRaft mode synthesize via rdmaMigrationSelfName(). */
    char src_id[CLUSTER_NAMELEN + 1];
    rdmaMigrationSelfName(src_id);

    int flipped = 0;

    /* 1. Lock source blocks for all N slots up-front. */
    for (int i = 0; i < n_slots; i++) {
        r_allocator_lock_slot_blocks(chosen[i]);
    }

    /* 2. Tell recipient to flip its half in a single RDMA RESHARD-RECV-FLIP
     *    call — bypasses the SETSLOT IMPORTING/NODE safety checks that
     *    would otherwise refuse (owner + importing-from) simultaneous
     *    state. This sets importing_slots_from[slot]=<source> AND claims
     *    ownership on the recipient, atomically per slot. */
    {
        /* Build argv: ["RDMA", "RESHARD-RECV-FLIP", <src_id>, <slot1>, ...]. */
        int argc = 3 + n_slots;
        const char **argv = zmalloc((size_t) argc * sizeof(*argv));
        size_t *argvlen = zmalloc((size_t) argc * sizeof(*argvlen));
        argv[0] = "RDMA";               argvlen[0] = 4;
        argv[1] = "RESHARD-RECV-FLIP";  argvlen[1] = 17;
        argv[2] = src_id;               argvlen[2] = CLUSTER_NAMELEN;
        char (*numbuf)[16] = zmalloc((size_t) n_slots * sizeof(*numbuf));
        for (int i = 0; i < n_slots; i++) {
            argvlen[3 + i] = (size_t) snprintf(numbuf[i], 16, "%d", chosen[i]);
            argv  [3 + i] = numbuf[i];
        }
        redisReply *r_recv = redisCommandArgv(L->ctrl, argc, argv, argvlen);
        zfree(argv);
        zfree(argvlen);
        zfree(numbuf);

        if (r_recv == NULL || r_recv->type == REDIS_REPLY_ERROR) {
            pthread_mutex_unlock(&L->mu);
            zfree(chosen);
            addReplyErrorFormat(c,
                "RESHARD-FLIP: recipient RECV-FLIP failed: %s",
                r_recv ? r_recv->str : "no reply");
            if (r_recv) freeReplyObject(r_recv);
            return;
        }
        freeReplyObject(r_recv);
    }

    /* 3. Locally flip ownership for all N slots: clusterDelSlot +
     *    clusterAddSlot bypasses the SETSLOT NODE safety check that
     *    refuses to release a slot while local keys exist. Also set
     *    migrating_slots_to[slot] = recipient_node so the patched
     *    getNodeByQuery on the source recognises v2-active slots and
     *    accepts ASKING reads for them while we still hold the locked
     *    snapshot.
     *
     *    Multi-slot mutation; topology wrlock for the whole loop.
     *
     *    AqRaft note: when server.cluster is NULL (RedisRaft mode), the
     *    slot ownership lives in redisraft's slot-config, not in the
     *    legacy cluster tables. Skip the local flip — redisraft on the
     *    donor will detect the migration via RAFT.SHARDGROUP events and
     *    update its slot map separately. Routing during the migration
     *    window depends on clients honoring MOVED responses correctly
     *    (no ASKING redirect — degraded behavior, acceptable for the
     *    AqRaft phase-1 integration). */
    clusterTopoLockWrite();
    for (int i = 0; i < n_slots; i++) {
        int slot = chosen[i];
        if (server.cluster != NULL) {
            clusterDelSlot(slot);
            clusterAddSlot(recipient_node, slot);
            server.cluster->migrating_slots_to[slot] = recipient_node;
        }
        flipped++;
        serverLog(LL_NOTICE,
            "RDMA RESHARD-FLIP: slot=%d ownership flipped to %s "
            "(%s)",
            slot, recipient_id,
            server.cluster != NULL ? "source locked, migrating_slots_to set"
                                   : "AqRaft mode: legacy cluster tables skipped");
    }
    clusterTopoUnlock();

    pthread_mutex_unlock(&L->mu);

    int first = chosen[0], last = chosen[n_slots - 1];
    zfree(chosen);
    addReplyStatusFormat(c,
        "OK %d slots flipped (slots %d..%d, flipped %d)",
        n_slots, first, last, flipped);
}


/* ====================================================================== *
 *  RDMA RESHARD-RECV-FLIP  (recipient side, called from source's FLIP)
 *
 *      RDMA RESHARD-RECV-FLIP <source-node-id> <slot> [<slot> ...]
 *
 *  Replaces the SETSLOT IMPORTING + SETSLOT NODE pair the source would
 *  otherwise issue via L->ctrl. Reason: standard CLUSTER SETSLOT NODE on
 *  the recipient clears importing_slots_from[], and re-setting IMPORTING
 *  afterwards is refused with "I'm already the owner" — so the recipient
 *  ends up without the IMPORTING marker that the v2 -ASK-on-miss
 *  predicate in getNodeByQuery() needs.
 *
 *  This command sets state directly, atomically per slot:
 *    1. importing_slots_from[slot] = <source-node>
 *    2. clusterDelSlot(slot) + clusterAddSlot(myself, slot)
 *
 *  Bypasses the standard SETSLOT safety checks because the v2 protocol
 *  explicitly wants the recipient to be in (owns slot + importing-from
 *  source) state — exactly what those checks normally forbid.
 *
 *  Reply: +OK <n> slots flipped on recipient
 * ====================================================================== */

void rdmaReshardRecvFlipCommand(client *c) {
    if (c->argc < 4) {
        addReplyError(c,
            "syntax: RDMA RESHARD-RECV-FLIP source-node-id slot [slot ...]");
        return;
    }
    const char *src_id_str = c->argv[2]->ptr;
    if ((int) strlen(src_id_str) < CLUSTER_NAMELEN) {
        addReplyErrorFormat(c, "RESHARD-RECV-FLIP: bad source node id: %s",
                            src_id_str);
        return;
    }

    /* Recipient mode:
     *   - server.cluster != NULL: vanilla cluster recipient. Take slot
     *     ownership via the legacy cluster tables (clusterDel/AddSlot).
     *   - server.cluster == NULL but rdma-migration-redisraft-mode: AqRaft
     *     redisraft recipient. Slot ownership lives in raft.slot-config
     *     (sg4 owns 0:16383 already); cluster tables don't exist. Skip
     *     the cluster-table mutations and only update the aqueduct
     *     slot-state machine below.
     *   - Neither: error. */
    int aqraft_recipient = (server.cluster == NULL);
    if (aqraft_recipient && !(server.rdma_migration_redisraft_mode &&
                              server.rdma_migration_redisraft_slots != NULL &&
                              sdslen(server.rdma_migration_redisraft_slots) > 0)) {
        addReplyError(c, "cluster mode not enabled on this node");
        return;
    }

    /* In AqRaft, the source is a redisraft donor whose synthesized node id
     * ("AQRAFT_…") is not in this recipient's gossip — clusterLookupNode
     * returns NULL. Two cases:
     *   - src_node != NULL: vanilla→vanilla migration, set the importing_from
     *     marker so the v2 -ASK-on-miss predicate in getNodeByQuery() works.
     *   - src_node == NULL: skip importing_slots_from[] (the ASK-back
     *     redirect for in-flight reads of moved keys is the only thing lost;
     *     misses on the recipient just fall through, and the chain delivers
     *     post-FLIP keys to followers via the chain code separately). */
    clusterNode *src_node = NULL;
    if (!aqraft_recipient) {
        src_node = clusterLookupNode(src_id_str, CLUSTER_NAMELEN);
    }
    int aqraft_src = (src_node == NULL);
    if (aqraft_src && !aqraft_recipient) {
        serverLog(LL_NOTICE,
            "RDMA RESHARD-RECV-FLIP: source %s not in local cluster gossip — "
            "treating as AqRaft donor (skipping importing_slots_from tracking)",
            src_id_str);
    }

    int flipped = 0;
    /* Multi-slot mutation across user-supplied slot list. Topology wrlock
     * for the whole loop; the early-return on bad-slot must drop the lock
     * (pre-existing bug: slots already flipped before the bad one stay
     * flipped — not addressed here). AqRaft recipient: no cluster table,
     * no lock needed. */
    if (!aqraft_recipient) clusterTopoLockWrite();
    for (int j = 3; j < c->argc; j++) {
        long long slot_ll;
        if (getLongLongFromObject(c->argv[j], &slot_ll) != C_OK ||
            slot_ll < 0 || slot_ll >= CLUSTER_SLOTS) {
            if (!aqraft_recipient) clusterTopoUnlock();
            addReplyErrorFormat(c, "RESHARD-RECV-FLIP: bad slot %s",
                                (char *) c->argv[j]->ptr);
            return;
        }
        int slot = (int) slot_ll;

        /* 1. Set importing marker first — the v2 -ASK-on-miss predicate
         *    in getNodeByQuery() keys off getImportingSlotSource(slot).
         *    Skipped in AqRaft mode (src_node == NULL OR recipient is AqRaft).
         * 2. Take ownership of the slot.
         * Both skipped for AqRaft recipient — redisraft already owns this. */
        if (!aqraft_recipient) {
            if (!aqraft_src) {
                server.cluster->importing_slots_from[slot] = src_node;
            }
            clusterDelSlot(slot);
            clusterAddSlot(server.cluster->myself, slot);
        }

        flipped++;

        serverLog(LL_NOTICE,
            "RDMA RESHARD-RECV-FLIP: slot=%d imported from %s, ownership claimed%s",
            slot, src_id_str,
            aqraft_recipient ? " (AqRaft recipient: raft.slot-config owns it)"
                             : (aqraft_src ? " (AqRaft: no ASK fallback)" : ""));
    }
    if (!aqraft_recipient) clusterTopoUnlock();

    /* Aqueduct slot-state: recipient now owns the slots cleanly, clear
     * MIGRATING and return to STABLE. (Originally we tried deferring this
     * to BACKPATCH_DONE; that exposed a concurrent-mutation issue between
     * the backpatch worker and main-thread Path B wraps, so we keep the
     * transition here at RECV-FLIP and rely on donor's STABLE-at-DONE to
     * shift traffic to the recipient after migration completes.) */
    for (int j = 3; j < c->argc; j++) {
        long long slot_ll;
        if (getLongLongFromObject(c->argv[j], &slot_ll) != C_OK) continue;
        if (slot_ll < 0 || slot_ll >= CLUSTER_SLOTS) continue;
        slotMigStateSet((int) slot_ll, SLOT_STATE_STABLE, NULL);
    }

    /* Under the early-FLIP ordering, RECV-FLIP fires BEFORE backpatch has even
     * started — so the databasesCron gate (recipient_backpatch_in_progress) is
     * NOT decremented here. The decrement now lives at the BACKPATCH_DONE
     * transition sites (chunked-fallback inline, pool-worker path), so the
     * gate stays held until backpatch genuinely finishes. */

    addReplyStatusFormat(c, "OK %d slots flipped on recipient", flipped);
}


/* ======================================================================== *
 *                                                                          *
 *  RDMA MIGRATE — single autonomous-thread migration                       *
 *                                                                          *
 *  Replaces the four-command Ansible-driven sequence (MIGRATE-PREP →       *
 *  RESHARD → RESHARD-FLIP → RESHARD-TRANSFER) with a single command that       *
 *  spawns a detached worker thread to walk the full pipeline off the       *
 *  event loop.                                                             *
 *                                                                          *
 *      RDMA MIGRATE  <recipient-host> <recipient-port> <n-slots>           *
 *      RDMA MIGRATE-STATUS [migration_id]                                  *
 *                                                                          *
 *  Lock ordering inside the worker:                                        *
 *      mig->mu  <  L->mu  <  cluster_topology_lock  <  slot_locks[S]       *
 *                                                                          *
 *  Stale legacy commands (MIGRATE-PREP / RESHARD / RESHARD-FLIP /          *
 *  RESHARD-TRANSFER) remain available, calling shared helpers below.           *
 * ======================================================================== */

#include "cluster_legacy.h"  /* for clusterState slot_locks + clusterAddSlot/Del proto */

/* Phase 4d: TLS flag — see cluster.h. Default 0 on every thread, set to 1
 * by recipientBackpatchThreadMain while it holds clusterSlotLockWrite(s). */
__thread int cluster_slot_lock_held_by_thread = 0;

/* ---- helpers (shared by the legacy command handlers above and the new
 *      autonomous worker below) ---------------------------------------- */

/* ====================================================================== *
 *  Phase 4d callback-RPC: source-side pendingRegistration table.
 *
 *  Source's PREP step sends `RDMA REGISTER-BLOCK-SLOTS <register_id> ...`
 *  to the recipient, then waits (on a condvar) for the recipient to dial
 *  back with `RDMA REGISTER-RESULT <register_id> <status> <payload>`.
 *  Source's main thread receives that RPC (rdmaRegisterResultCommand),
 *  looks up the table entry by register_id, copies the payload, and
 *  signals the condvar. The PREP-thread wakes, parses the payload into
 *  L->buffers[], frees the entry. */
typedef struct pendingRegistration {
    int               done;
    int               error;
    sds               err_msg;          /* set when error==1 */
    rdmaRemoteBufferInfo *payload;      /* len total_buffers (worker fills) */
    int               total_buffers;
    pthread_mutex_t   mu;
    pthread_cond_t    cond;
} pendingRegistration;

/* Custom dictType for pending_registrations: sds keys (we free them via
 * sdsfree on remove), opaque pointer values (caller owns the lifetime —
 * dict NEVER auto-frees them). Using a stock dictType with dictVanillaFree
 * as the val destructor would cause a use-after-free when dictDelete frees
 * `p` while the worker thread still holds a pointer to it. */
/* Prototypes: live in server.h but include guard ordering means we declare
 * them locally to keep this section self-contained. */
uint64_t dictSdsCaseHash(const void *key);
int dictSdsKeyCaseCompare(dictCmpCache *cache, const void *key1, const void *key2);
void dictSdsDestructor(dict *d, void *val);

static dictType pendingRegistrationsDictType = {
    dictSdsCaseHash,
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,
    dictSdsDestructor,          /* key destructor frees the sds */
    NULL,                       /* val destructor — caller frees p */
    NULL
};

static dict *pending_registrations = NULL;
static pthread_mutex_t pending_registrations_mu = PTHREAD_MUTEX_INITIALIZER;
static _Atomic long long register_id_counter = 0;

static void pendingRegistrationsInit(void) {
    if (pending_registrations == NULL) {
        pending_registrations = dictCreate(&pendingRegistrationsDictType);
    }
}

/* Source-side: receive the recipient's callback delivery of the registration
 * result. Looks up the pending entry by register_id, copies payload, signals
 * the waiting PREP-helper. */
void rdmaRegisterResultCommand(client *c) {
    if (c->argc != 5) {
        addReplyError(c,
            "syntax: RDMA REGISTER-RESULT register_id status payload");
        return;
    }
    sds reg_id = c->argv[2]->ptr;
    sds status = c->argv[3]->ptr;
    robj *payload_obj = c->argv[4];
    size_t payload_len = sdslen(payload_obj->ptr);

    pendingRegistrationsInit();

    pthread_mutex_lock(&pending_registrations_mu);
    pendingRegistration *p = dictFetchValue(pending_registrations, reg_id);
    pthread_mutex_unlock(&pending_registrations_mu);
    if (p == NULL) {
        addReplyErrorFormat(c, "unknown register_id: %s", reg_id);
        return;
    }

    pthread_mutex_lock(&p->mu);
    if (strcmp(status, "OK") == 0) {
        if (payload_len % sizeof(rdmaRemoteBufferInfo) != 0) {
            p->error = 1;
            p->err_msg = sdscatfmt(sdsempty(),
                "REGISTER-RESULT payload len %U not a multiple of tuple size %U",
                (uint64_t) payload_len, (uint64_t) sizeof(rdmaRemoteBufferInfo));
        } else {
            p->total_buffers = (int) (payload_len / sizeof(rdmaRemoteBufferInfo));
            p->payload = zmalloc(payload_len);
            memcpy(p->payload, payload_obj->ptr, payload_len);
        }
    } else {
        /* Recipient reported an error. */
        p->error = 1;
        p->err_msg = sdsnew(status);
    }
    p->done = 1;
    pthread_cond_signal(&p->cond);
    pthread_mutex_unlock(&p->mu);

    serverLog(LL_NOTICE,
        "RDMA REGISTER-RESULT: register_id=%s received (%zu bytes, status=%s)",
        reg_id, payload_len, status);
    addReply(c, shared.ok);
}

/* PREP-helper: REGISTER-BLOCK-SLOTS RPC over L->ctrl, fills L->buffers[slot].
 * Phase 4d callback-RPC variant: the recipient replies +OK immediately to
 * the REGISTER-BLOCK-SLOTS RPC and dials back later with REGISTER-RESULT.
 * This helper allocates a pendingRegistration, sends the request with
 * call-back coordinates, then condvar-waits for the result. The waiting
 * happens on the migration worker thread — source's main event loop stays
 * free to serve YCSB throughout. */
static int rdmaMigratePrepHelper(rdmaOutboundLink *L,
                                  const int *slots, int n_slots,
                                  sds *err_out) {
    pendingRegistrationsInit();

    /* Build a unique register_id for the callback-RPC correlation. */
    long long counter = atomic_fetch_add_explicit(&register_id_counter, 1,
                                                  memory_order_relaxed);
    char register_id[64];
    snprintf(register_id, sizeof(register_id), "%d-%lld",
             (int) getpid(), counter);

    /* Allocate the pending entry + register it. */
    pendingRegistration *p = zmalloc(sizeof(*p));
    p->done = 0;
    p->error = 0;
    p->err_msg = NULL;
    p->payload = NULL;
    p->total_buffers = 0;
    pthread_mutex_init(&p->mu, NULL);
    pthread_cond_init(&p->cond, NULL);

    sds key = sdsnew(register_id);
    pthread_mutex_lock(&pending_registrations_mu);
    dictReplace(pending_registrations, key, p);
    pthread_mutex_unlock(&pending_registrations_mu);

    /* Figure out which host:port the recipient should call back.
     * Priority order:
     *   1. server.cluster->myself->ip (vanilla cluster)
     *   2. gethostname() (AqRaft mode where server.cluster is NULL — hostname
     *      resolves to the donor's actual address on cloudlab/LAN setups).
     *   3. "127.0.0.1" last-resort fallback (forces recipient to dial itself,
     *      which is the bug that breaks REGISTER-RESULT in AqRaft mode). */
    char src_host_buf[256];
    const char *src_host;
    if (server.cluster && server.cluster->myself &&
        server.cluster->myself->ip[0]) {
        src_host = server.cluster->myself->ip;
    } else if (gethostname(src_host_buf, sizeof(src_host_buf)) == 0) {
        src_host_buf[sizeof(src_host_buf) - 1] = '\0';
        src_host = src_host_buf;
    } else {
        src_host = "127.0.0.1";
    }
    int src_port = (int) server.port;

    pthread_mutex_lock(&L->mu);

    /* Build the wire command:
     *   RDMA REGISTER-BLOCK-SLOTS <register_id> <src-host> <src-port>
     *                             SLOTS <slot> <nblocks> ... */
    int argc = 6 + 2 * n_slots;
    const char **argv = zmalloc((size_t) argc * sizeof(*argv));
    size_t *argvlen = zmalloc((size_t) argc * sizeof(*argvlen));
    char src_port_buf[16];
    snprintf(src_port_buf, sizeof(src_port_buf), "%d", src_port);
    argv[0] = "RDMA";                       argvlen[0] = 4;
    argv[1] = "REGISTER-BLOCK-SLOTS";       argvlen[1] = 20;
    argv[2] = register_id;                  argvlen[2] = strlen(register_id);
    argv[3] = src_host;                     argvlen[3] = strlen(src_host);
    argv[4] = src_port_buf;                 argvlen[4] = strlen(src_port_buf);
    argv[5] = "SLOTS";                      argvlen[5] = 5;

    char (*numbuf)[16] = zmalloc((size_t)(2 * n_slots) * sizeof(*numbuf));
    for (int i = 0; i < n_slots; i++) {
        argvlen[6 + 2*i]     = (size_t) snprintf(numbuf[2*i],     16, "%d", slots[i]);
        argv  [6 + 2*i]      = numbuf[2*i];
        argvlen[6 + 2*i + 1] = (size_t) snprintf(numbuf[2*i + 1], 16, "%d", 1);
        argv  [6 + 2*i + 1]  = numbuf[2*i + 1];
    }
    redisReply *r = redisCommandArgv(L->ctrl, argc, argv, argvlen);
    zfree(argv);
    zfree(argvlen);
    zfree(numbuf);

    if (r == NULL || r->type != REDIS_REPLY_STATUS) {
        const char *type_name =
            r == NULL                           ? "NULL"    :
            r->type == REDIS_REPLY_ERROR        ? "ERROR"   :
                                                   "other";
        const char *body = (r && r->str) ? r->str : "(no body)";
        const char *ctxerr = (L->ctrl && L->ctrl->errstr[0]) ? L->ctrl->errstr : "(no ctxerr)";
        if (err_out) *err_out = sdscatfmt(sdsempty(),
            "REGISTER-BLOCK-SLOTS dispatch failed (type=%s body=\"%s\" ctxerr=\"%s\")",
            type_name, body, ctxerr);
        if (r) freeReplyObject(r);
        pthread_mutex_unlock(&L->mu);
        /* Drop the pending entry. */
        pthread_mutex_lock(&pending_registrations_mu);
        dictDelete(pending_registrations, key);
        pthread_mutex_unlock(&pending_registrations_mu);
        pthread_mutex_destroy(&p->mu);
        pthread_cond_destroy(&p->cond);
        zfree(p);
        return -1;
    }
    freeReplyObject(r);
    pthread_mutex_unlock(&L->mu);

    /* Wait on the condvar — release the main thread fully; only this worker
     * thread sleeps. The recipient's worker will dial back to our main port
     * via RDMA REGISTER-RESULT, which signals p->cond from
     * rdmaRegisterResultCommand. */
    serverLog(LL_NOTICE,
        "RDMA REGISTER-BLOCK-SLOTS: dispatched register_id=%s, waiting for callback...",
        register_id);
    pthread_mutex_lock(&p->mu);
    while (!p->done) pthread_cond_wait(&p->cond, &p->mu);
    int got_error = p->error;
    sds err_copy = p->err_msg ? sdsdup(p->err_msg) : NULL;
    int total = p->total_buffers;
    rdmaRemoteBufferInfo *buf_copy = NULL;
    if (!got_error && p->payload) {
        size_t bytes = (size_t) total * sizeof(rdmaRemoteBufferInfo);
        buf_copy = zmalloc(bytes);
        memcpy(buf_copy, p->payload, bytes);
    }
    pthread_mutex_unlock(&p->mu);

    /* Remove from the dict first (key is freed by dictDelete via the
     * key destructor; val destructor is NULL so `p` is NOT touched). */
    pthread_mutex_lock(&pending_registrations_mu);
    dictDelete(pending_registrations, key);
    pthread_mutex_unlock(&pending_registrations_mu);

    /* Now safely free p's owned memory + p itself. */
    if (p->payload) zfree(p->payload);
    if (p->err_msg) sdsfree(p->err_msg);
    pthread_mutex_destroy(&p->mu);
    pthread_cond_destroy(&p->cond);
    zfree(p);

    if (got_error) {
        if (err_out) *err_out = err_copy ? err_copy : sdsnew("REGISTER-RESULT: error");
        if (buf_copy) zfree(buf_copy);
        return -1;
    }
    if (total != n_slots) {
        if (err_out) *err_out = sdscatfmt(sdsempty(),
            "REGISTER-RESULT: got %i tuples, expected %i", total, n_slots);
        if (buf_copy) zfree(buf_copy);
        if (err_copy) sdsfree(err_copy);
        return -1;
    }

    /* Populate L->buffers[slot]. */
    pthread_mutex_lock(&L->mu);
    for (int i = 0; i < n_slots; i++) {
        L->buffers[slots[i]] = buf_copy[i];
    }
    pthread_mutex_unlock(&L->mu);
    zfree(buf_copy);
    if (err_copy) sdsfree(err_copy);
    return 0;
}

/* REGISTER-helper: the hot ibv_reg_mr loop. Mirrors rdmaReshardCommand's
 * inline body (lines ~883-923). `progress` is bumped under L->mu each time
 * a slot is registered, so RDMA MIGRATE-STATUS can read progress without
 * waiting for the worker to finish. */
static int rdmaReshardRegisterHelper(rdmaOutboundLink *L,
                                      const int *chosen, int n_slots,
                                      int *progress, sds *err_out) {
    pthread_mutex_lock(&L->mu);

    /* AqRaft Stage 5 (donor big-MR): the old path did one zmalloc(2MiB) + one
     * ibv_reg_mr PER SLOT — ~23ms/slot, ~16s for 683 slots, which the
     * critical-path trace identified as THE dominant migration cost. Instead,
     * register ONE contiguous pool with a SINGLE ibv_reg_mr the first time this
     * link needs source buffers, and hand each slot a lightweight VIEW into it
     * (shares the one MR's lkey; RDMA permits any VA inside the registered
     * range). The pool persists on the link and is reused across rounds, so
     * round 2+ pay zero registration. Mirrors the recipient's big-MR PREP
     * ("replaces N ibv_reg_mr ioctls with 1"). */
    if (L->src_mr_pool == NULL) {
        size_t stride   = (size_t) RDMAMIG_BLOCK_SIZE_BYTES;
        /* Size the pool for the whole keyspace band this link can migrate so it
         * never needs re-registration across rounds. n_slots for the first
         * round is the migrating band; round up to CLUSTER_SLOTS would be huge,
         * so size to the first call's n_slots with modest headroom (x2, capped
         * at CLUSTER_SLOTS). */
        int cap_blocks  = n_slots * 2;
        if (cap_blocks > CLUSTER_SLOTS) cap_blocks = CLUSTER_SLOTS;
        if (cap_blocks < n_slots)       cap_blocks = n_slots;
        size_t pool_bytes = (size_t) cap_blocks * stride;
        void *pool = mmap(NULL, pool_bytes, PROT_READ | PROT_WRITE,
                          MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
        if (pool == MAP_FAILED) {
            pthread_mutex_unlock(&L->mu);
            if (err_out) *err_out = sdscatprintf(sdsempty(),
                "mmap(%zu) for donor big-MR pool failed: %s",
                pool_bytes, strerror(errno));
            return -1;
        }
        struct rdmamig_buffer *parent = rdmamig_buffer_create(
            rdmamig_client_cm_id(L->client), (char *) pool, pool_bytes, 0);
        if (parent == NULL) {
            munmap(pool, pool_bytes);
            pthread_mutex_unlock(&L->mu);
            if (err_out) *err_out = sdsnew("ibv_reg_mr for donor big-MR pool failed");
            return -1;
        }
        L->src_mr_pool       = pool;
        L->src_mr_pool_bytes = pool_bytes;
        L->src_mr_parent     = parent;
        L->src_mr_used_blocks = 0;
        serverLog(LL_NOTICE,
            "RDMA RESHARD: donor big-MR pool base=%p bytes=%zu blocks=%d "
            "rkey=0x%x (1 ibv_reg_mr replaces per-slot registration) "
            "[AqRaft Stage 5]",
            pool, pool_bytes, cap_blocks, rdmamig_buffer_rkey(parent));
    }

    int registered = 0;
    for (int i = 0; i < n_slots; i++) {
        int slot = chosen[i];
        if (L->source_buffers[slot] != NULL) {
            /* already registered (e.g. a prior round) — count as progress. */
            if (progress) (*progress)++;
            continue;
        }
        struct rdmamig_buffer *rb;
        int pool_blocks = (int) (L->src_mr_pool_bytes / (size_t) RDMAMIG_BLOCK_SIZE_BYTES);
        if (L->src_mr_used_blocks < pool_blocks) {
            /* Carve the next pool block as a view (no ibv_reg_mr). */
            char *sub = (char *) L->src_mr_pool
                      + (size_t) L->src_mr_used_blocks * (size_t) RDMAMIG_BLOCK_SIZE_BYTES;
            memset(sub, 0, RDMAMIG_BLOCK_SIZE_BYTES);
            rb = rdmamig_buffer_create_view(L->src_mr_parent, sub,
                                            RDMAMIG_BLOCK_SIZE_BYTES);
            if (rb != NULL) L->src_mr_used_blocks++;
        } else {
            /* Pool exhausted (more slots than headroom) — fall back to a
             * standalone per-slot registration. Rare; keeps correctness. */
            void *staging = zmalloc(RDMAMIG_BLOCK_SIZE_BYTES);
            if (staging == NULL) {
                pthread_mutex_unlock(&L->mu);
                if (err_out) *err_out = sdsnew("zmalloc failed for staging buffer");
                return -1;
            }
            memset(staging, 0, RDMAMIG_BLOCK_SIZE_BYTES);
            rb = rdmamig_buffer_create(rdmamig_client_cm_id(L->client),
                                       staging, RDMAMIG_BLOCK_SIZE_BYTES, 0);
            if (rb == NULL) zfree(staging);
        }
        if (rb == NULL) {
            pthread_mutex_unlock(&L->mu);
            if (err_out) *err_out = sdsnew("source buffer registration failed");
            return -1;
        }
        L->source_buffers[slot] = rb;
        registered++;
        if (progress) (*progress)++;
    }
    /* One summary line instead of 683 per-slot lines (which themselves added
     * serverLog overhead to the hot loop). */
    serverLog(LL_NOTICE,
        "RDMA RESHARD: registered %d new source slots as big-MR views "
        "(pool used=%d/%zu blocks) [AqRaft Stage 5]",
        registered, L->src_mr_used_blocks,
        L->src_mr_pool_bytes / (size_t) RDMAMIG_BLOCK_SIZE_BYTES);
    pthread_mutex_unlock(&L->mu);
    return 0;
}

/* FLIP-helper: hiredis RECV-FLIP RPC + local slot-table mutation under the
 * topology wrlock. Mirrors rdmaReshardFlipCommand (lines ~1216-1322). */
static int rdmaReshardFlipHelper(rdmaOutboundLink *L,
                                  const int *chosen, int n_slots,
                                  sds *err_out) {
    pthread_mutex_lock(&L->mu);

    /* Probe the recipient's node id via CLUSTER MYID. On an AqRaft redisraft
     * recipient (cluster_enabled=no), CLUSTER MYID returns ERR Unknown
     * subcommand — we synthesize a placeholder recipient_id since the value
     * is only consumed by clusterLookupNode/clusterAddSlot which we skip
     * when server.cluster is NULL on the donor. The recipient itself doesn't
     * inspect this id either (RESHARD-RECV-FLIP only validates length). */
    redisReply *r = redisCommand(L->ctrl, "CLUSTER MYID");
    char recipient_id[CLUSTER_NAMELEN + 1];
    int recipient_id_synthesized = 0;
    if (r != NULL && r->type == REDIS_REPLY_STRING &&
        (int) strlen(r->str) >= CLUSTER_NAMELEN) {
        memcpy(recipient_id, r->str, CLUSTER_NAMELEN);
        recipient_id[CLUSTER_NAMELEN] = '\0';
    } else if (server.cluster == NULL) {
        /* AqRaft donor + AqRaft recipient: synthesize a fixed-length opaque
         * placeholder; the recipient only validates length, the donor only
         * uses it for clusterLookupNode (skipped under server.cluster==NULL). */
        snprintf(recipient_id, sizeof(recipient_id),
                 "AQRECP_%s", "ppppppppppppppppppppppppppppppppp");
        recipient_id[CLUSTER_NAMELEN] = '\0';
        recipient_id_synthesized = 1;
        serverLog(LL_NOTICE,
            "FLIP: CLUSTER MYID unsupported on AqRaft recipient — using placeholder id");
    } else {
        const char *why = r == NULL ? "hiredis_null"
                        : r->type == REDIS_REPLY_ERROR ? "redis_error"
                        : r->type == REDIS_REPLY_NIL   ? "nil_reply"
                        : "short_or_wrong_type";
        const char *body = (r != NULL && r->str != NULL) ? r->str : "(no body)";
        const char *ctxerr = (L->ctrl != NULL && L->ctrl->errstr[0]) ? L->ctrl->errstr : "(no ctx err)";
        if (err_out) *err_out = sdscatfmt(sdsempty(),
            "FLIP: CLUSTER MYID failed (why=%s rtype=%i body='%s' ctxerr='%s')",
            why, r ? r->type : -1, body, ctxerr);
        if (r) freeReplyObject(r);
        pthread_mutex_unlock(&L->mu);
        return -1;
    }
    if (r) freeReplyObject(r);
    (void) recipient_id_synthesized;

    /* recipient_node only needed for the local clusterDelSlot/AddSlot flip;
     * in AqRaft mode (server.cluster == NULL) we skip that step entirely
     * and identify the recipient by host:port + recipient_id for RPCs. */
    clusterNode *recipient_node = NULL;
    if (server.cluster != NULL) {
        recipient_node = clusterLookupNode(recipient_id, CLUSTER_NAMELEN);
        if (recipient_node == NULL) {
            if (err_out) *err_out = sdscatfmt(sdsempty(),
                "FLIP: recipient %s not in local cluster view (CLUSTER MEET?)",
                recipient_id);
            pthread_mutex_unlock(&L->mu);
            return -1;
        }
    }

    char src_id[CLUSTER_NAMELEN + 1];
    rdmaMigrationSelfName(src_id);

    /* Lock source blocks for all N slots up front. */
    for (int i = 0; i < n_slots; i++) {
        r_allocator_lock_slot_blocks(chosen[i]);
    }

    /* Recipient flips its half atomically per slot. */
    int argc = 3 + n_slots;
    const char **argv = zmalloc((size_t) argc * sizeof(*argv));
    size_t *argvlen = zmalloc((size_t) argc * sizeof(*argvlen));
    argv[0] = "RDMA";               argvlen[0] = 4;
    argv[1] = "RESHARD-RECV-FLIP";  argvlen[1] = 17;
    argv[2] = src_id;               argvlen[2] = CLUSTER_NAMELEN;
    char (*numbuf)[16] = zmalloc((size_t) n_slots * sizeof(*numbuf));
    for (int i = 0; i < n_slots; i++) {
        argvlen[3 + i] = (size_t) snprintf(numbuf[i], 16, "%d", chosen[i]);
        argv  [3 + i] = numbuf[i];
    }
    redisReply *r_recv = redisCommandArgv(L->ctrl, argc, argv, argvlen);
    zfree(argv);
    zfree(argvlen);
    zfree(numbuf);

    if (r_recv == NULL || r_recv->type == REDIS_REPLY_ERROR) {
        if (err_out) *err_out = sdscatfmt(sdsempty(),
            "FLIP: recipient RECV-FLIP failed: %s",
            r_recv ? r_recv->str : "no reply");
        if (r_recv) freeReplyObject(r_recv);
        pthread_mutex_unlock(&L->mu);
        return -1;
    }
    freeReplyObject(r_recv);

    /* Local slot-table mutation under the topology write lock — multi-slot
     * mutation, so we take topology-wrlock (excludes all readers + per-slot
     * writers) instead of per-slot wrlocks. Short critical section
     * (microseconds per slot); event-loop readers stall for the loop. */
    clusterTopoLockWrite();
    for (int i = 0; i < n_slots; i++) {
        int slot = chosen[i];
        if (server.cluster != NULL) {
            clusterDelSlot(slot);
            clusterAddSlot(recipient_node, slot);
            server.cluster->migrating_slots_to[slot] = recipient_node;
        }
        serverLog(LL_NOTICE,
            "RDMA MIGRATE worker: slot=%d ownership flipped to %s%s",
            slot, recipient_id,
            server.cluster != NULL ? "" : " (AqRaft: legacy cluster tables skipped)");
    }
    clusterTopoUnlock();

    pthread_mutex_unlock(&L->mu);
    return 0;
}

/* EXEC-helper: encode + RDMA-WRITE + DONE-SLOTS. Mirrors reshardTransferWorker
 * (lines ~974-1048).
 *
 * Two code paths, gated by server.rdma_transfer_overlap:
 *   off (default): single end-of-TRANSFER `RDMA DONE-SLOTS` RPC — recipient
 *                  backpatch starts only AFTER all RDMA-WRITEs complete.
 *                  Byte-identical to pre-Aqueduct-overlap behavior.
 *   on:            send `RDMA DONE-SLOTS-INIT` synchronously up front, then
 *                  pipeline `RDMA DONE-SLOTS-CHUNK` RPCs every K slots so the
 *                  recipient backpatch worker pool starts applying as soon as
 *                  the first chunk lands — hiding most of BACKPATCH inside
 *                  TRANSFER's RDMA-WRITE tail.
 *
 * RDMA RC mode invariant: `rdmamig_client_wait_send` returns only after the
 * recipient's NIC acks the write → by the time we proceed past wait_send, the
 * slot's data is durable in the recipient's pool. Chunked CHUNK RPCs sent
 * AFTER each chunk's wait_sends are race-free for the recipient's backpatch
 * pool worker to consume. */
static int rdmaReshardTransferHelper(rdmaOutboundLink *L, redisDb *db,
                                  const int *chosen, int n_slots,
                                  long long mig_id, sds *err_out) {
    pthread_mutex_lock(&L->mu);

    /* Cache the (src_id, mig_id) string forms once — used by both code paths,
     * and (for overlap) by every CHUNK RPC. MUST match what the BACKPATCH-STATUS
     * poller in migrationWorker uses (rdmaMigrationSelfName) so the recipient's
     * batch dict key — built from this src_id on DONE-SLOTS-INIT — matches the
     * key the donor later polls under. */
    char src_id[CLUSTER_NAMELEN + 1];
    rdmaMigrationSelfName(src_id);
    char migid_buf[24];
    int migid_len = snprintf(migid_buf, sizeof(migid_buf), "%lld", mig_id);

    const int overlap = server.rdma_transfer_overlap;
    const int K = (server.rdma_transfer_chunk_slots > 0)
                  ? server.rdma_transfer_chunk_slots : 32;
    int chunk_seq = 0;
    int pending_replies = 0;   /* hiredis pipelined replies we still need to drain (overlap path) */

    /* Overlap: send DONE-SLOTS-INIT synchronously so the recipient's
     * backpatchBatch exists before any CHUNK RPC arrives. TCP ordering on
     * L->ctrl guarantees subsequent CHUNK RPCs land after the INIT reply. */
    if (overlap) {
        char total_buf[16];
        int total_len = snprintf(total_buf, sizeof(total_buf), "%d", n_slots);
        const char *init_argv[5] = {
            "RDMA", "DONE-SLOTS-INIT", src_id, migid_buf, total_buf
        };
        size_t init_argvlen[5] = {
            4, 15, CLUSTER_NAMELEN, (size_t) migid_len, (size_t) total_len
        };
        redisReply *ir = redisCommandArgv(L->ctrl, 5, init_argv, init_argvlen);
        if (ir == NULL || ir->type != REDIS_REPLY_STATUS) {
            const char *body = (ir && ir->str) ? ir->str : "(no body)";
            if (err_out) *err_out = sdscatfmt(sdsempty(),
                "TRANSFER: DONE-SLOTS-INIT failed (%s)", body);
            if (ir) freeReplyObject(ir);
            pthread_mutex_unlock(&L->mu);
            return -1;
        }
        freeReplyObject(ir);
        serverLog(LL_NOTICE,
            "RDMA MIGRATE worker: TRANSFER overlap on (K=%d), DONE-SLOTS-INIT sent",
            K);
    }

    /* Per-chunk accumulators (overlap path). Reused across chunks. */
    int chunk_used = 0;
    int *chunk_slots = overlap ? zmalloc((size_t) K * sizeof(int)) : NULL;

    size_t total_bytes = 0;
    int errs = 0;
    /* AqRaft: number of RDMA WRs posted to the QP but not yet reaped from the
     * send CQ. With pipelining we post up to K WRs before reaping them as a
     * batch — keeping K writes in flight instead of stalling per-slot on
     * wait_send (which capped the transfer at ~1 WR in flight, ~5 Gbps). */
    int inflight = 0;
    for (int i = 0; i < n_slots; i++) {
        int slot = chosen[i];
        char *staging = rdmamig_buffer_data(L->source_buffers[slot]);
        /* Patch 9 (raw-block pass-through TRANSFER): ship the donor's
         * r_allocator block byte-for-byte. The recipient walks it via
         * r_allocator_walk_used_segments + dbAdds each kvobj in place
         * (fixing kv->ptr from data_offset). No encode/decode pass.
         * Falls back to encode if the slot has no r_allocator block yet
         * (shouldn't happen for migrated slots — they were loaded by YCSB
         * which goes through r_allocator). */
        int n_blocks = 0;
        char **donor_blocks = r_allocator_get_block_buffers_for_slot(slot, &n_blocks);
        uint32_t n_entries = 0;  /* Used only by debug log below. */
        if (donor_blocks != NULL && n_blocks > 0 && donor_blocks[0] != NULL) {
            /* Patch 9 limitation: only block[0] is shipped per slot. Slots
             * with n_blocks > 1 (typically slots that grew after the initial
             * insert, e.g. via UPDATE) leak keys in the additional blocks.
             * For workloada/b/c with ~30 keys per slot in 2 MiB blocks this
             * is a ~5% shortfall. Multi-block ship would need n MR posts per
             * slot OR a contiguous donor-side block layout. */
            memcpy(staging, donor_blocks[0], RDMAMIG_BLOCK_SIZE_BYTES);
        } else {
            /* No r_allocator block — fall back to encode. */
            n_entries = rdmaEncodeSlotEntries(db, slot, staging,
                                              RDMAMIG_BLOCK_SIZE_BYTES);
        }
        if (donor_blocks) zfree(donor_blocks);
        rdmaDebugDumpSlotBytes("SRC", slot, staging);
        if (server.rdma_reshard_debug_bytes) {
            r_allocator_log_slot_stats(slot);
            serverLog(LL_NOTICE,
                "RDMA MIGRATE worker TX: slot=%d n_entries=%u rdma_bytes=%d",
                slot, n_entries, RDMAMIG_BLOCK_SIZE_BYTES);
        }
        /* AqRaft (pipelined): post the WR and keep it in flight — do NOT
         * wait_send per slot. rdmamig_client_post_write signals every WR, so
         * we reap the whole batch at the chunk boundary below. Posting up to
         * K (2 MiB) writes before reaping fills the QP send queue and the NIC
         * pipe; the old post-1/wait-1 capped throughput at ~1 WR in flight
         * (~5 Gbps regardless of payload size). */
        int rc = rdmamig_client_post_write(L->source_buffers[slot], staging,
                                           L->buffers[slot].ptr,
                                           L->buffers[slot].rkey,
                                           RDMAMIG_BLOCK_SIZE_BYTES);
        if (rc != 0) {
            errs++;
            serverLog(LL_WARNING,
                "RDMA MIGRATE worker: slot=%d post_write failed rc=%d", slot, rc);
            continue;
        }
        inflight++;
        total_bytes += RDMAMIG_BLOCK_SIZE_BYTES;
        if (overlap) chunk_slots[chunk_used++] = slot;
        (void) n_entries;  /* per-slot success log dropped; n_entries kept for the debug-bytes log above */

        /* Batch boundary: reap all in-flight WRs (RC QP completes in order, so
         * draining `inflight` completions confirms every write in this batch
         * has landed in the recipient's pinned memory). For the overlap path
         * we then fire the chunk's DONE-SLOTS-CHUNK RPC — the recipient can
         * backpatch this chunk while we transfer the next batch — preserving
         * the existing "data has landed before CHUNK RPC" invariant. */
        int is_last_slot = (i + 1 == n_slots);
        if (is_last_slot || inflight >= K) {
            int reaped = 0;
            while (reaped < inflight) {
                struct ibv_wc wc[64];
                int n = rdmamig_client_poll_send(L->client, wc, 64);
                if (n < 0) {
                    errs++;
                    serverLog(LL_WARNING,
                        "RDMA MIGRATE worker: send completion error in batch (reaped %d/%d, last_slot=%d)",
                        reaped, inflight, slot);
                    break;
                }
                if (n == 0) continue;  /* spin until the batch's WRs complete */
                reaped += n;
            }
            inflight = 0;

            if (overlap && chunk_used > 0) {
                int chunk_argc = 5 + chunk_used;
                const char **cargv = zmalloc((size_t) chunk_argc * sizeof(*cargv));
                size_t *cargvlen = zmalloc((size_t) chunk_argc * sizeof(*cargvlen));
                char seq_buf[16];
                int seq_len = snprintf(seq_buf, sizeof(seq_buf), "%d", chunk_seq);
                cargv[0] = "RDMA";              cargvlen[0] = 4;
                cargv[1] = "DONE-SLOTS-CHUNK";  cargvlen[1] = 16;
                cargv[2] = src_id;              cargvlen[2] = CLUSTER_NAMELEN;
                cargv[3] = migid_buf;           cargvlen[3] = (size_t) migid_len;
                cargv[4] = seq_buf;             cargvlen[4] = (size_t) seq_len;
                char (*cnumbuf)[16] = zmalloc((size_t) chunk_used * sizeof(*cnumbuf));
                for (int j = 0; j < chunk_used; j++) {
                    cargvlen[5 + j] = (size_t) snprintf(cnumbuf[j], 16, "%d", chunk_slots[j]);
                    cargv  [5 + j] = cnumbuf[j];
                }
                /* Append + flush, but DO NOT block on reply — that's the
                 * whole point: while the recipient backpatches this chunk we
                 * keep RDMA-writing the next K slots. */
                redisAppendCommandArgv(L->ctrl, chunk_argc, cargv, cargvlen);
                int wdone = 0;
                while (!wdone) {
                    if (redisBufferWrite(L->ctrl, &wdone) == REDIS_ERR) {
                        serverLog(LL_WARNING,
                            "RDMA MIGRATE worker: DONE-SLOTS-CHUNK seq=%d buffer write failed",
                            chunk_seq);
                        break;
                    }
                }
                pending_replies++;
                chunk_seq++;
                chunk_used = 0;
                zfree(cargv); zfree(cargvlen); zfree(cnumbuf);
            }
        }
    }

    /* Defensive: drain any leftover completions (e.g. last batch had a
     * post failure that bypassed the boundary). RC every-WR-signaled
     * guarantees each posted WR has a CQE we can poll off, no leaks. */
    while (inflight > 0) {
        struct ibv_wc wc[64];
        int n = rdmamig_client_poll_send(L->client, wc, 64);
        if (n < 0) { errs++; break; }
        if (n == 0) continue;
        inflight -= n;
    }

    if (overlap) {
        zfree(chunk_slots);
        /* Drain all pipelined CHUNK replies before returning. We don't error
         * on individual chunk failures here — BACKPATCH-STATUS polling on the
         * donor will surface a stuck batch. */
        for (int i = 0; i < pending_replies; i++) {
            redisReply *cr = NULL;
            if (redisGetReply(L->ctrl, (void **) &cr) == REDIS_OK) {
                if (cr && cr->type == REDIS_REPLY_ERROR) {
                    serverLog(LL_WARNING,
                        "RDMA MIGRATE worker: DONE-SLOTS-CHUNK reply error: %s",
                        cr->str ? cr->str : "(no body)");
                }
                if (cr) freeReplyObject(cr);
            }
        }
        serverLog(LL_NOTICE,
            "RDMA MIGRATE worker: TRANSFER (overlap) finished n=%d bytes=%zu errs=%d chunks=%d",
            n_slots, total_bytes, errs, chunk_seq);
    } else {
        /* Legacy path: single end-of-TRANSFER DONE-SLOTS RPC. The recipient
         * also accepts the legacy 2-arg form for back-compat (no tracking). */
        int argc = 4 + n_slots;
        const char **argv = zmalloc((size_t) argc * sizeof(*argv));
        size_t *argvlen = zmalloc((size_t) argc * sizeof(*argvlen));
        argv[0] = "RDMA";        argvlen[0] = 4;
        argv[1] = "DONE-SLOTS";  argvlen[1] = 10;
        argv[2] = src_id;        argvlen[2] = CLUSTER_NAMELEN;
        argv[3] = migid_buf;     argvlen[3] = (size_t) migid_len;
        char (*numbuf)[16] = zmalloc((size_t) n_slots * sizeof(*numbuf));
        for (int i = 0; i < n_slots; i++) {
            argvlen[4 + i] = (size_t) snprintf(numbuf[i], 16, "%d", chosen[i]);
            argv  [4 + i] = numbuf[i];
        }
        redisReply *r = redisCommandArgv(L->ctrl, argc, argv, argvlen);
        zfree(argv);
        zfree(argvlen);
        zfree(numbuf);

        if (r == NULL || r->type != REDIS_REPLY_STATUS) {
            serverLog(LL_WARNING,
                "RDMA MIGRATE worker: DONE-SLOTS reply not OK (data may still have landed)");
        }
        if (r) freeReplyObject(r);

        serverLog(LL_NOTICE,
            "RDMA MIGRATE worker: TRANSFER finished n=%d bytes=%zu errs=%d",
            n_slots, total_bytes, errs);
    }

    pthread_mutex_unlock(&L->mu);

    if (errs > 0 && err_out) {
        *err_out = sdscatfmt(sdsempty(), "TRANSFER: %i slot writes failed", errs);
        return -1;
    }
    return 0;
}

/* ---- worker thread state transitions -------------------------------- */

static const char *migStateName(rdmaMigrationState s) {
    switch (s) {
    case RDMA_MIG_INIT:        return "INIT";
    case RDMA_MIG_PREP:        return "PREP";
    case RDMA_MIG_REGISTERING: return "REGISTERING";
    case RDMA_MIG_FLIPPING:    return "FLIPPING";
    case RDMA_MIG_TRANSFER:    return "TRANSFER";
    case RDMA_MIG_BACKPATCH:   return "BACKPATCH";
    case RDMA_MIG_DONE:        return "DONE";
    case RDMA_MIG_FAILED:      return "FAILED";
    }
    return "?";
}

/* Worker-side state setter — single point so logging stays consistent.
 * Worker holds mig->mu only briefly across the assignment. */
static void migSetState(rdmaMigration *mig, rdmaMigrationState s) {
    pthread_mutex_lock(&mig->mu);
    mig->state = s;
    pthread_mutex_unlock(&mig->mu);
    serverLog(LL_NOTICE,
        "RDMA MIGRATE worker: id=%lld state=%s n_slots=%d",
        mig->id, migStateName(s), mig->n_slots);
}

/* If this migration was launched by an orchestrator (`RDMA MIGRATE-ALL`),
 * dial back to the orchestrator endpoint with a one-shot hiredis TCP
 * connection and fire `RDMA MIGRATE-COMPLETE`. Fire-and-forget — we don't
 * retry on connection failure (the orchestrator's status poll will time
 * out and trip ansible's fail-check if a peer doesn't report in).
 *
 * Called by the worker thread immediately before it returns NULL on DONE
 * or after entering FAILED. Safe to call when there's no orchestrator —
 * mig->orchestrator_endpoint is NULL in the legacy `RDMA MIGRATE` path.
 *
 * `state_name` is "DONE" or "FAILED" (passed in to avoid re-reading the
 * mig->state under the lock here). */
static void migNotifyOrchestratorIfAny(rdmaMigration *mig,
                                       const char *state_name,
                                       long long applied) {
    if (mig->orchestrator_endpoint == NULL) return;

    /* Parse "host:port" — last ':' splits. */
    sds ep = mig->orchestrator_endpoint;
    char *colon = strrchr(ep, ':');
    if (!colon) {
        serverLog(LL_WARNING,
            "RDMA MIGRATE worker: id=%lld bad orchestrator endpoint '%s'",
            mig->id, ep);
        return;
    }
    size_t hostlen = colon - ep;
    char host_buf[256];
    if (hostlen >= sizeof(host_buf)) hostlen = sizeof(host_buf) - 1;
    memcpy(host_buf, ep, hostlen);
    host_buf[hostlen] = '\0';
    int port = atoi(colon + 1);
    if (port <= 0 || port > 65535) {
        serverLog(LL_WARNING,
            "RDMA MIGRATE worker: id=%lld bad orchestrator port in '%s'",
            mig->id, ep);
        return;
    }

    /* Best-effort node id: copy our own (AqRaft-mode synth if cluster==NULL). */
    char my_id[CLUSTER_NAMELEN + 1];
    rdmaMigrationSelfName(my_id);

    struct timeval tv = {.tv_sec = 2, .tv_usec = 0};
    redisContext *cc = redisConnectWithTimeout(host_buf, port, tv);
    if (cc == NULL || cc->err) {
        serverLog(LL_WARNING,
            "RDMA MIGRATE worker: id=%lld could not dial orchestrator %s:%d (%s)",
            mig->id, host_buf, port,
            cc ? cc->errstr : "alloc failed");
        if (cc) redisFree(cc);
        return;
    }
    redisSetTimeout(cc, tv);

    redisReply *r = redisCommand(cc,
        "RDMA MIGRATE-COMPLETE %lld %s %s %lld",
        mig->orchestrator_orch_id, my_id, state_name, applied);
    if (r == NULL) {
        serverLog(LL_WARNING,
            "RDMA MIGRATE worker: id=%lld orchestrator callback timed out",
            mig->id);
    } else {
        if (r->type == REDIS_REPLY_ERROR) {
            serverLog(LL_WARNING,
                "RDMA MIGRATE worker: id=%lld orchestrator returned -ERR: %s",
                mig->id, r->str);
        } else {
            serverLog(LL_NOTICE,
                "RDMA MIGRATE worker: id=%lld notified orchestrator %s:%d state=%s",
                mig->id, host_buf, port, state_name);
        }
        freeReplyObject(r);
    }
    redisFree(cc);
}

/* Worker-side failure setter. Caller passes ownership of `err` (sds). */
static void migFail(rdmaMigration *mig, sds err) {
    pthread_mutex_lock(&mig->mu);
    mig->state = RDMA_MIG_FAILED;
    if (mig->err) sdsfree(mig->err);
    mig->err = err ? err : sdsnew("unknown error");
    mig->t_ended = time(NULL);
    pthread_mutex_unlock(&mig->mu);
    serverLog(LL_WARNING,
        "RDMA MIGRATE worker: id=%lld FAILED: %s", mig->id, mig->err);
    migNotifyOrchestratorIfAny(mig, "FAILED", 0);
}

static void *migrationWorker(void *arg) {
    rdmaMigration *mig = (rdmaMigration *) arg;
    sds err = NULL;

    serverLog(LL_NOTICE,
        "RDMA MIGRATE worker: started id=%lld addr=%s n_slots=%d start_delay_ms=%d",
        mig->id, mig->addr, mig->n_slots, mig->start_delay_ms);

    /* AqRaft Patch 22: server-side stagger. When the orchestrator dispatches
     * to multiple donor peers, each peer is stamped with a per-peer delay
     * so they don't all start hammering the recipient simultaneously. We
     * sleep here (on the worker thread, not the main loop) before doing
     * any work, including the PREP RPC. */
    if (mig->start_delay_ms > 0) {
        serverLog(LL_NOTICE,
            "RDMA MIGRATE worker: id=%lld sleeping %d ms before PREP (stagger)",
            mig->id, mig->start_delay_ms);
        struct timespec ts;
        ts.tv_sec  = mig->start_delay_ms / 1000;
        ts.tv_nsec = (long) (mig->start_delay_ms % 1000) * 1000000L;
        nanosleep(&ts, NULL);
    }

    /* Protocol log: donor opens a migration session. All donor replicas
     * record the event via the Raft log. */
    {
        char mgn_payload[160];
        int first_slot = (mig->n_slots > 0) ? mig->chosen[0] : -1;
        int last_slot  = (mig->n_slots > 0) ? mig->chosen[mig->n_slots - 1] : -1;
        snprintf(mgn_payload, sizeof(mgn_payload),
                 "sess=%lld slots=%d-%d n=%d recipient=%s",
                 mig->id, first_slot, last_slot, mig->n_slots, mig->addr);
        rdmaMgnLogSync("TXN_START", mgn_payload);
    }

    /* PREP: register recipient landing buffers for the chosen slots. */
    migSetState(mig, RDMA_MIG_PREP);
    if (rdmaMigratePrepHelper(mig->L, mig->chosen, mig->n_slots, &err) != 0) {
        migFail(mig, err);
        return NULL;
    }

    /* Aqueduct slot-state: PREP returned successfully, which means the
     * recipient has already flipped its own state to MIGRATING (see the
     * recipient-side hook in rdmaRegisterBlockSlotsCommand). Donor flips
     * MIGRATING now, with the recipient's host:port as the peer endpoint.
     * Ordering matters: recipient first (inside the PREP RPC handler),
     * donor second (here, after the RPC returns). This guarantees a client
     * that sees "MIGRATING" on either side knows the other side is also
     * in-flight. */
    {
        char recipient_endpoint[NET_HOST_PORT_STR_LEN];
        snprintf(recipient_endpoint, sizeof(recipient_endpoint), "%s:%d",
                 mig->host, mig->port);
        for (int i = 0; i < mig->n_slots; i++) {
            slotMigStateSet(mig->chosen[i], SLOT_STATE_MIGRATING,
                            recipient_endpoint);
        }
    }

    /* REGISTERING: ibv_reg_mr each source-side staging buffer. */
    migSetState(mig, RDMA_MIG_REGISTERING);
    if (rdmaReshardRegisterHelper(mig->L, mig->chosen, mig->n_slots,
                                  &mig->registered, &err) != 0) {
        migFail(mig, err);
        return NULL;
    }

    /* FLIPPING (early-ownership): flip ownership BEFORE TRANSFER so all
     * client writes during the transfer window route to the recipient (the
     * new owner) instead of orphaning on the donor. The cluster router's
     * read/write split (cluster.c) keeps reads bypassed for non-STABLE slots
     * — donor still serves its snapshot — while writes fall through to
     * standard routing and get a -MOVED to the recipient, which JedisCluster
     * uses to refresh its topology once per slot. The parallel two-sided
     * read in the YCSB client (donor + recipient, recipient-wins) is what
     * makes recipient-as-owner semantically correct from the start: the
     * recipient holds any post-FLIP writes; the donor's snapshot fills in
     * for keys not yet transferred. */
    migSetState(mig, RDMA_MIG_FLIPPING);
    if (rdmaReshardFlipHelper(mig->L, mig->chosen, mig->n_slots, &err) != 0) {
        migFail(mig, err);
        return NULL;
    }

    /* Aqueduct slot-state: donor flips MIGRATING → MIGRATED. Recipient
     * flipped MIGRATING → STABLE inside its RECV-FLIP handler. The donor's
     * MIGRATED state persists through TRANSFER + BACKPATCH so its snapshot
     * remains reachable to readers; a follow-up GC timer transitions
     * MIGRATED → STABLE once the migration is fully complete. */
    for (int i = 0; i < mig->n_slots; i++) {
        slotMigStateSet(mig->chosen[i], SLOT_STATE_MIGRATED, NULL);
    }

    /* TRANSFER: RDMA-WRITE + DONE-SLOTS. Donor no longer owns the slots but
     * still has the snapshot data; transfer runs against that snapshot. */
    migSetState(mig, RDMA_MIG_TRANSFER);
    /* Use db 0 — same as the legacy reshardTransferWorker path (DB selection
     * comes from the dispatching client; cluster mode is single-DB). */
    if (rdmaReshardTransferHelper(mig->L, &server.db[0], mig->chosen, mig->n_slots,
                              mig->id, &err) != 0) {
        migFail(mig, err);
        return NULL;
    }

    /* BACKPATCH: poll the recipient's RDMA BACKPATCH-STATUS until the
     * backpatch thread on the recipient reports the batch as done. The recipient
     * acknowledged DONE-SLOTS as soon as it queued the batch onto its
     * SPSC ring; the actual backpatch runs asynchronously on the recipient's
     * backpatch thread. We tail the status here so the source-side
     * RDMA_MIG_DONE transition genuinely means "data is in the recipient's
     * keyspace", not just "the recipient acked the buffer transfer."
     *
     * Poll cadence 10 ms; bails on FAILED. Retries on transient hiredis
     * errors (recipient busy). Upper bound: 60 s (~6000 polls). For the
     * typical 4095-slot batch the backpatch completes in ~tens to hundreds of
     * milliseconds; polling overhead is negligible. */
    migSetState(mig, RDMA_MIG_BACKPATCH);
    {
        char src_id[CLUSTER_NAMELEN + 1];
        rdmaMigrationSelfName(src_id);

        const int max_polls = 6000;  /* ~60 s at 10 ms each */
        int polls = 0;
        int done = 0;
        int chain_durable_sent = 0;  /* AqRaft Round 2: early dispatch signal */
        sds backpatch_err = NULL;
        while (polls < max_polls && !done) {
            pthread_mutex_lock(&mig->L->mu);
            redisReply *r = redisCommand(mig->L->ctrl,
                "RDMA BACKPATCH-STATUS %s %lld", src_id, mig->id);
            pthread_mutex_unlock(&mig->L->mu);
            if (r == NULL) {
                /* Hiredis error — treat as transient; brief sleep and retry. */
                usleep(10000);
                polls++;
                continue;
            }
            if (r->type == REDIS_REPLY_ERROR) {
                /* Recipient says no such batch (yet) — early poll race; retry. */
                freeReplyObject(r);
                usleep(10000);
                polls++;
                continue;
            }
            if (r->type == REDIS_REPLY_ARRAY && r->elements >= 6) {
                const char *state_str = r->element[0]->str;
                /* AqRaft Round 2: element[6] (when present) is the chain-durable
                 * flag - merge_done && chain_acked, with only the metadata-only
                 * MGN_INDX_UPD raft append still pending. Signal the orchestrator
                 * NOW (once) so it can dispatch the next donor while our INDX_UPD
                 * commits, overlapping the raft round-trip with the next donor's
                 * PREP+TRANSFER. We keep polling here until full "done" so the
                 * donor still holds its slots until all three durability flags
                 * are set (the 3-flag DONE invariant is unchanged). */
                if (!chain_durable_sent && r->elements >= 7 &&
                    r->element[6]->type == REDIS_REPLY_INTEGER &&
                    r->element[6]->integer == 1) {
                    chain_durable_sent = 1;
                    migNotifyOrchestratorIfAny(mig, "CHAIN_DURABLE",
                                               (long long) mig->n_slots);
                }
                if (state_str && strcmp(state_str, "done") == 0) {
                    done = 1;
                    /* Protocol log: donor sees recipient acked done. All
                     * donor replicas record the session close. */
                    char mgn_payload[64];
                    snprintf(mgn_payload, sizeof(mgn_payload),
                             "sess=%lld", mig->id);
                    rdmaMgnLogSync("TXN_DONE", mgn_payload);
                }
                else if (state_str && strcmp(state_str, "failed") == 0) {
                    backpatch_err = sdscatfmt(sdsempty(),
                        "recipient backpatch FAILED: %s",
                        (r->element[5]->str ? r->element[5]->str : "(no detail)"));
                    freeReplyObject(r);
                    break;
                }
            }
            freeReplyObject(r);
            if (!done) {
                usleep(10000);
                polls++;
            }
        }
        if (backpatch_err) {
            migFail(mig, backpatch_err);
            return NULL;
        }
        if (!done) {
            migFail(mig, sdsnew("recipient backpatch timed out after 60s of polling"));
            return NULL;
        }
    }

    /* Donor migration DONE: backpatch is genuinely applied on the recipient.
     * Flip the donor's slot meta state back to STABLE for the migrated slots.
     * With state=STABLE, the slot_meta bypass at cluster.c:1326 stops firing
     * on this node — reads fall through to standard cluster routing, which
     * returns MOVED → recipient (since the cluster table was flipped at FLIP
     * time). JedisCluster refreshes once per slot and all subsequent client
     * traffic on those slots routes to the recipient.
     *
     * Direct byte-write rather than per-slot slotMigStateSet to avoid 1365
     * lock cycles racing with the donor's main thread serving live client
     * traffic. */
    /* Slot-meta cleanup is a vanilla-cluster concept; AqRaft donors (no
     * server.cluster) don't maintain slot_mig_state / slot_peer_endpoint. */
    if (server.cluster != NULL) {
        for (int i = 0; i < mig->n_slots; i++) {
            int s = mig->chosen[i];
            server.cluster->slot_mig_state[s] = (uint8_t) SLOT_STATE_STABLE;
            server.cluster->slot_peer_endpoint[s][0] = '\0';
        }
    }

    /* NOTE: previously cleared migrating_slots_to[] here to close the
     * ~5.5% throughput gap vs 4-node-baseline (the cluster.c routing
     * walks importing_slot/v2_src_serving branches when this is set).
     * Reverted because it broke quorum: ~10s after cleanup, the
     * cluster marked the recipient as failing. Root cause not yet
     * identified — likely a routing interaction post-FLIP+post-cleanup
     * that causes redis3's main thread to fall behind on cluster
     * heartbeats. Keep migrating_slots_to[] set for now. */

    pthread_mutex_lock(&mig->mu);
    mig->state = RDMA_MIG_DONE;
    mig->t_ended = time(NULL);
    pthread_mutex_unlock(&mig->mu);
    serverLog(LL_NOTICE,
        "RDMA MIGRATE worker: id=%lld DONE n_slots=%d", mig->id, mig->n_slots);
    migNotifyOrchestratorIfAny(mig, "DONE", (long long) mig->n_slots);
    return NULL;
}

/* ---- public commands ------------------------------------------------- */

/* RDMA MIGRATE recipient-host recipient-port n-slots
 *   [--orchestrator orch-host:orch-port orch-id]
 *
 * The optional --orchestrator triplet is set by `RDMA MIGRATE-ALL` when a
 * peer donor is dispatched from the orchestrator node; on entering
 * DONE/FAILED the worker dials back to the orchestrator with
 * `RDMA MIGRATE-COMPLETE`. When not set the command behaves identically
 * to the legacy 5-arg form (full backwards compatibility). */
/* Core dispatch routine extracted from rdmaMigrateCommand so that the
 * orchestrator (RDMA MIGRATE-ALL) can drive its OWN local migration
 * without going through a hiredis loopback (which would deadlock — the
 * orchestrator command is currently holding the event loop and can't
 * service its own dial-back).
 *
 * On success: returns the new migration's id and `*err_out` is NULL.
 * On failure: returns -1 and `*err_out` is a static string with the
 * cause. The function owns `orch_endpoint` (takes it; caller must not
 * free it after a successful call). On failure the function frees it. */
static long long startLocalMigration(const char *host, int port, int n_slots,
                                     sds orch_endpoint, long long orch_id,
                                     int start_delay_ms,
                                     const char **err_out) {
    *err_out = NULL;

    /* AqRaft-aware guard: ok if either vanilla-cluster is up OR
     * redisraft-mode slot config is set. */
    int aqraft_ok = (server.rdma_migration_redisraft_mode &&
                     server.rdma_migration_redisraft_slots != NULL &&
                     sdslen(server.rdma_migration_redisraft_slots) > 0);
    if ((server.cluster == NULL || server.cluster->myself == NULL) && !aqraft_ok) {
        if (orch_endpoint) sdsfree(orch_endpoint);
        *err_out = "cluster mode not enabled on this node";
        return -1;
    }

    int *chosen = zmalloc((size_t) n_slots * sizeof(int));
    int picked = 0;
    clusterTopoLockRead();
    for (int i = 0; i < CLUSTER_SLOTS && picked < n_slots; i++) {
        if (rdmaMigrationOwnsSlot(i)) {
            chosen[picked++] = i;
        }
    }
    clusterTopoUnlock();
    if (picked < n_slots) {
        zfree(chosen);
        if (orch_endpoint) sdsfree(orch_endpoint);
        *err_out = "self owns fewer slots than requested";
        return -1;
    }

    sds key = sdscatfmt(sdsempty(), "%s:%i", host, port);
    rdmaOutboundLink *L = dictFetchValue(server.rdma_outbound_links, key);
    if (L == NULL) {
        L = rdmaOutboundLinkOpen(host, port);
        if (L == NULL) {
            sdsfree(key);
            zfree(chosen);
            if (orch_endpoint) sdsfree(orch_endpoint);
            *err_out = "could not establish outbound RDMA link (see server log)";
            return -1;
        }
        dictAdd(server.rdma_outbound_links, key, L);
    } else {
        sdsfree(key);
    }

    rdmaMigration *mig = zcalloc(sizeof(*mig));
    mig->id        = server.rdma_migration_next_id++;
    mig->addr      = sdscatfmt(sdsempty(), "%s:%i", host, port);
    mig->host      = sdsnew(host);
    mig->port      = port;
    mig->n_slots   = n_slots;
    mig->chosen    = chosen;
    mig->L         = L;
    mig->orchestrator_endpoint = orch_endpoint;   /* takes ownership */
    mig->orchestrator_orch_id  = orch_id;
    mig->start_delay_ms        = (start_delay_ms > 0) ? start_delay_ms : 0;
    pthread_mutex_init(&mig->mu, NULL);
    mig->state     = RDMA_MIG_INIT;
    mig->registered = 0;
    mig->err       = NULL;
    mig->t_started = time(NULL);
    mig->t_ended   = 0;

    sds dict_key = sdsfromlonglong(mig->id);
    dictAdd(server.rdma_migrations, dict_key, mig);
    server.rdma_migration_last_id = mig->id;

    if (pthread_create(&mig->thr, NULL, migrationWorker, mig) != 0) {
        dictDelete(server.rdma_migrations, dict_key);
        *err_out = "pthread_create failed";
        return -1;
    }
    pthread_detach(mig->thr);
    return mig->id;
}

void rdmaMigrateCommand(client *c) {
    /* Accepted argcs:
     *   5  : RDMA MIGRATE host port n-slots
     *   7  : RDMA MIGRATE host port n-slots --start-delay-ms N
     *   8  : RDMA MIGRATE host port n-slots --orchestrator ep id
     *   10 : RDMA MIGRATE host port n-slots --orchestrator ep id --start-delay-ms N
     */
    if (c->argc != 5 && c->argc != 7 && c->argc != 8 && c->argc != 10) {
        addReplyError(c, "syntax: RDMA MIGRATE recipient-host recipient-port n-slots "
                         "[--orchestrator orch-host:orch-port orch-id] "
                         "[--start-delay-ms N]");
        return;
    }
    const char *host = c->argv[2]->ptr;
    long long port_ll, n_slots_ll;
    if (getLongLongFromObject(c->argv[3], &port_ll) != C_OK ||
        port_ll <= 0 || port_ll > 65535) {
        addReplyError(c, "port out of range");
        return;
    }
    if (getLongLongFromObject(c->argv[4], &n_slots_ll) != C_OK ||
        n_slots_ll <= 0 || n_slots_ll > CLUSTER_SLOTS) {
        addReplyError(c, "n-slots out of range");
        return;
    }
    int port = (int) port_ll;
    int n_slots = (int) n_slots_ll;

    sds       orch_endpoint  = NULL;
    long long orch_id        = 0;
    long long start_delay_ms = 0;

    int idx = 5;
    /* Optional --orchestrator block (3 tokens). */
    if (c->argc >= 8 && strcasecmp(c->argv[5]->ptr, "--orchestrator") == 0) {
        orch_endpoint = sdsdup((sds) c->argv[6]->ptr);
        if (getLongLongFromObject(c->argv[7], &orch_id) != C_OK || orch_id <= 0) {
            sdsfree(orch_endpoint);
            addReplyError(c, "orchestrator id must be a positive integer");
            return;
        }
        idx = 8;
    }
    /* Optional --start-delay-ms block (2 tokens). */
    if (c->argc > idx) {
        if (c->argc != idx + 2 ||
            strcasecmp(c->argv[idx]->ptr, "--start-delay-ms") != 0 ||
            getLongLongFromObject(c->argv[idx + 1], &start_delay_ms) != C_OK ||
            start_delay_ms < 0 || start_delay_ms > 600000) {
            if (orch_endpoint) sdsfree(orch_endpoint);
            addReplyError(c, "--start-delay-ms must be in [0..600000] (ms)");
            return;
        }
    }

    const char *err = NULL;
    long long mig_id = startLocalMigration(host, port, n_slots,
                                           orch_endpoint, orch_id,
                                           (int) start_delay_ms, &err);
    if (mig_id < 0) {
        addReplyError(c, err ? err : "RDMA MIGRATE: dispatch failed");
        return;
    }
    addReplyStatusFormat(c, "OK migration_id=%lld running (n_slots=%d, %s:%d, start_delay_ms=%d)",
                         mig_id, n_slots, host, port, (int) start_delay_ms);
}

/* RDMA MIGRATE-STATUS [migration_id] */
void rdmaMigrateStatusCommand(client *c) {
    if (c->argc > 3) {
        addReplyError(c, "syntax: RDMA MIGRATE-STATUS [migration_id]");
        return;
    }

    long long want_id;
    if (c->argc == 3) {
        if (getLongLongFromObject(c->argv[2], &want_id) != C_OK || want_id <= 0) {
            addReplyError(c, "migration_id must be a positive integer");
            return;
        }
    } else {
        want_id = server.rdma_migration_last_id;
        if (want_id == 0) {
            addReplyError(c, "no migrations have been dispatched yet");
            return;
        }
    }

    sds key = sdsfromlonglong(want_id);
    rdmaMigration *mig = dictFetchValue(server.rdma_migrations, key);
    sdsfree(key);
    if (mig == NULL) {
        addReplyErrorFormat(c, "no such migration id: %lld", want_id);
        return;
    }

    pthread_mutex_lock(&mig->mu);
    const char *state = migStateName(mig->state);
    int registered   = mig->registered;
    int n_slots      = mig->n_slots;
    sds err_copy     = mig->err ? sdsdup(mig->err) : sdsempty();
    time_t t_started = mig->t_started;
    time_t t_ended   = mig->t_ended;
    int slot_first   = (n_slots > 0) ? mig->chosen[0] : -1;
    int slot_last    = (n_slots > 0) ? mig->chosen[n_slots - 1] : -1;
    pthread_mutex_unlock(&mig->mu);

    long long elapsed = (long long) ((t_ended ? t_ended : time(NULL)) - t_started);

    /* 8-element flat reply: state, registered, n_slots, slot_first, slot_last,
     * elapsed_seconds, ended (0 if still running), err (empty if none). */
    addReplyArrayLen(c, 8);
    addReplyBulkCString(c, state);
    addReplyLongLong(c, registered);
    addReplyLongLong(c, n_slots);
    addReplyLongLong(c, slot_first);
    addReplyLongLong(c, slot_last);
    addReplyLongLong(c, elapsed);
    addReplyLongLong(c, t_ended ? 1 : 0);
    addReplyBulkSds(c, err_copy);
    /* err_copy is consumed by addReplyBulkSds (it takes ownership of the sds). */
}

/* Dict valDestructor for server.rdma_migrations. */
void rdmaMigrationFree(dict *d, void *v) {
    (void) d;
    rdmaMigration *mig = v;
    if (mig == NULL) return;
    /* mig->chosen is freed here; the worker only reads it. By the time the
     * dict frees this entry (server shutdown, manual eviction), the worker
     * has either DONE or FAILED — we don't attempt to cancel in-flight
     * threads. */
    if (mig->chosen) zfree(mig->chosen);
    if (mig->addr)   sdsfree(mig->addr);
    if (mig->host)   sdsfree(mig->host);
    if (mig->err)    sdsfree(mig->err);
    if (mig->orchestrator_endpoint) sdsfree(mig->orchestrator_endpoint);
    pthread_mutex_destroy(&mig->mu);
    zfree(mig);
}

/* ====================================================================== *
 *  Server-side reshard orchestrator: RDMA MIGRATE-ALL                    *
 *                                                                        *
 *  Replaces ansible's per-source serial loop with a single redis command *
 *  that dispatches RDMA MIGRATE to N donors (self + peer-host:port list) *
 *  in parallel. Each peer donor's worker thread, on entering DONE or     *
 *  FAILED, dials back to this orchestrator with RDMA MIGRATE-COMPLETE.   *
 *  Ansible polls one aggregated status command instead of N separate    *
 *  MIGRATE-STATUS polls.                                                 *
 * ====================================================================== */

static const char *orchStateName(rdmaOrchestrationState s) {
    switch (s) {
        case RDMA_ORCH_INIT:        return "INIT";
        case RDMA_ORCH_DISPATCHING: return "DISPATCHING";
        case RDMA_ORCH_RUNNING:     return "RUNNING";
        case RDMA_ORCH_DONE:        return "DONE";
        case RDMA_ORCH_FAILED:      return "FAILED";
    }
    return "?";
}

/* Helper: parse "host:port" → (host_buf, port). Returns -1 on parse error. */
static int parseHostPort(const char *s, char *host_buf, size_t host_buf_sz, int *port_out) {
    const char *colon = strrchr(s, ':');
    if (!colon) return -1;
    size_t hostlen = (size_t) (colon - s);
    if (hostlen == 0 || hostlen >= host_buf_sz) return -1;
    memcpy(host_buf, s, hostlen);
    host_buf[hostlen] = '\0';
    int p = atoi(colon + 1);
    if (p <= 0 || p > 65535) return -1;
    *port_out = p;
    return 0;
}

/* Dispatch RDMA MIGRATE to a peer donor over hiredis TCP. Returns the
 * peer's migration id on success, -1 on failure (and sets *err_out to a
 * borrowed static string describing the failure). The connection is
 * closed before returning — short-lived, one-shot.
 *
 * `rdma_port` is the per-source RDMA port this donor should tell the
 * recipient to bind for it. The function does a CONFIG SET first to
 * avoid the recipient-port-collision that happens if all donors default
 * to the same port (17777). */
static long long orchDispatchPeer(const char *peer_host, int peer_port,
                                  const char *recipient_host, int recipient_port,
                                  int n_slots, int rdma_port,
                                  const char *orch_endpoint, long long orch_id,
                                  int start_delay_ms,
                                  const char **err_out) {
    *err_out = NULL;
    struct timeval tv = {.tv_sec = 5, .tv_usec = 0};
    redisContext *cc = redisConnectWithTimeout(peer_host, peer_port, tv);
    if (cc == NULL || cc->err) {
        *err_out = "peer connect failed";
        if (cc) redisFree(cc);
        return -1;
    }
    redisSetTimeout(cc, tv);

    /* Set the per-source RDMA port BEFORE dispatching MIGRATE — otherwise
     * the second/third concurrent donor will collide with the first on
     * the recipient's RDMA listener. */
    {
        redisReply *cfg = redisCommand(cc,
            "CONFIG SET rdma-migration-port %d", rdma_port);
        if (cfg == NULL) {
            *err_out = "peer CONFIG SET timed out";
            redisFree(cc);
            return -1;
        }
        int ok = (cfg->type == REDIS_REPLY_STATUS && strcasecmp(cfg->str, "OK") == 0);
        freeReplyObject(cfg);
        if (!ok) {
            *err_out = "peer CONFIG SET failed";
            redisFree(cc);
            return -1;
        }
    }

    redisReply *r = redisCommand(cc,
        "RDMA MIGRATE %s %d %d --orchestrator %s %lld --start-delay-ms %d",
        recipient_host, recipient_port, n_slots,
        orch_endpoint, orch_id, start_delay_ms);
    long long peer_mig_id = -1;
    if (r == NULL) {
        *err_out = "peer dispatch timed out";
    } else if (r->type == REDIS_REPLY_ERROR) {
        *err_out = "peer returned -ERR";
        serverLog(LL_WARNING, "RDMA MIGRATE-ALL: peer %s:%d returned -ERR: %s",
                  peer_host, peer_port, r->str);
    } else if (r->type == REDIS_REPLY_STATUS || r->type == REDIS_REPLY_STRING) {
        /* Reply form: "OK migration_id=<N> running (...)". */
        const char *p = strstr(r->str, "migration_id=");
        if (p) {
            peer_mig_id = strtoll(p + strlen("migration_id="), NULL, 10);
        }
        if (peer_mig_id <= 0) {
            *err_out = "could not parse migration_id from peer reply";
            peer_mig_id = -1;
        }
    } else {
        *err_out = "unexpected peer reply type";
    }
    if (r) freeReplyObject(r);
    redisFree(cc);
    return peer_mig_id;
}

/* AqRaft completion-driven dispatch: the sequencer dispatches each peer
 * donor the instant the prior donor reaches terminal state — strictly
 * sequential, zero idle gap, zero overlap. Replaces the fixed per-peer
 * stagger when server.rdma_migration_peer_stagger_ms == 0. Spawned by
 * orchAllocateAndDispatch after SELF is dispatched; runs detached on its
 * own thread so the orchestrator's main-thread MIGRATE-ALL handler returns
 * immediately. Owns all sds and the arg block. */
typedef struct {
    rdmaOrchestration *orch;    /* not owned; lifetime managed by orch dict */
    int                n_peers;
    sds               *peer_endpoints; /* owned: n_peers entries + the array */
    char               recipient_host[256];
    int                recipient_port;
    int                n_slots_per_source;
    sds                self_endpoint;  /* owned */
    int                rdma_port_base;
} orchSequencerArg;

static void *orchSequencerMain(void *arg) {
    orchSequencerArg *sa = (orchSequencerArg *) arg;
    rdmaOrchestration *orch = sa->orch;

    /* donors[0] = self; peer i lives at donors[i+1]. To dispatch peer i we
     * wait for donors[i] (the prior donor in the chain) to reach terminal. */
    for (int i = 0; i < sa->n_peers; i++) {
        /* Spin-wait for the prior donor's terminal flag. 10ms poll is fine —
         * a per-donor cycle is hundreds of ms to seconds. */
        /* AqRaft Round 2: gate on chain_durable OR terminal, not terminal
         * alone. chain_durable means the prior donor's data is merged on the
         * recipient and chain-replicated to a majority; only its metadata-only
         * MGN_INDX_UPD raft append is still in flight. Dispatching the next
         * donor now overlaps that raft round-trip with the next donor's
         * PREP+TRANSFER. terminal is still honored so a FAILED prior donor
         * (which may never go chain_durable) doesn't stall the chain. */
        while (1) {
            pthread_mutex_lock(&orch->mu);
            int ready = orch->donors[i].chain_durable || orch->donors[i].terminal;
            pthread_mutex_unlock(&orch->mu);
            if (ready) break;
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 10 * 1000 * 1000 };
            nanosleep(&ts, NULL);
        }

        /* Dispatch peer i with zero start delay — the wait above is the only
         * sequencing point we need. */
        char host_buf[256]; int peer_port;
        if (parseHostPort(sa->peer_endpoints[i], host_buf, sizeof(host_buf), &peer_port) != 0) {
            serverLog(LL_WARNING,
                "RDMA MIGRATE-ALL (completion-driven): bad peer endpoint '%s'",
                sa->peer_endpoints[i]);
            pthread_mutex_lock(&orch->mu);
            orch->donors[i + 1].terminal = 1;
            orch->donors[i + 1].terminal_state = RDMA_MIG_FAILED;
            orch->donors[i + 1].err = sdsnew("bad endpoint format");
            orch->n_terminal++;
            orch->n_failed++;
            pthread_mutex_unlock(&orch->mu);
            continue;
        }
        int peer_rdma_port = sa->rdma_port_base + 1 + i;
        const char *perr = NULL;
        long long peer_mig_id = orchDispatchPeer(host_buf, peer_port,
                                                 sa->recipient_host, sa->recipient_port,
                                                 sa->n_slots_per_source, peer_rdma_port,
                                                 sa->self_endpoint, orch->id,
                                                 0 /* zero start delay — gapless */,
                                                 &perr);
        pthread_mutex_lock(&orch->mu);
        if (peer_mig_id < 0) {
            serverLog(LL_WARNING,
                "RDMA MIGRATE-ALL (completion-driven): failed to dispatch to %s: %s",
                sa->peer_endpoints[i], perr ? perr : "?");
            orch->donors[i + 1].terminal = 1;
            orch->donors[i + 1].terminal_state = RDMA_MIG_FAILED;
            orch->donors[i + 1].err = sdsnew(perr ? perr : "dispatch failed");
            orch->n_terminal++;
            orch->n_failed++;
        } else {
            orch->donors[i + 1].migration_id = peer_mig_id;
            serverLog(LL_NOTICE,
                "RDMA MIGRATE-ALL (completion-driven): dispatched peer %d (%s) "
                "after donor %d terminal",
                i, sa->peer_endpoints[i], i);
        }
        pthread_mutex_unlock(&orch->mu);
    }

    /* Cleanup owned state. */
    for (int i = 0; i < sa->n_peers; i++) sdsfree(sa->peer_endpoints[i]);
    zfree(sa->peer_endpoints);
    sdsfree(sa->self_endpoint);
    zfree(sa);
    return NULL;
}

/* Allocate, dispatch, and register an orchestration. Returns the
 * orchestration id on success; 0 on failure with an error reply already
 * sent to the client `c`. */
static long long orchAllocateAndDispatch(client *c,
                                         const char *recipient_host, int recipient_port,
                                         int n_slots_per_source,
                                         sds *peer_endpoints, int n_peers)
{
    /* Figure out our own endpoint to pass to peers. Prefer the cluster
     * node's announced ip+port; fall back to bindaddr. */
    int aqraft_ok = (server.rdma_migration_redisraft_mode &&
                     server.rdma_migration_redisraft_slots != NULL &&
                     sdslen(server.rdma_migration_redisraft_slots) > 0);
    if ((server.cluster == NULL || server.cluster->myself == NULL) && !aqraft_ok) {
        addReplyError(c, "cluster mode not enabled on this node");
        return 0;
    }
    char self_host[256];
    int  self_port = server.port;     /* the client port we accept commands on */
    if (server.cluster != NULL && server.cluster->myself != NULL) {
        clusterNode *me = server.cluster->myself;
        if (me->ip && me->ip[0])
            snprintf(self_host, sizeof(self_host), "%s", me->ip);
        else
            snprintf(self_host, sizeof(self_host), "127.0.0.1");
    } else {
        /* AqRaft: fall back to hostname for self ip. */
        if (gethostname(self_host, sizeof(self_host)) != 0)
            snprintf(self_host, sizeof(self_host), "127.0.0.1");
        self_host[sizeof(self_host) - 1] = '\0';
    }
    sds self_endpoint = sdscatfmt(sdsempty(), "%s:%i", self_host, self_port);

    /* Allocate the orchestration. n_donors = self + n_peers. */
    int n_donors = 1 + n_peers;
    rdmaOrchestration *orch = zcalloc(sizeof(*orch));
    orch->id = server.rdma_orchestration_next_id++;
    orch->n_donors = n_donors;
    orch->donors = zcalloc((size_t) n_donors * sizeof(orch->donors[0]));
    pthread_mutex_init(&orch->mu, NULL);
    orch->state = RDMA_ORCH_INIT;
    orch->t_started = time(NULL);

    /* Slot 0 = self; subsequent slots = peers. */
    char self_name[CLUSTER_NAMELEN + 1];
    rdmaMigrationSelfName(self_name);
    orch->donors[0].endpoint = sdsdup(self_endpoint);
    orch->donors[0].node_id  = sdsnewlen(self_name, CLUSTER_NAMELEN);
    orch->donors[0].is_local = 1;
    orch->donors[0].migration_id = 0;     /* set after self-dispatch */

    for (int i = 0; i < n_peers; i++) {
        orch->donors[i + 1].endpoint = sdsdup(peer_endpoints[i]);
        orch->donors[i + 1].node_id  = NULL;
        orch->donors[i + 1].is_local = 0;
    }

    /* Register before dispatching so MIGRATE-COMPLETE callbacks can find us. */
    sds dict_key = sdsfromlonglong(orch->id);
    dictAdd(server.rdma_orchestrations, dict_key, orch);
    server.rdma_orchestration_last_id = orch->id;

    orch->state = RDMA_ORCH_DISPATCHING;

    /* Per-donor RDMA ports. Same base as the legacy playbook (17777) so
     * the recipient's binding behaviour is identical. Self takes index 0
     * (matching the existing single-source path), peers take 1.. n_peers.
     * This must be different per donor because all donors send INIT-SERVER
     * to the recipient and the recipient binds the requested port. */
    const int RDMA_PORT_BASE = 17777;

    /* Dispatch to peer donors first (so they're already in-flight while we
     * set up our own).
     *
     * AqRaft Patch 22: server-side stagger. Each peer is stamped with a
     * per-peer start delay so peers don't all start RDMA-WRITEing the
     * recipient simultaneously. Self gets delay=0 (runs immediately).
     * Stagger amount is from CONFIG rdma-migration-peer-stagger-ms
     * (default 10000ms, 0 disables). Peer i gets (i+1)*stagger. */
    const int PEER_STAGGER_MS = server.rdma_migration_peer_stagger_ms;
    int n_dispatched = 0;

    if (PEER_STAGGER_MS == 0) {
        /* AqRaft COMPLETION-DRIVEN sequential dispatch (PEER_STAGGER_MS=0):
         * dispatch SELF first; spawn a sequencer thread that dispatches each
         * peer the moment the prior donor reaches terminal state. Donors run
         * strictly sequentially with zero idle gap (vs the fixed-stagger
         * path's idle gap when per-donor work < stagger) and zero overlap
         * (vs the simultaneous-donors crash at stagger=0 in the old code).
         * See orchSequencerMain above. */
        server.rdma_migration_port = RDMA_PORT_BASE;
        const char *self_err = NULL;
        sds self_orch_ep = sdsdup(self_endpoint);
        long long self_mig_id = startLocalMigration(recipient_host, recipient_port,
                                                    n_slots_per_source,
                                                    self_orch_ep, orch->id,
                                                    0, &self_err);
        if (self_mig_id < 0) {
            serverLog(LL_WARNING, "RDMA MIGRATE-ALL: self dispatch failed: %s",
                      self_err ? self_err : "?");
            orch->donors[0].terminal = 1;
            orch->donors[0].terminal_state = RDMA_MIG_FAILED;
            orch->donors[0].err = sdsnew(self_err ? self_err : "self dispatch failed");
            orch->n_terminal++;
            orch->n_failed++;
        } else {
            orch->donors[0].migration_id = self_mig_id;
            n_dispatched++;
        }

        if (n_peers > 0) {
            orchSequencerArg *sa = zcalloc(sizeof(*sa));
            sa->orch = orch;
            sa->n_peers = n_peers;
            sa->peer_endpoints = zcalloc((size_t) n_peers * sizeof(sds));
            for (int i = 0; i < n_peers; i++) sa->peer_endpoints[i] = sdsdup(peer_endpoints[i]);
            snprintf(sa->recipient_host, sizeof(sa->recipient_host), "%s", recipient_host);
            sa->recipient_port = recipient_port;
            sa->n_slots_per_source = n_slots_per_source;
            sa->self_endpoint = sdsdup(self_endpoint);
            sa->rdma_port_base = RDMA_PORT_BASE;
            pthread_t tid;
            if (pthread_create(&tid, NULL, orchSequencerMain, sa) != 0) {
                serverLog(LL_WARNING,
                    "RDMA MIGRATE-ALL: completion-driven sequencer spawn failed; "
                    "marking %d peers as failed", n_peers);
                for (int i = 0; i < n_peers; i++) {
                    sdsfree(sa->peer_endpoints[i]);
                    orch->donors[i + 1].terminal = 1;
                    orch->donors[i + 1].terminal_state = RDMA_MIG_FAILED;
                    orch->donors[i + 1].err = sdsnew("sequencer spawn failed");
                    orch->n_terminal++;
                    orch->n_failed++;
                }
                zfree(sa->peer_endpoints);
                sdsfree(sa->self_endpoint);
                zfree(sa);
            } else {
                pthread_detach(tid);
                /* Don't pre-bump n_dispatched for peers — the sequencer
                 * dispatches them asynchronously and reports actual
                 * outcomes into orch state via the same terminal/migration_id
                 * fields. Self alone keeps orch in RUNNING below. */
            }
        }
    } else {
        /* Legacy fixed-stagger dispatch (PEER_STAGGER_MS > 0): each peer is
         * stamped with start_delay_ms = (i+1)*PEER_STAGGER_MS. Self runs
         * immediately. Kept for back-compat; new default is the
         * completion-driven path above (PEER_STAGGER_MS=0). */
        for (int i = 0; i < n_peers; i++) {
            char host_buf[256]; int peer_port;
            if (parseHostPort(peer_endpoints[i], host_buf, sizeof(host_buf), &peer_port) != 0) {
                serverLog(LL_WARNING, "RDMA MIGRATE-ALL: bad peer endpoint '%s'",
                          peer_endpoints[i]);
                orch->donors[i + 1].terminal = 1;
                orch->donors[i + 1].terminal_state = RDMA_MIG_FAILED;
                orch->donors[i + 1].err = sdsnew("bad endpoint format");
                orch->n_terminal++;
                orch->n_failed++;
                continue;
            }
            int peer_rdma_port = RDMA_PORT_BASE + 1 + i;
            int peer_delay_ms = (i + 1) * PEER_STAGGER_MS;
            const char *perr = NULL;
            long long peer_mig_id = orchDispatchPeer(host_buf, peer_port,
                                                     recipient_host, recipient_port,
                                                     n_slots_per_source, peer_rdma_port,
                                                     self_endpoint, orch->id,
                                                     peer_delay_ms,
                                                     &perr);
            if (peer_mig_id < 0) {
                serverLog(LL_WARNING, "RDMA MIGRATE-ALL: failed to dispatch to %s: %s",
                          peer_endpoints[i], perr ? perr : "?");
                orch->donors[i + 1].terminal = 1;
                orch->donors[i + 1].terminal_state = RDMA_MIG_FAILED;
                orch->donors[i + 1].err = sdsnew(perr ? perr : "dispatch failed");
                orch->n_terminal++;
                orch->n_failed++;
            } else {
                orch->donors[i + 1].migration_id = peer_mig_id;
                n_dispatched++;
            }
        }

        /* Dispatch self in-process, NOT via hiredis loopback. The orchestrator
         * command currently holds the event loop, so a hiredis dial-back to
         * ourselves would deadlock waiting for the listener to accept the new
         * connection. startLocalMigration sets up and detaches the worker
         * thread without going through redis-cli.
         *
         * Set our own rdma-migration-port to the base value first so the
         * recipient binds a port that doesn't collide with the peers'. */
        server.rdma_migration_port = RDMA_PORT_BASE;
        const char *self_err = NULL;
        sds self_orch_ep = sdsdup(self_endpoint);   /* startLocalMigration takes ownership */
        long long self_mig_id = startLocalMigration(recipient_host, recipient_port,
                                                    n_slots_per_source,
                                                    self_orch_ep, orch->id,
                                                    0, &self_err);
        if (self_mig_id < 0) {
            serverLog(LL_WARNING, "RDMA MIGRATE-ALL: self dispatch failed: %s",
                      self_err ? self_err : "?");
            orch->donors[0].terminal = 1;
            orch->donors[0].terminal_state = RDMA_MIG_FAILED;
            orch->donors[0].err = sdsnew(self_err ? self_err : "self dispatch failed");
            orch->n_terminal++;
            orch->n_failed++;
        } else {
            orch->donors[0].migration_id = self_mig_id;
            n_dispatched++;
        }
    }

    orch->state = (n_dispatched == 0) ? RDMA_ORCH_FAILED : RDMA_ORCH_RUNNING;
    if (orch->n_terminal == orch->n_donors) {
        /* All donors failed to dispatch — already terminal. */
        orch->state = (orch->n_failed > 0) ? RDMA_ORCH_FAILED : RDMA_ORCH_DONE;
        orch->t_ended = time(NULL);
    }

    serverLog(LL_NOTICE,
        "RDMA MIGRATE-ALL orchestration_id=%lld dispatched: n_donors=%d n_dispatched=%d target=%s:%d slots_per_source=%d",
        orch->id, n_donors, n_dispatched, recipient_host, recipient_port,
        n_slots_per_source);

    sdsfree(self_endpoint);
    return orch->id;
}

/* RDMA MIGRATE-ALL recipient-host recipient-port slots-per-source peer-host:port ... */
void rdmaMigrateAllCommand(client *c) {
    if (c->argc < 5) {
        addReplyError(c, "syntax: RDMA MIGRATE-ALL recipient-host recipient-port slots-per-source peer-host:port ...");
        return;
    }
    const char *recipient_host = c->argv[2]->ptr;
    long long port_ll, n_slots_ll;
    if (getLongLongFromObject(c->argv[3], &port_ll) != C_OK ||
        port_ll <= 0 || port_ll > 65535) {
        addReplyError(c, "recipient port out of range");
        return;
    }
    if (getLongLongFromObject(c->argv[4], &n_slots_ll) != C_OK ||
        n_slots_ll <= 0 || n_slots_ll > CLUSTER_SLOTS) {
        addReplyError(c, "slots-per-source out of range");
        return;
    }
    int recipient_port = (int) port_ll;
    int n_slots = (int) n_slots_ll;
    int n_peers = c->argc - 5;

    sds *peers = (n_peers > 0) ? zmalloc((size_t) n_peers * sizeof(sds)) : NULL;
    for (int i = 0; i < n_peers; i++) {
        peers[i] = (sds) c->argv[5 + i]->ptr;
    }

    long long orch_id = orchAllocateAndDispatch(c, recipient_host, recipient_port,
                                                n_slots, peers, n_peers);
    if (peers) zfree(peers);
    if (orch_id == 0) return;  /* reply already sent */

    addReplyStatusFormat(c, "OK orchestration_id=%lld running (n_donors=%d, target=%s:%d, slots_per_source=%d)",
                         orch_id, 1 + n_peers, recipient_host, recipient_port, n_slots);
}

/* RDMA MIGRATE-WARM <recipient-host> <recipient-port> <slots-per-source>
 *
 * AqRaft Stage 6 (pre-establish): warm up the migration path for THIS donor
 * WITHOUT moving any data or touching any slot state, so a later RDMA MIGRATE-ALL
 * pays only the data-movement window (FLIP → TRANSFER → merge → chain-ack).
 *
 * Warm-up does exactly two state-free things:
 *   1. Open (or reuse) the outbound RDMA link to the recipient — TCP ctrl +
 *      INIT-SERVER + RDMA client QP (rdmaOutboundLinkOpen; cached in
 *      server.rdma_outbound_links across rounds).
 *   2. Pre-register THIS donor's source staging buffers for the first-N owned
 *      slots (rdmaReshardRegisterHelper → Stage-5 one-mmap+one-ibv_reg_mr big-MR
 *      pool + per-slot views). This is the ~16s-pre-Stage-5 / ~0.9s-now donor
 *      ibv_reg_mr work, hoisted out of the timed window.
 *
 * CRITICAL INVARIANT: warm-up performs NO slot-state change. It does NOT send
 * the recipient RDMA REGISTER-BLOCK-SLOTS (which flips the recipient to
 * MIGRATING) and does NOT call slotMigStateSet. ALL MIGRATING/FLIP transitions
 * stay in the in-window migrationWorker (recipient flip in
 * rdmaRegisterBlockSlotsCommand; donor flip + RESHARD-FLIP in the worker). The
 * recipient landing-pool MR is intentionally NOT warmed — it must be a fresh
 * pool per round for r_allocator same-slot safety (see Stage 6 notes).
 *
 * Idempotent: the link is cached and source_buffers[slot] registration skips
 * already-registered slots, so calling WARM repeatedly (or before each round)
 * is cheap. Run it on each donor leader during the pre-reshard pause. */
void rdmaMigrateWarmCommand(client *c) {
    if (c->argc != 5) {
        addReplyError(c, "syntax: RDMA MIGRATE-WARM recipient-host recipient-port slots-per-source");
        return;
    }
    const char *recipient_host = c->argv[2]->ptr;
    long long port_ll, n_slots_ll;
    if (getLongLongFromObject(c->argv[3], &port_ll) != C_OK ||
        port_ll <= 0 || port_ll > 65535) {
        addReplyError(c, "recipient port out of range");
        return;
    }
    if (getLongLongFromObject(c->argv[4], &n_slots_ll) != C_OK ||
        n_slots_ll <= 0 || n_slots_ll > CLUSTER_SLOTS) {
        addReplyError(c, "slots-per-source out of range");
        return;
    }
    int recipient_port = (int) port_ll;
    int n_slots = (int) n_slots_ll;

    /* Pick this donor's first-N owned slots — same selection the in-window
     * worker (startLocalMigration) uses, so we warm exactly the buffers it
     * will reuse. */
    int *chosen = zmalloc((size_t) n_slots * sizeof(int));
    int picked = 0;
    clusterTopoLockRead();
    for (int i = 0; i < CLUSTER_SLOTS && picked < n_slots; i++) {
        if (rdmaMigrationOwnsSlot(i)) chosen[picked++] = i;
    }
    clusterTopoUnlock();
    if (picked < n_slots) {
        zfree(chosen);
        addReplyErrorFormat(c, "self owns only %d slots, asked to warm %d", picked, n_slots);
        return;
    }

    /* 1. Open / reuse the outbound link (state-free transport setup). */
    sds key = sdscatfmt(sdsempty(), "%s:%i", recipient_host, recipient_port);
    rdmaOutboundLink *L = dictFetchValue(server.rdma_outbound_links, key);
    int newly = 0;
    if (L == NULL) {
        L = rdmaOutboundLinkOpen(recipient_host, recipient_port);
        if (L == NULL) {
            sdsfree(key);
            zfree(chosen);
            addReplyError(c, "MIGRATE-WARM: could not establish outbound RDMA link");
            return;
        }
        dictAdd(server.rdma_outbound_links, key, L);   /* dict owns key */
        newly = 1;
    } else {
        sdsfree(key);
    }

    /* 2. Pre-register this donor's source buffers (state-free; no recipient RPC,
     *    no slot-state change). */
    sds err = NULL;
    int registered = 0;
    if (rdmaReshardRegisterHelper(L, chosen, n_slots, &registered, &err) != 0) {
        zfree(chosen);
        addReplyErrorFormat(c, "MIGRATE-WARM: source register failed: %s",
                            err ? err : "?");
        if (err) sdsfree(err);
        return;
    }
    zfree(chosen);

    serverLog(LL_NOTICE,
        "RDMA MIGRATE-WARM: target=%s:%d n_slots=%d registered=%d (%s link) "
        "[AqRaft Stage 6 — connection + source-buffer MR pre-staged, no slot-state]",
        recipient_host, recipient_port, n_slots, registered, newly ? "new" : "cached");
    addReplyStatusFormat(c, "OK warmed target=%s:%d n_slots=%d registered=%d link=%s",
                         recipient_host, recipient_port, n_slots, registered,
                         newly ? "new" : "cached");
}

/* RDMA MIGRATE-ALL-STATUS <orchestration_id> */
void rdmaMigrateAllStatusCommand(client *c) {
    if (c->argc != 3) {
        addReplyError(c, "syntax: RDMA MIGRATE-ALL-STATUS orchestration_id");
        return;
    }
    long long want_id;
    if (getLongLongFromObject(c->argv[2], &want_id) != C_OK || want_id <= 0) {
        addReplyError(c, "orchestration_id must be a positive integer");
        return;
    }
    sds key = sdsfromlonglong(want_id);
    rdmaOrchestration *orch = dictFetchValue(server.rdma_orchestrations, key);
    sdsfree(key);
    if (orch == NULL) {
        addReplyError(c, "no such orchestration_id");
        return;
    }

    pthread_mutex_lock(&orch->mu);
    long long elapsed = (long long) (
        (orch->t_ended ? orch->t_ended : time(NULL)) - orch->t_started);
    /* Reply: 6-element array:
     *   [0] state         (string)
     *   [1] n_donors      (integer)
     *   [2] n_terminal    (integer; count of donors reported terminal)
     *   [3] n_failed      (integer)
     *   [4] elapsed_s     (integer)
     *   [5] per-donor details (array of bulks "endpoint=... mig_id=... state=... err=...")
     */
    addReplyArrayLen(c, 6);
    addReplyBulkCString(c, orchStateName(orch->state));
    addReplyLongLong(c, orch->n_donors);
    addReplyLongLong(c, orch->n_terminal);
    addReplyLongLong(c, orch->n_failed);
    addReplyLongLong(c, elapsed);
    addReplyArrayLen(c, orch->n_donors);
    for (int i = 0; i < orch->n_donors; i++) {
        rdmaOrchestrationDonor *d = &orch->donors[i];
        const char *st = d->terminal
            ? (d->terminal_state == RDMA_MIG_DONE ? "DONE" : "FAILED")
            : "RUNNING";
        sds detail = sdscatfmt(sdsempty(),
            "endpoint=%s mig_id=%I state=%s err=%s",
            d->endpoint ? d->endpoint : "?",
            d->migration_id,
            st,
            d->err ? d->err : "");
        addReplyBulkSds(c, detail);   /* takes ownership of detail */
    }
    pthread_mutex_unlock(&orch->mu);
}

/* RDMA MIGRATE-COMPLETE <orchestration_id> <donor_node_id> <state-string> <applied>
 *
 * Internal callback fired by a donor's migrationWorker on terminal state.
 * The state-string is "DONE" or "FAILED". */
void rdmaMigrateCompleteCommand(client *c) {
    if (c->argc != 6) {
        addReplyError(c, "syntax: RDMA MIGRATE-COMPLETE orch_id donor_node_id state applied");
        return;
    }
    long long want_id, applied;
    if (getLongLongFromObject(c->argv[2], &want_id) != C_OK || want_id <= 0) {
        addReplyError(c, "orch_id must be a positive integer");
        return;
    }
    const char *donor_id = c->argv[3]->ptr;
    const char *st_str   = c->argv[4]->ptr;
    if (getLongLongFromObject(c->argv[5], &applied) != C_OK) applied = 0;

    /* AqRaft Round 2: CHAIN_DURABLE is a non-terminal early signal - the donor
     * reports that the recipient has merged its data and the chain replicated
     * to a majority, with only MGN_INDX_UPD (metadata) still outstanding. It
     * lets the sequencer dispatch the next donor before this donor reaches the
     * full 3-flag DONE. */
    int is_chain_durable = 0;
    rdmaMigrationState st = RDMA_MIG_DONE;
    if (strcasecmp(st_str, "CHAIN_DURABLE") == 0) is_chain_durable = 1;
    else if (strcasecmp(st_str, "DONE") == 0)       st = RDMA_MIG_DONE;
    else if (strcasecmp(st_str, "FAILED") == 0)     st = RDMA_MIG_FAILED;
    else {
        addReplyError(c, "state must be DONE, FAILED or CHAIN_DURABLE");
        return;
    }

    sds key = sdsfromlonglong(want_id);
    rdmaOrchestration *orch = dictFetchValue(server.rdma_orchestrations, key);
    sdsfree(key);
    if (orch == NULL) {
        addReplyError(c, "no such orchestration_id");
        return;
    }

    pthread_mutex_lock(&orch->mu);
    /* Find the donor slot by node_id (set at dispatch for local; for peers
     * we set it on first callback). Match by endpoint if id mismatches. */
    int matched = -1;
    for (int i = 0; i < orch->n_donors; i++) {
        if (orch->donors[i].node_id &&
            strcmp(orch->donors[i].node_id, donor_id) == 0) {
            matched = i; break;
        }
    }
    if (matched < 0) {
        /* First callback from this donor: claim the first slot with
         * NULL node_id (peer slots start unidentified). */
        for (int i = 0; i < orch->n_donors; i++) {
            if (orch->donors[i].node_id == NULL && !orch->donors[i].terminal) {
                orch->donors[i].node_id = sdsnew(donor_id);
                matched = i; break;
            }
        }
    }
    if (matched < 0) {
        pthread_mutex_unlock(&orch->mu);
        serverLog(LL_WARNING,
            "RDMA MIGRATE-COMPLETE: orch_id=%lld donor_id=%s — no slot matched",
            want_id, donor_id);
        addReplyError(c, "no donor slot matched");
        return;
    }
    if (is_chain_durable) {
        /* Non-terminal: just arm the early-dispatch flag the sequencer waits
         * on. Do NOT touch terminal/n_terminal - the full DONE callback (with
         * all 3 durability flags) still arrives later and completes the donor. */
        orch->donors[matched].chain_durable = 1;
        pthread_mutex_unlock(&orch->mu);
        serverLog(LL_NOTICE,
            "RDMA MIGRATE-COMPLETE orch_id=%lld donor=%s CHAIN_DURABLE "
            "(pre-INDX_UPD early-dispatch signal)",
            want_id, donor_id);
        addReply(c, shared.ok);
        return;
    }
    if (!orch->donors[matched].terminal) {
        orch->donors[matched].terminal = 1;
        orch->donors[matched].terminal_state = st;
        orch->donors[matched].applied = applied;
        /* A terminal callback also implies chain-durable (DONE) or supersedes
         * it (FAILED) - keep the sequencer's wait satisfied either way. */
        orch->donors[matched].chain_durable = 1;
        orch->n_terminal++;
        if (st == RDMA_MIG_FAILED) orch->n_failed++;
    }
    if (orch->n_terminal == orch->n_donors) {
        orch->state = (orch->n_failed > 0) ? RDMA_ORCH_FAILED : RDMA_ORCH_DONE;
        orch->t_ended = time(NULL);
        serverLog(LL_NOTICE,
            "RDMA MIGRATE-ALL orch_id=%lld %s n_donors=%d n_failed=%d elapsed=%lds",
            orch->id, orchStateName(orch->state), orch->n_donors,
            orch->n_failed, (long) (orch->t_ended - orch->t_started));
    }
    pthread_mutex_unlock(&orch->mu);
    serverLog(LL_NOTICE,
        "RDMA MIGRATE-COMPLETE orch_id=%lld donor=%s state=%s applied=%lld (terminal=%d/%d)",
        want_id, donor_id, st_str, applied,
        orch->n_terminal, orch->n_donors);
    addReply(c, shared.ok);
}

void rdmaOrchestrationFree(dict *d, void *v) {
    (void) d;
    rdmaOrchestration *orch = v;
    if (orch == NULL) return;
    if (orch->donors) {
        for (int i = 0; i < orch->n_donors; i++) {
            if (orch->donors[i].endpoint) sdsfree(orch->donors[i].endpoint);
            if (orch->donors[i].node_id)  sdsfree(orch->donors[i].node_id);
            if (orch->donors[i].err)      sdsfree(orch->donors[i].err);
        }
        zfree(orch->donors);
    }
    pthread_mutex_destroy(&orch->mu);
    zfree(orch);
}
