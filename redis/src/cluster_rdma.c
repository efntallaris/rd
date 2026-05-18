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
#include "rdma_migration/include/rdma_migration.h"
#include "rdma_migration/allocator.h"   /* r_allocator_log_slot_stats */
#include "kvstore.h"
#include "hiredis.h"
#include <arpa/inet.h>

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

    /* Aqueduct slot-state: flip each slot to MIGRATING with the donor's
     * host:port as the peer endpoint *before* replying +OK, so the donor
     * sees the recipient already in MIGRATING when it sets its own state
     * (closes the PREP race). */
    char donor_endpoint[NET_HOST_PORT_STR_LEN];
    snprintf(donor_endpoint, sizeof(donor_endpoint), "%s:%lld",
             src_host, src_port_ll);
    for (int i = 0; i < n_pairs; i++) {
        slotMigStateSet(job->slot_ids[i], SLOT_STATE_MIGRATING, donor_endpoint);
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
 *   5. RPC: RDMA DONE-SLOTS <slot> ... to trigger the apply.
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

    /* Send DONE-SLOTS to trigger apply. */
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

/* ---- DONE-SLOTS (recipient apply) --------------------------------------- */

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

/* Diagnostic byte dump for RDMA RESHARD-EXEC / rdmaApplySlot. Logs first 32
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
        "RDMA RESHARD-EXEC %s: slot=%d n_entries=%u first32=[%s] last32=[%s]",
        tag, slot, n_entries, hex0, hexN);
}

/* Iterate the donor-staged blocks for a single slot from the recipient's
 * slot-keyed allocator, decode the flat-format entries, and dbAdd new keys
 * into the keyspace. Returns the number of keys added. */
static int rdmaApplySlot(redisDb *db, int slot) {
    int n_blocks = 0;
    char **block_buffers = r_allocator_get_block_buffers_for_slot(slot, &n_blocks);
    if (block_buffers == NULL || n_blocks == 0) return 0;

    int total_added = 0;
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
                    "RDMA apply: malformed entry at slot=%d block=%d entry=%u offset=%zu",
                    slot, b, i, cursor);
                break;
            }
            cursor += consumed;
            if (lookupKeyWrite(db, key) == NULL) {
                /* dbAdd takes the value by ref and consumes it. */
                dbAdd(db, key, &val);
                total_added++;
            } else {
                /* Key already exists; drop the staged value. */
                decrRefCount(val);
            }
            decrRefCount(key);
        }
    }
    zfree(block_buffers);
    return total_added;
}

/* ====================================================================== *
 *  RDMA DONE-SLOTS  (recipient side, chunked main-thread apply)
 *
 *  Reverted from pthread workers because the kvstore isn't thread-safe at
 *  the dict-resize / bucket level vs main-thread READ paths.
 *
 *  Two attempts were made to thread this on branch aqueduct-thread-migration:
 *
 *    Attempt 1: worker started in initServerConfig — silently lost because
 *      that runs BEFORE daemonize() forks. Pthreads don't survive fork.
 *    Attempt 2: worker started in initServer (post-fork) with a
 *      `recipient_apply_mu` that processCommand acquires for commands
 *      hitting importing slots. The worker reached slot ~1120 on the first
 *      DONE-SLOTS batch, then segfaulted at address 0x48 (NULL field
 *      access) — a race against one of the many non-`processCommand`
 *      main-thread paths that also touch `db->keys` / `migrating_slots_to`
 *      / `importing_slots_from`: clusterCron, the RDMA RESHARD-RECV-FLIP
 *      RPC handler, AOF, expiration, replication. The mutex only covered
 *      the command-dispatch path.
 *
 *  Properly threading the apply requires either (a) a broad audit + lock
 *  on every keyspace mutation path, or (b) a kvstore redesign with per-
 *  slot mutexes. Both are out of scope for this PR. Sticking with the
 *  chunked-tick approach in the meantime.
 *
 *  Concretely:
 *    rdmaDoneSlotsCommand: parse slots → push (db, slot_list) onto a
 *      pending list → reply +OK immediately. If no timer is running, arm
 *      a 1 ms time event that calls migrationApplyTick.
 *    migrationApplyTick: take the head pending item, call rdmaApplySlot
 *      for the next SLOTS_PER_TICK slots, advance the per-item index.
 *      When the item is exhausted, free it and try the next. When the list
 *      is empty, return AE_NOMORE to drop the timer.
 * ====================================================================== */

typedef struct pendingApply {
    redisDb *db;
    int *slots;
    int n_slots;
    int idx;             /* next slot index to process */
    long long applied;   /* running total added */
} pendingApply;

#define MIGRATION_SLOTS_PER_TICK 8     /* small enough to yield often,
                                         large enough to make progress */
#define MIGRATION_TICK_DELAY_MS 1      /* 1 ms reschedule; ample for the
                                         event loop to drain client work */

static list *pending_applies = NULL;
static long long migration_apply_timer_id = -1;

static int migrationApplyTick(struct aeEventLoop *el, long long id, void *clientData) {
    UNUSED(el); UNUSED(id); UNUSED(clientData);

    if (pending_applies == NULL || listLength(pending_applies) == 0) {
        migration_apply_timer_id = -1;
        return AE_NOMORE;
    }

    listNode *ln = listFirst(pending_applies);
    pendingApply *p = listNodeValue(ln);

    int processed = 0;
    while (processed < MIGRATION_SLOTS_PER_TICK && p->idx < p->n_slots) {
        p->applied += rdmaApplySlot(p->db, p->slots[p->idx]);
        p->idx++;
        processed++;
    }

    if (p->idx >= p->n_slots) {
        serverLog(LL_NOTICE,
            "RDMA DONE-SLOTS (chunked): finished %d slots, applied %lld keys total",
            p->n_slots, p->applied);
        zfree(p->slots);
        zfree(p);
        listDelNode(pending_applies, ln);
    }

    return MIGRATION_TICK_DELAY_MS;
}

/* ====================================================================== *
 *  Phase 4d types + statics (definitions used by rdmaDoneSlotsCommand
 *  below and the apply-thread / status code further down).               *
 * ====================================================================== */

#include <semaphore.h>
#include <fcntl.h>
#include <unistd.h>

/* Same value bio.c uses for its worker threads. */
#define APPLY_THREAD_STACK_SIZE (1024*1024*4)

/* Phase 4d callback-RPC variant: no shared globals needed on the recipient
 * for the deferred REGISTER-BLOCK-SLOTS path — each worker thread owns its
 * own job struct end-to-end (parse → ibv_reg_mr → callback hiredis to source
 * → free) without main-thread coordination after enqueue. */

/* dictType for apply_batches_by_key: sds key, applyBatch* value (free
 * the key sds, leave the value to the dispose path). */
extern dictType sdsHashDictType;

typedef enum {
    APPLY_QUEUED   = 0,
    APPLY_RUNNING  = 1,
    APPLY_DONE     = 2,
    APPLY_FAILED   = 3
} applyBatchState;

typedef struct applyBatch {
    redisDb *db;
    int     *slots;
    int      n_slots;
    char     src_node_id[CLUSTER_NAMELEN + 1];
    long long src_mig_id;
    _Atomic int          idx;
    _Atomic long long    applied;
    _Atomic int          state;
    sds                  err;
    pthread_mutex_t      err_mu;
    time_t               t_started;
    time_t               t_ended;
} applyBatch;

#define APPLY_RING_CAPACITY 64u

static applyBatch *apply_ring[APPLY_RING_CAPACITY];
static _Atomic uint64_t apply_ring_head = 0;
static _Atomic uint64_t apply_ring_tail = 0;
static sem_t apply_wake;
static pthread_t apply_thread_tid;
static _Atomic int apply_thread_shutdown = 0;
static int apply_thread_started = 0;

static int apply_dispose_pipe[2] = {-1, -1};
static pthread_mutex_t apply_dispose_mu = PTHREAD_MUTEX_INITIALIZER;
static list *apply_dispose_list = NULL;

static dict *apply_batches_by_key = NULL;
static pthread_mutex_t apply_batches_mu = PTHREAD_MUTEX_INITIALIZER;

static sds applyBatchKey(const char *src_node_id, long long src_mig_id) {
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

    /* Allocate the applyBatch up front; ownership transfers to the apply
     * thread on successful enqueue. */
    applyBatch *b = zmalloc(sizeof(*b));
    b->db = c->db;
    b->n_slots = n_slots;
    b->slots = zmalloc((size_t) n_slots * sizeof(int));
    memcpy(b->src_node_id, src_node_id, strlen(src_node_id));
    b->src_node_id[strlen(src_node_id)] = '\0';
    b->src_mig_id = src_mig_id;
    atomic_store(&b->idx, 0);
    atomic_store(&b->applied, 0);
    atomic_store(&b->state, APPLY_QUEUED);
    b->err = NULL;
    pthread_mutex_init(&b->err_mu, NULL);
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

    /* Index by (src_node_id, src_mig_id) for APPLY-STATUS lookups. The dict
     * stores the batch under the key sds; we drop+re-add if a previous
     * batch with the same key is still hanging around (rare; admin retry). */
    if (apply_batches_by_key != NULL && strlen(src_node_id) == CLUSTER_NAMELEN) {
        sds key = applyBatchKey(src_node_id, src_mig_id);
        pthread_mutex_lock(&apply_batches_mu);
        dictReplace(apply_batches_by_key, key, b);
        pthread_mutex_unlock(&apply_batches_mu);
    }

    /* Try to enqueue on the SPSC ring. */
    int enqueued = 0;
    if (apply_thread_started) {
        uint64_t head = atomic_load_explicit(&apply_ring_head, memory_order_relaxed);
        uint64_t tail = atomic_load_explicit(&apply_ring_tail, memory_order_acquire);
        if (head - tail < APPLY_RING_CAPACITY) {
            apply_ring[head & (APPLY_RING_CAPACITY - 1)] = b;
            atomic_store_explicit(&apply_ring_head, head + 1, memory_order_release);
            sem_post(&apply_wake);
            enqueued = 1;
        }
    }

    if (!enqueued) {
        /* Ring full or thread not running — fall back to the chunked main-
         * thread tick path. Convert applyBatch into pendingApply and let
         * migrationApplyTick handle it. */
        pendingApply *p = zmalloc(sizeof(*p));
        p->db = c->db;
        p->n_slots = n_slots;
        p->slots = zmalloc((size_t) n_slots * sizeof(int));
        memcpy(p->slots, b->slots, (size_t) n_slots * sizeof(int));
        p->idx = 0;
        p->applied = 0;
        if (pending_applies == NULL) pending_applies = listCreate();
        listAddNodeTail(pending_applies, p);
        if (migration_apply_timer_id == -1) {
            migration_apply_timer_id = aeCreateTimeEvent(server.el,
                MIGRATION_TICK_DELAY_MS, migrationApplyTick, NULL, NULL);
        }
        /* The applyBatch we allocated for tracking is left in the index
         * dict; mark it as APPLY_DONE inline so APPLY-STATUS reports it
         * correctly once the chunked tick finishes the work. Note: this
         * inline state transition is unsynchronized with the actual
         * chunked apply progress — it's a coarse "queued / done" view in
         * the fallback path. The fallback is taken only on overflow, so
         * this is best-effort. */
        atomic_store(&b->state, APPLY_DONE);
        b->t_ended = time(NULL);
        serverLog(LL_NOTICE,
            "RDMA DONE-SLOTS (chunked fallback): queued %d slots; %lu in-flight",
            n_slots, listLength(pending_applies));
    } else {
        serverLog(LL_NOTICE,
            "RDMA DONE-SLOTS (apply-thread): enqueued %d slots from %.*s mig_id=%lld",
            n_slots, CLUSTER_NAMELEN, src_node_id, src_mig_id);
    }
    addReply(c, shared.ok);
}

/* Legacy stubs kept for server.c init-time call; the new recipientApply
 * thread API below replaces these in functionality. */
void recipientApplyWorkerStart(void)  {}
void recipientApplyWorkerStop(void)   {}
void recipientApplyMuLock(void)       {}
void recipientApplyMuUnlock(void)     {}

/* ====================================================================== *
 *  Phase 4d: Recipient apply thread + SPSC lock-free ring                 *
 *                                                                          *
 *  Replaces the main-thread chunked migrationApplyTick. Producer is the    *
 *  main event loop (rdmaDoneSlotsCommand) when a DONE-SLOTS RPC lands;     *
 *  consumer is a single dedicated pthread that walks each batch's slots    *
 *  under clusterSlotLockWrite(slot) and calls rdmaApplySlot — the same     *
 *  apply primitive the chunked path uses, just on a different thread.     *
 *                                                                          *
 *  Concurrency invariants:                                                 *
 *    - Single producer (main thread) → single consumer (apply thread).     *
 *    - Ring slots are applyBatch* pointers; ownership transfers on enqueue.*
 *    - apply thread sets cluster_slot_lock_held_by_thread = 1 around the   *
 *      slot wrlock so Path B keyspace wraps in db.c skip their own lock    *
 *      (no same-thread recursive rwlock).                                  *
 *    - applyBatch is freed by the apply thread via a 1-byte pipe message   *
 *      to the main thread; main thread does the zfree to keep             *
 *      malloc/free pairs single-threaded (jemalloc arena friendliness).   *
 *    - On ring overflow the producer falls back to the existing chunked    *
 *      migrationApplyTick path so DONE-SLOTS never blocks.                 *
 * ====================================================================== */

/* Drain the SPSC ring; called by recipientApplyThreadMain after sem_wait. */
static void drainApplyRing(void) {
    uint64_t tail = atomic_load_explicit(&apply_ring_tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&apply_ring_head, memory_order_acquire);
    while (tail != head) {
        applyBatch *b = apply_ring[tail & (APPLY_RING_CAPACITY - 1)];
        atomic_store_explicit(&b->state, APPLY_RUNNING, memory_order_relaxed);
        serverLog(LL_NOTICE,
            "RDMA apply-thread: starting batch from %.*s mig_id=%lld n_slots=%d",
            CLUSTER_NAMELEN, b->src_node_id, b->src_mig_id, b->n_slots);

        /* PHASE 4d STUB v2: NO per-slot work at all. No slot wrlocks. No
         * loop. Just sleep for a token duration to simulate "the apply took
         * some time" and mark all slots applied. If the YCSB dip persists
         * even with this — there's no apply-thread keyspace touch, no slot
         * lock contention — then the dip is NOT caused by the apply thread.
         * Either the RECV-FLIP topology wrlock (contributor #1) or the
         * client-side TCP recovery aftermath (contributor #3) carries it. */
        usleep(100000);  /* 100 ms — much less than the 1.5 s "real work" */
        atomic_store_explicit(&b->idx, b->n_slots, memory_order_release);
        /* Stub: no keys applied; b->applied stays at 0. */

        atomic_store_explicit(&b->state, APPLY_DONE, memory_order_release);
        b->t_ended = time(NULL);
        serverLog(LL_NOTICE,
            "RDMA apply-thread: batch DONE from %.*s mig_id=%lld n_slots=%d (stub)",
            CLUSTER_NAMELEN, b->src_node_id, b->src_mig_id, b->n_slots);

        /* Notify main thread to free old completed batches. We push onto the
         * dispose list under a mutex and tickle the pipe. */
        pthread_mutex_lock(&apply_dispose_mu);
        listAddNodeTail(apply_dispose_list, b);
        pthread_mutex_unlock(&apply_dispose_mu);
        char tick = 1;
        ssize_t w = write(apply_dispose_pipe[1], &tick, 1);
        (void) w;  /* best-effort; main thread will catch up on next tickle. */

        atomic_store_explicit(&apply_ring_tail, tail + 1, memory_order_release);
        tail++;
        head = atomic_load_explicit(&apply_ring_head, memory_order_acquire);
    }
}

static void *recipientApplyThreadMain(void *arg) {
    UNUSED(arg);
    while (!atomic_load_explicit(&apply_thread_shutdown, memory_order_acquire)) {
        sem_wait(&apply_wake);
        if (atomic_load_explicit(&apply_thread_shutdown, memory_order_acquire))
            break;
        drainApplyRing();
    }
    return NULL;
}

/* Main-thread pipe handler: free completed batches. The applyBatch records
 * are kept in apply_batches_by_key (the APPLY-STATUS handler may still read
 * them); we only ack the pipe tickle here so the apply thread can write more.
 * Actual eviction of stale records is via a TTL sweep in clusterCron — kept
 * simple for now: keep last N completed batches indexed by key. */
static void applyDisposePipeHandler(aeEventLoop *el, int fd, void *priv, int mask) {
    UNUSED(el); UNUSED(priv); UNUSED(mask);
    char buf[64];
    while (read(fd, buf, sizeof(buf)) > 0) { /* drain */ }
    /* The completed batches stay in apply_batches_by_key until cleanup.
     * For now we don't free here — APPLY-STATUS needs them. Future:
     * TTL sweep based on (now - b->t_ended). */
}

void recipientApplyThreadStart(void) {
    if (apply_thread_started) return;

    sem_init(&apply_wake, 0, 0);

    if (pipe(apply_dispose_pipe) != 0) {
        serverLog(LL_WARNING, "recipientApplyThreadStart: pipe() failed: %s",
                  strerror(errno));
        return;
    }
    /* Non-blocking on the read end so the handler's drain loop terminates. */
    int flags = fcntl(apply_dispose_pipe[0], F_GETFL, 0);
    fcntl(apply_dispose_pipe[0], F_SETFL, flags | O_NONBLOCK);
    if (aeCreateFileEvent(server.el, apply_dispose_pipe[0], AE_READABLE,
                          applyDisposePipeHandler, NULL) == AE_ERR) {
        serverLog(LL_WARNING, "recipientApplyThreadStart: aeCreateFileEvent failed");
        close(apply_dispose_pipe[0]);
        close(apply_dispose_pipe[1]);
        apply_dispose_pipe[0] = apply_dispose_pipe[1] = -1;
        return;
    }

    apply_dispose_list  = listCreate();
    /* sdsHashDictType: sds key (we free it via dictSdsDestructor), opaque
     * value (we don't auto-free it — the dispose path owns the lifetime). */
    apply_batches_by_key = dictCreate(&sdsHashDictType);

    /* Register-job pipe + list for deferred REGISTER-BLOCK-SLOTS replies. */
    /* No infra init for the REGISTER-BLOCK-SLOTS path — see comment near the
     * registerJob struct: each worker is self-contained. */

    pthread_attr_t attr;
    size_t stacksize;
    pthread_attr_init(&attr);
    pthread_attr_getstacksize(&attr, &stacksize);
    if (!stacksize) stacksize = 1;
    while (stacksize < APPLY_THREAD_STACK_SIZE) stacksize *= 2;
    pthread_attr_setstacksize(&attr, stacksize);

    int err = pthread_create(&apply_thread_tid, &attr, recipientApplyThreadMain, NULL);
    pthread_attr_destroy(&attr);
    if (err) {
        serverLog(LL_WARNING,
                  "recipientApplyThreadStart: pthread_create failed: %s",
                  strerror(err));
        return;
    }
    apply_thread_started = 1;
    serverLog(LL_NOTICE, "Recipient apply thread started (Phase 4d).");
}

void recipientApplyThreadStop(void) {
    if (!apply_thread_started) return;
    atomic_store_explicit(&apply_thread_shutdown, 1, memory_order_release);
    sem_post(&apply_wake);
    pthread_join(apply_thread_tid, NULL);
    apply_thread_started = 0;
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

    for (int i = 0; i < job->n_pairs; i++) {
        int slot = job->slot_ids[i];
        int n = job->n_blocks_per_slot[i];
        for (int k = 0; k < n; k++) {
            void *block_ptr = r_allocator_alloc_new_empty_block(slot);
            if (block_ptr == NULL) {
                job->has_error = 1;
                snprintf(job->err_msg, sizeof(job->err_msg),
                    "r_allocator_alloc_new_empty_block returned NULL (slot=%d)", slot);
                goto deliver;
            }
            struct rdmamig_buffer *rb = rdmamig_buffer_create(
                rdmamig_server_cm_id(job->conn->s),
                block_ptr, RDMAMIG_BLOCK_SIZE_BYTES, 0);
            if (rb == NULL) {
                job->has_error = 1;
                snprintf(job->err_msg, sizeof(job->err_msg),
                    "rdmamig_buffer_create failed (slot=%d block=%d)", slot, k);
                goto deliver;
            }
            job->result[job->total_buffers].ptr  = (uint64_t) block_ptr;
            job->result[job->total_buffers].rkey = rdmamig_buffer_rkey(rb);
            job->total_buffers++;
        }
    }

deliver:
    {
        serverLog(LL_NOTICE,
            "RDMA REGISTER-RESULT: register_id=%s total_buffers=%d, dialing back to %s:%d",
            job->register_id, job->total_buffers, job->src_host, job->src_port);

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

/* RDMA APPLY-STATUS <src_node_id> <src_mig_id>
 *
 * Source-polled status of a recipient apply batch. Returns a 6-element
 * reply: state (string: queued|running|done|failed), idx (slots applied
 * so far), n_slots, applied_keys, elapsed_seconds, err (empty if none). */
static const char *applyStateName(applyBatchState s) {
    switch (s) {
    case APPLY_QUEUED:  return "queued";
    case APPLY_RUNNING: return "running";
    case APPLY_DONE:    return "done";
    case APPLY_FAILED:  return "failed";
    }
    return "unknown";
}

void rdmaApplyStatusCommand(client *c) {
    if (c->argc != 4) {
        addReplyError(c, "syntax: RDMA APPLY-STATUS src_node_id src_mig_id");
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

    sds key = applyBatchKey(src_id, mig_id);
    pthread_mutex_lock(&apply_batches_mu);
    applyBatch *b = apply_batches_by_key ? dictFetchValue(apply_batches_by_key, key) : NULL;
    pthread_mutex_unlock(&apply_batches_mu);
    sdsfree(key);

    if (b == NULL) {
        addReplyError(c, "no such apply batch");
        return;
    }

    int state    = atomic_load_explicit(&b->state, memory_order_acquire);
    int idx      = atomic_load_explicit(&b->idx,   memory_order_relaxed);
    long long ap = atomic_load_explicit(&b->applied, memory_order_relaxed);
    pthread_mutex_lock(&b->err_mu);
    sds err_copy = b->err ? sdsdup(b->err) : sdsempty();
    pthread_mutex_unlock(&b->err_mu);

    long long elapsed = (long long) ((b->t_ended ? b->t_ended : time(NULL)) - b->t_started);

    addReplyArrayLen(c, 6);
    addReplyBulkCString(c, applyStateName((applyBatchState) state));
    addReplyLongLong(c, idx);
    addReplyLongLong(c, b->n_slots);
    addReplyLongLong(c, ap);
    addReplyLongLong(c, elapsed);
    addReplyBulkSds(c, err_copy);
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
    if (server.cluster == NULL || server.cluster->myself == NULL) {
        addReplyError(c, "cluster mode not enabled on this node");
        return;
    }
    int *chosen = zmalloc((size_t) n_slots * sizeof(int));
    int picked = 0;
    for (int i = 0; i < CLUSTER_SLOTS && picked < n_slots; i++) {
        if (server.cluster->slots[i] == server.cluster->myself) {
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
 *  RDMA RESHARD-EXEC  (Phase 2: data-plane transfer)
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
 * notification. rdmaReshardExecCommand replies +OK immediately and this
 * thread does the network-bound work off the main event loop so the source
 * can keep serving traffic. */
struct reshardExecArgs {
    rdmaOutboundLink *L;
    redisDb *db;
    int *chosen;       /* owned; freed by the worker */
    int n_slots;
};

static void *reshardExecWorker(void *p) {
    struct reshardExecArgs *a = (struct reshardExecArgs *) p;

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
                "RDMA RESHARD-EXEC TX (thr): slot=%d n_entries=%u rdma_bytes=%d",
                slot, n_entries, RDMAMIG_BLOCK_SIZE_BYTES);
        }
        int rc = rdmamig_client_post_write(a->L->source_buffers[slot], staging,
                                           a->L->buffers[slot].ptr,
                                           a->L->buffers[slot].rkey,
                                           RDMAMIG_BLOCK_SIZE_BYTES);
        if (rc != 0) {
            errs++;
            serverLog(LL_WARNING,
                "RDMA RESHARD-EXEC (thr): slot=%d post_write failed rc=%d", slot, rc);
            continue;
        }
        int wc_rc = rdmamig_client_wait_send(a->L->client);
        if (wc_rc < 0) {
            errs++;
            serverLog(LL_WARNING,
                "RDMA RESHARD-EXEC (thr): slot=%d wait_send failed rc=%d", slot, wc_rc);
            continue;
        }
        total_bytes += RDMAMIG_BLOCK_SIZE_BYTES;
        serverLog(LL_NOTICE,
            "RDMA RESHARD-EXEC (thr): slot=%d entries=%u bytes=%d rc=0",
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
            "RDMA RESHARD-EXEC (thr): DONE-SLOTS reply not OK (data may still have landed)");
    }
    if (r) freeReplyObject(r);

    pthread_mutex_unlock(&a->L->mu);

    int first = a->chosen[0], last = a->chosen[a->n_slots - 1];
    serverLog(LL_NOTICE,
        "RDMA RESHARD-EXEC (thr): finished n=%d slots %d..%d bytes=%zu errs=%d",
        a->n_slots, first, last, total_bytes, errs);

    zfree(a->chosen);
    zfree(a);
    return NULL;
}

void rdmaReshardExecCommand(client *c) {
    if (c->argc != 5) {
        addReplyError(c, "syntax: RDMA RESHARD-EXEC recipient-host recipient-port n-slots");
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

    if (server.cluster == NULL || server.cluster->myself == NULL) {
        addReplyError(c, "cluster mode not enabled on this node");
        return;
    }

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
    struct reshardExecArgs *a = zmalloc(sizeof(*a));
    a->L = L;
    a->db = c->db;
    a->chosen = chosen;        /* worker owns it now */
    a->n_slots = n_slots;

    pthread_t t;
    if (pthread_create(&t, NULL, reshardExecWorker, a) != 0) {
        zfree(a->chosen);
        zfree(a);
        addReplyError(c, "RESHARD-EXEC: pthread_create failed");
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
 *       this protocol wants: source keeps the keys until RESHARD-EXEC
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

    if (server.cluster == NULL || server.cluster->myself == NULL) {
        addReplyError(c, "cluster mode not enabled on this node");
        return;
    }

    /* Pick the same N slots as RESHARD: walk self-owned, lowest first. */
    int *chosen = zmalloc((size_t) n_slots * sizeof(int));
    int picked = 0;
    for (int i = 0; i < CLUSTER_SLOTS && picked < n_slots; i++) {
        if (server.cluster->slots[i] == server.cluster->myself) {
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

    /* Look up the local clusterNode * for the recipient. The recipient must
     * be known to us via cluster gossip (it joined the cluster as a normal
     * master earlier via CLUSTER MEET). */
    clusterNode *recipient_node = clusterLookupNode(recipient_id, CLUSTER_NAMELEN);
    if (recipient_node == NULL) {
        pthread_mutex_unlock(&L->mu);
        zfree(chosen);
        addReplyErrorFormat(c,
            "RESHARD-FLIP: recipient %s not in local cluster view "
            "(have you done CLUSTER MEET?)", recipient_id);
        return;
    }

    /* Our own (source's) node id — recipient uses it for SETSLOT IMPORTING. */
    char src_id[CLUSTER_NAMELEN + 1];
    memcpy(src_id, server.cluster->myself->name, CLUSTER_NAMELEN);
    src_id[CLUSTER_NAMELEN] = '\0';

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
     *    Multi-slot mutation; topology wrlock for the whole loop. */
    clusterTopoLockWrite();
    for (int i = 0; i < n_slots; i++) {
        int slot = chosen[i];
        clusterDelSlot(slot);
        clusterAddSlot(recipient_node, slot);
        server.cluster->migrating_slots_to[slot] = recipient_node;
        flipped++;
        serverLog(LL_NOTICE,
            "RDMA RESHARD-FLIP: slot=%d ownership flipped to %s "
            "(source locked, migrating_slots_to set)",
            slot, recipient_id);
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

    if (server.cluster == NULL || server.cluster->myself == NULL) {
        addReplyError(c, "cluster mode not enabled on this node");
        return;
    }

    clusterNode *src_node = clusterLookupNode(src_id_str, CLUSTER_NAMELEN);
    if (src_node == NULL) {
        addReplyErrorFormat(c,
            "RESHARD-RECV-FLIP: source %s not in local cluster view "
            "(have you done CLUSTER MEET?)", src_id_str);
        return;
    }

    int flipped = 0;
    /* Multi-slot mutation across user-supplied slot list. Topology wrlock
     * for the whole loop; the early-return on bad-slot must drop the lock
     * (pre-existing bug: slots already flipped before the bad one stay
     * flipped — not addressed here). */
    clusterTopoLockWrite();
    for (int j = 3; j < c->argc; j++) {
        long long slot_ll;
        if (getLongLongFromObject(c->argv[j], &slot_ll) != C_OK ||
            slot_ll < 0 || slot_ll >= CLUSTER_SLOTS) {
            clusterTopoUnlock();
            addReplyErrorFormat(c, "RESHARD-RECV-FLIP: bad slot %s",
                                (char *) c->argv[j]->ptr);
            return;
        }
        int slot = (int) slot_ll;

        /* 1. Set importing marker first — the v2 -ASK-on-miss predicate
         *    in getNodeByQuery() keys off getImportingSlotSource(slot). */
        server.cluster->importing_slots_from[slot] = src_node;

        /* 2. Take ownership of the slot. */
        clusterDelSlot(slot);
        clusterAddSlot(server.cluster->myself, slot);

        flipped++;

        serverLog(LL_NOTICE,
            "RDMA RESHARD-RECV-FLIP: slot=%d imported from %s, ownership claimed",
            slot, src_id_str);
    }
    clusterTopoUnlock();

    /* Aqueduct slot-state: recipient now owns the slots cleanly, clear
     * MIGRATING and return to STABLE. Done outside the topology wrlock to
     * avoid deadlocking against slotMigStateSet's internal rdlock acquire. */
    for (int j = 3; j < c->argc; j++) {
        long long slot_ll;
        if (getLongLongFromObject(c->argv[j], &slot_ll) != C_OK) continue;
        if (slot_ll < 0 || slot_ll >= CLUSTER_SLOTS) continue;
        slotMigStateSet((int) slot_ll, SLOT_STATE_STABLE, NULL);
    }

    addReplyStatusFormat(c, "OK %d slots flipped on recipient", flipped);
}


/* ======================================================================== *
 *                                                                          *
 *  RDMA MIGRATE — single autonomous-thread migration                       *
 *                                                                          *
 *  Replaces the four-command Ansible-driven sequence (MIGRATE-PREP →       *
 *  RESHARD → RESHARD-FLIP → RESHARD-EXEC) with a single command that       *
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
 *  RESHARD-EXEC) remain available, calling shared helpers below.           *
 * ======================================================================== */

#include "cluster_legacy.h"  /* for clusterState slot_locks + clusterAddSlot/Del proto */

/* Phase 4d: TLS flag — see cluster.h. Default 0 on every thread, set to 1
 * by recipientApplyThreadMain while it holds clusterSlotLockWrite(s). */
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

    /* Figure out which host:port the recipient should call back. */
    const char *src_host = (server.cluster && server.cluster->myself &&
                            server.cluster->myself->ip[0]) ?
                           server.cluster->myself->ip : "127.0.0.1";
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
    for (int i = 0; i < n_slots; i++) {
        int slot = chosen[i];
        if (L->source_buffers[slot] != NULL) {
            /* already registered — count as progress and skip */
            if (progress) (*progress)++;
            continue;
        }
        void *staging = zmalloc(RDMAMIG_BLOCK_SIZE_BYTES);
        if (staging == NULL) {
            pthread_mutex_unlock(&L->mu);
            if (err_out) *err_out = sdsnew("zmalloc failed for staging buffer");
            return -1;
        }
        memset(staging, 0, RDMAMIG_BLOCK_SIZE_BYTES);
        struct rdmamig_buffer *rb = rdmamig_buffer_create(
            rdmamig_client_cm_id(L->client),
            staging, RDMAMIG_BLOCK_SIZE_BYTES, 0);
        if (rb == NULL) {
            zfree(staging);
            pthread_mutex_unlock(&L->mu);
            if (err_out) *err_out = sdsnew("rdmamig_buffer_create failed");
            return -1;
        }
        L->source_buffers[slot] = rb;
        if (progress) (*progress)++;
        /* Same per-slot log line as the legacy handler so existing log
         * scrapers and the throughput-vs-registration cross-reference
         * still work. */
        serverLog(LL_NOTICE,
                  "RDMA RESHARD: registered slot=%d buf=%p rkey=0x%x size=%d",
                  slot, staging, rdmamig_buffer_rkey(rb), RDMAMIG_BLOCK_SIZE_BYTES);
    }
    pthread_mutex_unlock(&L->mu);
    return 0;
}

/* FLIP-helper: hiredis RECV-FLIP RPC + local slot-table mutation under the
 * topology wrlock. Mirrors rdmaReshardFlipCommand (lines ~1216-1322). */
static int rdmaReshardFlipHelper(rdmaOutboundLink *L,
                                  const int *chosen, int n_slots,
                                  sds *err_out) {
    pthread_mutex_lock(&L->mu);

    redisReply *r = redisCommand(L->ctrl, "CLUSTER MYID");
    if (r == NULL || r->type != REDIS_REPLY_STRING ||
        (int) strlen(r->str) < CLUSTER_NAMELEN) {
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
    char recipient_id[CLUSTER_NAMELEN + 1];
    memcpy(recipient_id, r->str, CLUSTER_NAMELEN);
    recipient_id[CLUSTER_NAMELEN] = '\0';
    freeReplyObject(r);

    clusterNode *recipient_node = clusterLookupNode(recipient_id, CLUSTER_NAMELEN);
    if (recipient_node == NULL) {
        if (err_out) *err_out = sdscatfmt(sdsempty(),
            "FLIP: recipient %s not in local cluster view (CLUSTER MEET?)",
            recipient_id);
        pthread_mutex_unlock(&L->mu);
        return -1;
    }

    char src_id[CLUSTER_NAMELEN + 1];
    memcpy(src_id, server.cluster->myself->name, CLUSTER_NAMELEN);
    src_id[CLUSTER_NAMELEN] = '\0';

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
        clusterDelSlot(slot);
        clusterAddSlot(recipient_node, slot);
        server.cluster->migrating_slots_to[slot] = recipient_node;
        serverLog(LL_NOTICE,
            "RDMA MIGRATE worker: slot=%d ownership flipped to %s",
            slot, recipient_id);
    }
    clusterTopoUnlock();

    pthread_mutex_unlock(&L->mu);
    return 0;
}

/* EXEC-helper: encode + RDMA-WRITE + DONE-SLOTS. Mirrors reshardExecWorker
 * (lines ~974-1048). */
static int rdmaReshardExecHelper(rdmaOutboundLink *L, redisDb *db,
                                  const int *chosen, int n_slots,
                                  long long mig_id, sds *err_out) {
    pthread_mutex_lock(&L->mu);

    size_t total_bytes = 0;
    int errs = 0;
    for (int i = 0; i < n_slots; i++) {
        int slot = chosen[i];
        char *staging = rdmamig_buffer_data(L->source_buffers[slot]);
        uint32_t n_entries = rdmaEncodeSlotEntries(db, slot, staging,
                                                   RDMAMIG_BLOCK_SIZE_BYTES);
        rdmaDebugDumpSlotBytes("SRC", slot, staging);
        if (server.rdma_reshard_debug_bytes) {
            r_allocator_log_slot_stats(slot);
            serverLog(LL_NOTICE,
                "RDMA MIGRATE worker TX: slot=%d n_entries=%u rdma_bytes=%d",
                slot, n_entries, RDMAMIG_BLOCK_SIZE_BYTES);
        }
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
        int wc_rc = rdmamig_client_wait_send(L->client);
        if (wc_rc < 0) {
            errs++;
            serverLog(LL_WARNING,
                "RDMA MIGRATE worker: slot=%d wait_send failed rc=%d", slot, wc_rc);
            continue;
        }
        total_bytes += RDMAMIG_BLOCK_SIZE_BYTES;
        serverLog(LL_NOTICE,
            "RDMA MIGRATE worker: slot=%d entries=%u bytes=%d rc=0",
            slot, n_entries, RDMAMIG_BLOCK_SIZE_BYTES);
    }

    /* Notify recipient (Phase 4d):
     *   "RDMA DONE-SLOTS <src_node_id> <src_mig_id> <s1> ... <sN>"
     * The two new positional args let the recipient identify the batch in
     * its apply-status tracking dict so the source can later poll
     * RDMA APPLY-STATUS. The recipient also accepts the legacy 2-arg form
     * for back-compat (no tracking).                                       */
    char src_id[CLUSTER_NAMELEN + 1];
    if (server.cluster != NULL && server.cluster->myself != NULL) {
        memcpy(src_id, server.cluster->myself->name, CLUSTER_NAMELEN);
    } else {
        memset(src_id, 0, sizeof(src_id));
    }
    src_id[CLUSTER_NAMELEN] = '\0';
    char migid_buf[24];
    int migid_len = snprintf(migid_buf, sizeof(migid_buf), "%lld", mig_id);

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

    pthread_mutex_unlock(&L->mu);

    serverLog(LL_NOTICE,
        "RDMA MIGRATE worker: EXEC finished n=%d bytes=%zu errs=%d",
        n_slots, total_bytes, errs);

    if (errs > 0 && err_out) {
        *err_out = sdscatfmt(sdsempty(), "EXEC: %i slot writes failed", errs);
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
    case RDMA_MIG_EXECUTING:   return "EXECUTING";
    case RDMA_MIG_APPLYING:    return "APPLYING";
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
}

static void *migrationWorker(void *arg) {
    rdmaMigration *mig = (rdmaMigration *) arg;
    sds err = NULL;

    serverLog(LL_NOTICE,
        "RDMA MIGRATE worker: started id=%lld addr=%s n_slots=%d",
        mig->id, mig->addr, mig->n_slots);

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

    /* Option 2: FLIP is deferred to after the recipient has applied the data.
     * The old order (FLIP before EXEC) created a window where the recipient
     * owned the slots but didn't yet have any data — clients hitting either
     * end during that window triggered a -MOVED / -ASK ping-pong that stalled
     * YCSB for ~5 s per migration. New order keeps the source serving reads
     * from its live keyspace all the way through transfer + recipient-apply;
     * FLIP is the final microsecond ownership swap once the recipient
     * already has the data.                                                  */

    /* EXECUTING: RDMA-WRITE + DONE-SLOTS. Source still owns the slots, so
     * client traffic continues to hit the source's live keyspace.           */
    migSetState(mig, RDMA_MIG_EXECUTING);
    /* Use db 0 — same as the legacy reshardExecWorker path (DB selection
     * comes from the dispatching client; cluster mode is single-DB). */
    if (rdmaReshardExecHelper(mig->L, &server.db[0], mig->chosen, mig->n_slots,
                              mig->id, &err) != 0) {
        migFail(mig, err);
        return NULL;
    }

    /* APPLYING: poll the recipient's RDMA APPLY-STATUS until the apply
     * thread on the recipient reports the batch as done. The recipient
     * acknowledged DONE-SLOTS as soon as it queued the batch onto its
     * SPSC ring; the actual apply runs asynchronously on the recipient's
     * apply thread. We tail the status here so the source-side
     * RDMA_MIG_DONE transition genuinely means "data is in the recipient's
     * keyspace", not just "the recipient acked the buffer transfer."
     *
     * Poll cadence 10 ms; bails on FAILED. Retries on transient hiredis
     * errors (recipient busy). Upper bound: 60 s (~6000 polls). For the
     * typical 4095-slot batch the apply completes in ~tens to hundreds of
     * milliseconds; polling overhead is negligible. */
    migSetState(mig, RDMA_MIG_APPLYING);
    {
        char src_id[CLUSTER_NAMELEN + 1];
        memcpy(src_id, server.cluster->myself->name, CLUSTER_NAMELEN);
        src_id[CLUSTER_NAMELEN] = '\0';

        const int max_polls = 6000;  /* ~60 s at 10 ms each */
        int polls = 0;
        int done = 0;
        sds apply_err = NULL;
        while (polls < max_polls && !done) {
            pthread_mutex_lock(&mig->L->mu);
            redisReply *r = redisCommand(mig->L->ctrl,
                "RDMA APPLY-STATUS %s %lld", src_id, mig->id);
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
            if (r->type == REDIS_REPLY_ARRAY && r->elements == 6) {
                const char *state_str = r->element[0]->str;
                if (state_str && strcmp(state_str, "done") == 0) done = 1;
                else if (state_str && strcmp(state_str, "failed") == 0) {
                    apply_err = sdscatfmt(sdsempty(),
                        "recipient apply FAILED: %s",
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
        if (apply_err) {
            migFail(mig, apply_err);
            return NULL;
        }
        if (!done) {
            migFail(mig, sdsnew("recipient apply timed out after 60s of polling"));
            return NULL;
        }
    }

    /* FLIPPING (Option 2): now that the data is on the recipient, atomically
     * swap ownership. RECV-FLIP runs on the recipient, then we update our
     * local slot table. By the time clients see the MOVED, the recipient
     * already has the data — no -MOVED / -ASK ping-pong, no YCSB dip from
     * topology refresh storms.                                              */
    migSetState(mig, RDMA_MIG_FLIPPING);
    if (rdmaReshardFlipHelper(mig->L, mig->chosen, mig->n_slots, &err) != 0) {
        migFail(mig, err);
        return NULL;
    }

    /* Aqueduct slot-state: donor flips MIGRATING → MIGRATED. Recipient
     * flipped MIGRATING → STABLE inside its RECV-FLIP handler — by the
     * time we reach here the recipient is fully serving. Donor stays in
     * MIGRATED long enough for the slowest client to refresh its cache
     * via the next reply (a follow-up GC timer transitions MIGRATED →
     * STABLE; for the YCSB rig the next teardown reclusters the donor). */
    for (int i = 0; i < mig->n_slots; i++) {
        slotMigStateSet(mig->chosen[i], SLOT_STATE_MIGRATED, NULL);
    }

    pthread_mutex_lock(&mig->mu);
    mig->state = RDMA_MIG_DONE;
    mig->t_ended = time(NULL);
    pthread_mutex_unlock(&mig->mu);
    serverLog(LL_NOTICE,
        "RDMA MIGRATE worker: id=%lld DONE n_slots=%d", mig->id, mig->n_slots);
    return NULL;
}

/* ---- public commands ------------------------------------------------- */

/* RDMA MIGRATE recipient-host recipient-port n-slots */
void rdmaMigrateCommand(client *c) {
    if (c->argc != 5) {
        addReplyError(c, "syntax: RDMA MIGRATE recipient-host recipient-port n-slots");
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

    if (server.cluster == NULL || server.cluster->myself == NULL) {
        addReplyError(c, "cluster mode not enabled on this node");
        return;
    }

    /* Pick n_slots self-owned slots, lowest first. Multi-slot scan; take
     * topology rdlock so we don't race with a concurrent FLIP wrlock from
     * another migration on the same source. Per-slot rdlocks would be
     * 16384 acquires for a one-shot read — overkill. */
    int *chosen = zmalloc((size_t) n_slots * sizeof(int));
    int picked = 0;
    clusterTopoLockRead();
    for (int i = 0; i < CLUSTER_SLOTS && picked < n_slots; i++) {
        if (server.cluster->slots[i] == server.cluster->myself) {
            chosen[picked++] = i;
        }
    }
    clusterTopoUnlock();
    if (picked < n_slots) {
        zfree(chosen);
        addReplyErrorFormat(c, "self owns only %d slots, asked for %d",
                            picked, n_slots);
        return;
    }

    /* Resolve or create the outbound link (event-loop owned dict mutation,
     * so do it here, not in the worker). */
    sds key = sdscatfmt(sdsempty(), "%s:%i", host, port);
    rdmaOutboundLink *L = dictFetchValue(server.rdma_outbound_links, key);
    if (L == NULL) {
        L = rdmaOutboundLinkOpen(host, port);
        if (L == NULL) {
            sdsfree(key);
            zfree(chosen);
            addReplyError(c, "could not establish outbound RDMA link (see server log)");
            return;
        }
        dictAdd(server.rdma_outbound_links, key, L);
    } else {
        sdsfree(key);
    }

    /* Allocate the migration record. */
    rdmaMigration *mig = zcalloc(sizeof(*mig));
    mig->id        = server.rdma_migration_next_id++;
    mig->addr      = sdscatfmt(sdsempty(), "%s:%i", host, port);
    mig->host      = sdsnew(host);
    mig->port      = port;
    mig->n_slots   = n_slots;
    mig->chosen    = chosen;        /* worker reads but does not free */
    mig->L         = L;
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
        /* dict owns mig; remove + free via dictDelete which triggers our destructor */
        dictDelete(server.rdma_migrations, dict_key);
        /* dict_key was consumed by dictDelete -> sdsDestructor */
        addReplyError(c, "RDMA MIGRATE: pthread_create failed");
        return;
    }
    pthread_detach(mig->thr);

    addReplyStatusFormat(c, "OK migration_id=%lld running (n_slots=%d, %s:%d)",
                         mig->id, n_slots, host, port);
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
    pthread_mutex_destroy(&mig->mu);
    zfree(mig);
}
