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

/* RDMA REGISTER-BLOCK-SLOTS SLOTS <slot> <nblocks> [<slot> <nblocks> ...]
 *
 * Recipient side: for each (slot, nblocks) pair, allocate that many
 * RDMA-registered landing buffers via the slot-keyed allocator and reply
 * with the resulting (VA, rkey) array as a single binary bulk string.
 * The donor consumes that array verbatim during TRANSFER-SLOTS to
 * populate its post_write_at_address calls. */
void rdmaRegisterBlockSlotsCommand(client *c) {
    rdmaCachedConnection *cs = rdmaGetConnection(c);
    if (cs == NULL) {
        addReplyError(c, "no RDMA connection cached for this client; call RDMA INIT-SERVER first");
        return;
    }

    /* argv[0]=RDMA argv[1]=REGISTER-BLOCK-SLOTS, scan from argv[2]. */
    int start_idx = 0;
    for (int j = 2; j < c->argc; j++) {
        if (!strcasecmp(c->argv[j]->ptr, "SLOTS")) {
            start_idx = j + 1;
            break;
        }
    }
    if (start_idx == 0 || start_idx >= c->argc) {
        addReplyError(c, "missing SLOTS keyword or no slot/nblocks pairs");
        return;
    }
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

    serverLog(LL_NOTICE, "RDMA REGISTER-BLOCK-SLOTS: client_id=%llu starting (%d total blocks across %d pairs)",
              (unsigned long long) c->id, total_requested_blocks, (c->argc - start_idx) / 2);
    rdmaRemoteBufferInfo *remote_buffers = zmalloc(total_requested_blocks * sizeof(rdmaRemoteBufferInfo));
    int total_remote_buffers = 0;

    for (int idx = start_idx; idx + 1 < c->argc; idx += 2) {
        long long slot_id, n_blocks;
        if (getLongLongFromObject(c->argv[idx], &slot_id) != C_OK ||
            slot_id < 0 || slot_id >= CLUSTER_SLOTS) {
            zfree(remote_buffers);
            addReplyError(c, "slot id out of range");
            return;
        }
        if (getLongLongFromObject(c->argv[idx + 1], &n_blocks) != C_OK || n_blocks < 0) {
            zfree(remote_buffers);
            addReplyError(c, "n-blocks must be a non-negative integer");
            return;
        }

        for (int i = 0; i < n_blocks; i++) {
            void *block_ptr = r_allocator_alloc_new_empty_block((int) slot_id);
            if (block_ptr == NULL) {
                zfree(remote_buffers);
                addReplyError(c, "r_allocator_alloc_new_empty_block returned NULL");
                return;
            }
            struct rdmamig_buffer *rb = rdmamig_buffer_create(
                rdmamig_server_cm_id(cs->s), block_ptr, RDMAMIG_BLOCK_SIZE_BYTES, 0);
            if (rb == NULL) {
                zfree(remote_buffers);
                addReplyError(c, "rdmamig_buffer_create failed");
                return;
            }
            remote_buffers[total_remote_buffers].ptr = (uint64_t) block_ptr;
            remote_buffers[total_remote_buffers].rkey = rdmamig_buffer_rkey(rb);
            serverLog(LL_NOTICE, "RDMA REGISTER-BLOCK-SLOTS: slot=%lld block=%d VA=0x%llx rkey=0x%x",
                      slot_id, i,
                      (unsigned long long) remote_buffers[total_remote_buffers].ptr,
                      remote_buffers[total_remote_buffers].rkey);
            total_remote_buffers++;
        }
    }

    serverLog(LL_NOTICE, "RDMA REGISTER-BLOCK-SLOTS: client_id=%llu returning %d (VA, rkey) tuples",
              (unsigned long long) c->id, total_remote_buffers);
    addReplyBulkCBuffer(c, (char *) remote_buffers, total_remote_buffers * sizeof(rdmaRemoteBufferInfo));
    zfree(remote_buffers);
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

/* RDMA DONE-SLOTS <slot> [<slot> ...]
 *
 * Recipient side. For each slot in argv, walk the per-slot RDMA-registered
 * blocks (populated by an earlier REGISTER-BLOCK-SLOTS + RDMA writes from
 * the donor), parse the flat encoded entries, and dbAdd them into the local
 * keyspace. Synchronous; no thread.
 *
 * Note: ownership is NOT flipped here -- the recipient gains the keys but
 * the cluster slot map is unchanged, so normal client reads will still
 * MOVED-redirect. The ownership flip is out of scope for this slice. */
void rdmaDoneSlotsCommand(client *c) {
    /* argv[0]=RDMA argv[1]=DONE-SLOTS, real slot args start at argv[2]. */
    long long total_added = 0;
    for (int j = 2; j < c->argc; j++) {
        long long slot;
        if (getLongLongFromObject(c->argv[j], &slot) != C_OK ||
            slot < 0 || slot >= CLUSTER_SLOTS) {
            addReplyError(c, "slot id out of range");
            return;
        }
        total_added += rdmaApplySlot(c->db, (int) slot);
    }
    serverLog(LL_NOTICE, "RDMA DONE-SLOTS: applied %lld keys across %d slots",
              total_added, c->argc - 2);
    addReply(c, shared.ok);
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

    /* Pick the same N slots as Phase 1: walk own owned, lowest first. */
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

    sds key = sdscatfmt(sdsempty(), "%s:%i", host, port);
    rdmaOutboundLink *L = dictFetchValue(server.rdma_outbound_links, key);
    sdsfree(key);
    if (L == NULL) {
        zfree(chosen);
        addReplyError(c, "no outbound link cached; call RDMA MIGRATE-PREP + RDMA RESHARD first");
        return;
    }

    /* Verify ALL chosen slots are pre-registered (source) AND have a
     * recipient landing target. Atomicity: bail out before doing any RDMA
     * work if any slot is missing either side. */
    for (int i = 0; i < n_slots; i++) {
        int slot = chosen[i];
        if (L->source_buffers[slot] == NULL) {
            zfree(chosen);
            addReplyErrorFormat(c,
                "slot %d not pre-registered; call RDMA RESHARD first", slot);
            return;
        }
        if (L->buffers[slot].ptr == 0) {
            zfree(chosen);
            addReplyErrorFormat(c,
                "slot %d has no recipient landing buffer; call RDMA MIGRATE-PREP first", slot);
            return;
        }
    }

    pthread_mutex_lock(&L->mu);

    size_t total_bytes = 0;
    int errs = 0;
    for (int i = 0; i < n_slots; i++) {
        int slot = chosen[i];
        char *staging = rdmamig_buffer_data(L->source_buffers[slot]);
        uint32_t n_entries = rdmaEncodeSlotEntries(c->db, slot, staging,
                                                   RDMAMIG_BLOCK_SIZE_BYTES);
        rdmaDebugDumpSlotBytes("SRC", slot, staging);
        if (server.rdma_reshard_debug_bytes) {
            /* Per-slot r_allocator stats + the RDMA-transferred byte count
             * (always the full block; payload-relevant bytes are
             * approximately sizeof(u32) + n_entries * (8 + key+val sizes),
             * the rest is zero padding). */
            r_allocator_log_slot_stats(slot);
            serverLog(LL_NOTICE,
                "RDMA RESHARD-EXEC TX: slot=%d n_entries=%u rdma_bytes=%d",
                slot, n_entries, RDMAMIG_BLOCK_SIZE_BYTES);
        }
        int rc = rdmamig_client_post_write(L->source_buffers[slot], staging,
                                           L->buffers[slot].ptr,
                                           L->buffers[slot].rkey,
                                           RDMAMIG_BLOCK_SIZE_BYTES);
        if (rc != 0) {
            errs++;
            serverLog(LL_WARNING,
                "RDMA RESHARD-EXEC: slot=%d post_write failed rc=%d", slot, rc);
            continue;
        }
        /* wait_send returns # of completions polled (positive on success, <0 on QP error). */
        int wc_rc = rdmamig_client_wait_send(L->client);
        if (wc_rc < 0) {
            errs++;
            serverLog(LL_WARNING,
                "RDMA RESHARD-EXEC: slot=%d wait_send failed rc=%d", slot, wc_rc);
            continue;
        }
        total_bytes += RDMAMIG_BLOCK_SIZE_BYTES;
        serverLog(LL_NOTICE,
            "RDMA RESHARD-EXEC: slot=%d entries=%u bytes=%d rc=0",
            slot, n_entries, RDMAMIG_BLOCK_SIZE_BYTES);
    }

    /* Notify recipient: "RDMA DONE-SLOTS <s1> <s2> ... <sN>" via cached
     * hiredis ctrl channel. The recipient's rdmaDoneSlotsCommand walks
     * its own r_allocator blocks for each slot (now containing the
     * source's RDMA-written entries) and dbAdd's the keys. */
    int argc = 2 + n_slots;
    const char **argv = zmalloc((size_t) argc * sizeof(*argv));
    size_t *argvlen = zmalloc((size_t) argc * sizeof(*argvlen));
    argv[0] = "RDMA";       argvlen[0] = 4;
    argv[1] = "DONE-SLOTS"; argvlen[1] = 10;
    char (*numbuf)[16] = zmalloc((size_t) n_slots * sizeof(*numbuf));
    for (int i = 0; i < n_slots; i++) {
        argvlen[2 + i] = (size_t) snprintf(numbuf[i], 16, "%d", chosen[i]);
        argv  [2 + i] = numbuf[i];
    }
    redisReply *r = redisCommandArgv(L->ctrl, argc, argv, argvlen);
    zfree(argv);
    zfree(argvlen);
    zfree(numbuf);

    int done_ok = 1;
    if (r == NULL || r->type != REDIS_REPLY_STATUS) {
        done_ok = 0;
        serverLog(LL_WARNING,
            "RDMA RESHARD-EXEC: DONE-SLOTS reply not OK (data may still have landed)");
    }
    if (r) freeReplyObject(r);

    pthread_mutex_unlock(&L->mu);

    int first = chosen[0], last = chosen[n_slots - 1];
    zfree(chosen);
    addReplyStatusFormat(c,
        "OK %d slots transferred (slots %d..%d, bytes %zu, errors %d, done=%s)",
        n_slots, first, last, total_bytes, errs, done_ok ? "ok" : "fail");
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
     *    snapshot. */
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
    for (int j = 3; j < c->argc; j++) {
        long long slot_ll;
        if (getLongLongFromObject(c->argv[j], &slot_ll) != C_OK ||
            slot_ll < 0 || slot_ll >= CLUSTER_SLOTS) {
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

    addReplyStatusFormat(c, "OK %d slots flipped on recipient", flipped);
}
