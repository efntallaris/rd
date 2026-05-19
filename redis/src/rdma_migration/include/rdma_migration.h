/*
 * rdma_migration.h
 *
 * Public API of librdma_migration.a -- the slot-keyed RDMA migration
 * infrastructure used by Redis's cluster_rdma.c handlers.
 *
 * The library exposes three opaque handles plus a slot-keyed allocator:
 *
 *   - rdmamig_buffer  : a memory region registered with the local NIC.
 *   - rdmamig_client  : the donor's outbound RDMA queue-pair.
 *   - rdmamig_server  : the recipient's listening endpoint + accept thread.
 *   - r_allocator_*   : per-slot block pool whose blocks the caller can
 *                       wrap in rdmamig_buffer instances. (Declared in
 *                       <allocator.h>, re-exported below.)
 *
 * Callers install a logger once at startup via rdmamig_set_logger();
 * by default the library writes a line to stderr per diagnostic message.
 */

#ifndef RDMA_MIGRATION_H_
#define RDMA_MIGRATION_H_

#include <stddef.h>
#include <stdint.h>
#include <rdma/rdma_cma.h>     /* defines struct rdma_cm_id used in signatures */

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------------- *
 * Logging
 * ------------------------------------------------------------------------- */

/* Numeric levels match Redis's LL_* constants for trivial passthrough. */
#define RDMAMIG_LOG_DEBUG    0
#define RDMAMIG_LOG_VERBOSE  1
#define RDMAMIG_LOG_NOTICE   2
#define RDMAMIG_LOG_WARNING  3

typedef void (*rdmamig_log_fn)(int level, const char *fmt, ...);

/* Install a logger to receive the library's diagnostic messages. Pass NULL
 * to restore the default (one line per message to stderr). The host
 * application typically calls this once at startup. Thread-safe vs. itself
 * but not vs. concurrent log emission. */
void rdmamig_set_logger(rdmamig_log_fn fn);

/* ------------------------------------------------------------------------- *
 * Configuration
 * ------------------------------------------------------------------------- */

/* Default block size used by the per-slot allocator. Buffers exchanged
 * across the channel are sized in multiples of this. */
#define RDMAMIG_BLOCK_SIZE_BYTES  (2u << 20)   /* 2 MiB */

/* ------------------------------------------------------------------------- *
 * Buffers (memory regions registered with the local NIC)
 * ------------------------------------------------------------------------- */

typedef struct rdmamig_buffer rdmamig_buffer;

/* Wrap an existing memory region in an RDMA buffer. `id` is the cm_id whose
 * QP/PD will own the registration -- the buffer can only be used over that
 * connection. `access` is either:
 *   0  -> default (LOCAL_WRITE | REMOTE_READ | REMOTE_WRITE)
 *   10 -> no permissions (registered for rkey advertisement only)
 *   any other value -> raw IBV_ACCESS_* bitmask passed through.
 * Returns NULL on registration failure (details routed through the logger). */
rdmamig_buffer *rdmamig_buffer_create(struct rdma_cm_id *id, char *buffer,
                                      size_t size, int access);

/* Remote key the peer needs to address this buffer in RDMA writes. */
uint32_t rdmamig_buffer_rkey(rdmamig_buffer *b);

/* Pointer to the underlying user-supplied payload area passed to
 * rdmamig_buffer_create. Useful for callers that need to fill or read the
 * buffer in place (e.g. Phase 2 RDMA RESHARD-EXEC encodes slot entries into
 * the same registered buffer it later RDMA-WRITEs from). */
char *rdmamig_buffer_data(const rdmamig_buffer *b);

/* ------------------------------------------------------------------------- *
 * Client (donor outbound QP)
 * ------------------------------------------------------------------------- */

typedef struct rdmamig_client rdmamig_client;

/* Allocate a donor handle pointing at <ip,port>. No I/O is performed yet --
 * call rdmamig_client_connect to actually establish the queue pair. */
rdmamig_client *rdmamig_client_create(const char *ip, const char *port);

/* Resolve, build the endpoint, and call rdma_connect. Returns 0 on success,
 * -1 on any failure (with details routed through the logger). */
int rdmamig_client_connect(rdmamig_client *c);

/* Accessor for the underlying rdma_cm_id (needed when registering buffers
 * via rdmamig_buffer_create against this client's PD). */
struct rdma_cm_id *rdmamig_client_cm_id(rdmamig_client *c);

/* RDMA-write `len` bytes from `local_addr` (must lie inside the registered
 * region of `b`) into <remote_addr,remote_key> on the connected peer. Does
 * not block on completion; pair with rdmamig_client_wait_send. Returns 0 on
 * successful submission. */
int rdmamig_client_post_write(rdmamig_buffer *b, char *local_addr,
                              uint64_t remote_addr, uint32_t remote_key,
                              size_t len);

/* Block until the next send completion arrives. Returns the number of
 * completions polled (normally 1) or a negative value on QP error. */
int rdmamig_client_wait_send(rdmamig_client *c);

/* ------------------------------------------------------------------------- *
 * Server (recipient passive endpoint)
 * ------------------------------------------------------------------------- */

typedef struct rdmamig_server rdmamig_server;

/* Bind a listening endpoint on <port> and start the accept thread.
 * Returns NULL if rdma_getaddrinfo / rdma_create_ep / pthread_create fail. */
rdmamig_server *rdmamig_server_create(const char *port);

/* Accessor for the accepted-connection cm_id. NULL until a peer connects
 * (the accept thread sets it from rdma_get_request). */
struct rdma_cm_id *rdmamig_server_cm_id(rdmamig_server *s);

/* ------------------------------------------------------------------------- *
 * Slot-keyed RDMA-pinned allocator
 * ------------------------------------------------------------------------- *
 *
 * The allocator is an in-tree port of Antonis Papaioannou's 2021 design
 * (originally redis/src/allocator.{c,h} on blocking_version_2_threads).
 * It maintains per-slot lists of fixed-size blocks (BLOCK_SIZE_BYTES
 * = 1 MiB), each of which the caller can register as an RDMA buffer.
 *
 * The full API (segment iterator, debug helpers, dump/load, etc.) is
 * declared in <allocator.h>. The data-transfer slice uses only:
 *   r_allocator_init()                       -- one-shot setup at startup
 *   r_allocator_alloc_new_empty_block(slot)  -- get a fresh 1 MiB block
 *   r_allocator_get_block_buffers_for_slot(slot, &n)
 *                                            -- list a slot's blocks
 *   r_allocator_lock_slot_blocks(slot)       -- freeze a slot's blocks
 */
#include "../allocator.h"

#ifdef __cplusplus
}
#endif

#endif /* RDMA_MIGRATION_H_ */
