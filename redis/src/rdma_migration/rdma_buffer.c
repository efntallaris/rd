/*
 * rdma_buffer.c
 *
 * Wraps an arbitrary chunk of memory in an RDMA memory region (MR), so the
 * remote peer can later RDMA-read/write into it via its (virtual address,
 * remote key) pair. Also hosts the library's logger plumbing because every
 * other source file pulls this header in for the buffer type.
 */

#include "internal.h"
#include "zmalloc.h"
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>

/* ------------------------------------------------------------------------- *
 * Logger plumbing
 * ------------------------------------------------------------------------- */

/* Default logger: write each message as one line to stderr. The `level`
 * argument is ignored at this layer -- callers that want filtering install
 * their own logger via rdmamig_set_logger(). */
static void rmig_default_logger(int level, const char *fmt, ...) {
    (void) level;
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    fputc('\n', stderr);
    va_end(ap);
}

rdmamig_log_fn rmig_logger = rmig_default_logger;

void rdmamig_set_logger(rdmamig_log_fn fn) {
    rmig_logger = (fn != NULL) ? fn : rmig_default_logger;
}

/* ------------------------------------------------------------------------- *
 * Buffer registration
 * ------------------------------------------------------------------------- */

/* Translate the legacy `access` argument from the 6.2.4 source into an
 * IBV_ACCESS_* bitmask. The two magic values come from the original API:
 *   0  -> default: local write + remote read + remote write (the only mode
 *         the data-transfer slice exercises today).
 *   10 -> "no permissions" (used when the buffer is registered purely so
 *         that a peer can read its rkey but never actually accesses it).
 *   any other value is treated as a raw IBV_ACCESS_* mask passed through
 *   unchanged.
 */
static int translate_access_flags(int access) {
    switch (access) {
        case 0:  return IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
        case 10: return 0;
        default: return access;
    }
}

uint32_t rdmamig_buffer_rkey(rdmamig_buffer *b) {
    return b->mr->rkey;
}

char *rdmamig_buffer_data(const rdmamig_buffer *b) {
    return b->buffer;
}

rdmamig_buffer *rdmamig_buffer_create(struct rdma_cm_id *id, char *buffer,
                                      size_t size, int access)
{
    rdmamig_buffer *b = zmalloc(sizeof(*b));
    if (b == NULL) {
        RMIG_LOG(RDMAMIG_LOG_WARNING, "rdmamig_buffer_create: zmalloc failed");
        return NULL;
    }

    b->id            = id;
    b->buffer        = buffer;
    b->size          = size;
    b->buffer_access = translate_access_flags(access);
    b->mr            = ibv_reg_mr(id->pd, buffer, size, b->buffer_access);

    if (b->mr == NULL) {
        RMIG_LOG(RDMAMIG_LOG_WARNING,
                 "rdmamig_buffer_create: ibv_reg_mr failed for %zu bytes (errno=%d)",
                 size, errno);
        zfree(b);
        return NULL;
    }
    return b;
}
