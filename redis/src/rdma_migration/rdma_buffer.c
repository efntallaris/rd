/*
 * rdma_buffer.c
 *
 * Wraps an arbitrary chunk of memory in an RDMA memory region (MR), so the
 * remote peer can later RDMA-read/write into it via its (virtual address,
 * remote key) pair. Also hosts the library's logger plumbing because every
 * other source file pulls this header in for the buffer type.
 */

/* _DEFAULT_SOURCE: expose madvise()/MADV_DONTNEED from <sys/mman.h> under
 * -std=c11 (used by the rdma_migration sublib). Must precede all includes. */
#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE 1
#endif
#include "internal.h"
#include "zmalloc.h"
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <sys/mman.h>

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
    b->is_view       = 0;
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

/* AqRaft Stage 5 (donor big-MR): create a lightweight VIEW over an existing
 * registered buffer. The view shares `parent`'s MR (same lkey/rkey, same cm_id)
 * but exposes only the sub-range [sub_ptr, sub_ptr+sub_size). `sub_ptr` MUST lie
 * inside parent's registered range — RDMA lkeys/rkeys are valid for any VA in
 * the registered region, so post_write/data over the view use the parent's MR
 * with the view's own local address. This lets the donor replace N per-slot
 * ibv_reg_mr (N ioctls, ~23ms each = ~16s for 683 slots) with ONE big-MR
 * registration + N cheap views. The view does NOT own the MR: release_pages
 * skips ibv_dereg_mr for views (the parent owns teardown). Returns NULL only on
 * alloc failure or if sub_ptr is outside the parent range. */
rdmamig_buffer *rdmamig_buffer_create_view(rdmamig_buffer *parent,
                                           char *sub_ptr, size_t sub_size)
{
    if (parent == NULL || parent->mr == NULL || sub_ptr == NULL) return NULL;
    /* Bounds-check: the sub-range must lie fully inside the parent buffer. */
    if (sub_ptr < parent->buffer ||
        sub_ptr + sub_size > parent->buffer + parent->size) {
        RMIG_LOG(RDMAMIG_LOG_WARNING,
                 "rdmamig_buffer_create_view: sub-range [%p,+%zu) outside parent "
                 "[%p,+%zu)", (void *) sub_ptr, sub_size,
                 (void *) parent->buffer, parent->size);
        return NULL;
    }
    rdmamig_buffer *b = zmalloc(sizeof(*b));
    if (b == NULL) {
        RMIG_LOG(RDMAMIG_LOG_WARNING, "rdmamig_buffer_create_view: zmalloc failed");
        return NULL;
    }
    b->id            = parent->id;
    b->buffer        = sub_ptr;
    b->size          = sub_size;
    b->mr            = parent->mr;   /* SHARED — not owned by this view */
    b->buffer_access = parent->buffer_access;
    b->is_view       = 1;
    return b;
}

/* AqRaft pool-free: reclaim the physical pages backing this buffer once it is
 * no longer used for RDMA or local access. ibv_reg_mr pins (mlocks) the pages,
 * so madvise(MADV_DONTNEED) alone would fail (EINVAL) — we ibv_dereg_mr first
 * to unpin, then madvise to drop the resident pages. We deliberately do NOT
 * munmap and do NOT free the struct: the VA stays mapped so any stale pointers
 * into the region read zero-filled pages instead of SIGSEGV-ing. Returns the
 * number of bytes whose pages were released (0 on no-op). Idempotent (clears
 * the MR). */
size_t rdmamig_buffer_release_pages(rdmamig_buffer *b) {
    if (b == NULL) return 0;
    /* AqRaft Stage 5: a view shares the parent's MR and did not register it —
     * never dereg here (the parent owns teardown) and never madvise its
     * sub-range out from under the still-live parent MR. */
    if (b->is_view) return 0;
    if (b->mr != NULL) {
        ibv_dereg_mr(b->mr);
        b->mr = NULL;
    }
    if (b->buffer != NULL && b->size > 0) {
        if (madvise(b->buffer, b->size, MADV_DONTNEED) == 0) {
            return b->size;
        }
        RMIG_LOG(RDMAMIG_LOG_WARNING,
                 "rdmamig_buffer_release_pages: madvise(%zu) failed (errno=%d)",
                 b->size, errno);
    }
    return 0;
}
