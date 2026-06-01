/*
 * internal.h
 *
 * Private declarations shared across librdma_migration's implementation
 * files. Not intended for inclusion by callers; the public API surface
 * lives in include/rdma_migration.h.
 */

#ifndef RDMAMIG_INTERNAL_H_
#define RDMAMIG_INTERNAL_H_

#include "include/rdma_migration.h"
#include <netdb.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

/* ------------------------------------------------------------------------- *
 * Logging
 * ------------------------------------------------------------------------- *
 *
 * Implementation files emit diagnostics through RMIG_LOG; the function
 * pointer it dispatches to is set by rdmamig_set_logger() (see
 * rdma_buffer.c for the default implementation, which writes to stderr). */

extern rdmamig_log_fn rmig_logger;

#define RMIG_LOG(level, ...)  do { rmig_logger((level), __VA_ARGS__); } while (0)

/* ------------------------------------------------------------------------- *
 * Per-QP and CQ capacity
 * ------------------------------------------------------------------------- *
 *
 * These mirror the source 6.2.4 values. The Mellanox mlx5 family supports
 * up to max_qp_wr = 32768 (verifiable via `ibv_devinfo -d <dev> -v`), so the
 * 4096 used here is comfortably under-budget. */

#define MAX_SEND_WR     4096
#define MAX_SEND_SGE    1
#define MAX_RECV_WR     4096
#define MAX_RECV_SGE    1
#define CQ_CAPACITY     4096
#define MAX_SGE         1

/* Backlog passed to rdma_listen on the recipient. 0 means "implementation
 * default" (at least one) -- the migration channel only ever gets one
 * concurrent donor per recipient. */
#define MAX_CONNECTIONS 0

/* ------------------------------------------------------------------------- *
 * Concrete struct definitions
 * ------------------------------------------------------------------------- *
 *
 * Public callers see only the typedef'd opaque handles in
 * include/rdma_migration.h. */

struct rdmamig_buffer {
    struct rdma_cm_id *id;             /* QP this buffer is registered with */
    char              *buffer;         /* user-supplied payload area */
    size_t             size;
    struct ibv_mr     *mr;             /* registered memory region */
    int                buffer_access;  /* IBV_ACCESS_* bitmask */
    /* AqRaft Stage 5 (donor big-MR): when 1, this buffer is a lightweight VIEW
     * over another buffer's MR (shares ->mr / ->id, owns only its sub-range
     * ->buffer/->size). It did NOT call ibv_reg_mr, so release_pages must NOT
     * ibv_dereg_mr the shared MR. */
    int                is_view;
};

struct rdmamig_client {
    struct rdma_cm_id       *id;       /* set by rdmamig_client_connect() */
    struct rdma_addrinfo     hints;
    struct ibv_qp_init_attr  attr;
    struct rdma_addrinfo    *res;

    char ip[60];
    char ip_address[60];
    char port[60];
};

struct rdmamig_server {
    char                       serverPort[60];
    struct rdma_addrinfo       hints;
    struct rdma_addrinfo      *res;
    struct ibv_qp_init_attr    attr;
    struct rdma_event_channel *cm_channel;
    struct rdma_cm_id         *listen_id;       /* the bound passive id */
    struct rdma_cm_id         *id;              /* the accepted donor id */
    struct rdma_conn_param     conn_param;
    pthread_t                  listen_thread;
};

#endif /* RDMAMIG_INTERNAL_H_ */
