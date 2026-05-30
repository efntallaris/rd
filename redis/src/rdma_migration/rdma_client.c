/*
 * rdma_client.c
 *
 * Donor side of the RDMA migration channel: build a queue-pair targeting a
 * recipient's listening endpoint, and post one-sided RDMA writes through it.
 *
 *   rdmamig_client_create(ip, port)   <-- allocate a handle (no I/O yet)
 *   rdmamig_client_connect(c)         <-- resolve + bind + connect
 *   rdmamig_client_post_write(...)    <-- enqueue a one-sided RDMA WRITE
 *   rdmamig_client_wait_send(c)       <-- block on the next send completion
 *
 * All functions return 0 on success / negative on failure (POSIX style).
 * Diagnostic messages are funneled through RMIG_LOG.
 */

#include "internal.h"
#include "zmalloc.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------------- *
 * Connection setup
 * ------------------------------------------------------------------------- */

/* Tell rdma_getaddrinfo what kind of address we want. ACTIVE side, RC QP,
 * over the TCP-style RDMA port-space. */
static void init_addrinfo_hints(struct rdma_addrinfo *hints) {
    memset(hints, 0, sizeof(*hints));
    hints->ai_port_space = RDMA_PS_TCP;
    hints->ai_qp_type    = IBV_QPT_RC;
}

/* Per-QP capabilities the donor needs: deep enough send queue to hold many
 * in-flight RDMA writes without blocking. */
static void init_qp_init_attr(struct ibv_qp_init_attr *attr) {
    memset(attr, 0, sizeof(*attr));
    attr->cap.max_send_wr  = MAX_SEND_WR;
    attr->cap.max_recv_wr  = MAX_RECV_WR;
    attr->cap.max_send_sge = MAX_SEND_SGE;
    attr->cap.max_recv_sge = MAX_RECV_SGE;
    attr->sq_sig_all       = 0;
    attr->qp_type          = IBV_QPT_RC;
}

/* Resolve <ip,port> via rdma_getaddrinfo and bring up an endpoint that owns
 * a QP with the requested capabilities. On success c->id is the new cm_id;
 * on failure all addresses are released and c->id is left NULL. */
static int resolve_and_create_endpoint(rdmamig_client *c) {
    int err;

    init_addrinfo_hints(&c->hints);
    init_qp_init_attr(&c->attr);

    err = rdma_getaddrinfo(c->ip_address, c->port, &c->hints, &c->res);
    if (err) {
        RMIG_LOG(RDMAMIG_LOG_WARNING,
                 "rdma_getaddrinfo(%s,%s) failed: %d", c->ip_address, c->port, err);
        return -1;
    }

    err = rdma_create_ep(&c->id, c->res, NULL, &c->attr);
    if (err) {
        RMIG_LOG(RDMAMIG_LOG_WARNING,
                 "rdma_create_ep failed: %s", strerror(errno));
        rdma_freeaddrinfo(c->res);
        c->res = NULL;
        return -1;
    }

    rdma_freeaddrinfo(c->res);
    c->res = NULL;
    return 0;
}

/* ------------------------------------------------------------------------- *
 * Public API
 * ------------------------------------------------------------------------- */

rdmamig_client *rdmamig_client_create(const char *ip, const char *port) {
    rdmamig_client *c = zmalloc(sizeof(*c));
    if (c == NULL) return NULL;
    memset(c, 0, sizeof(*c));
    strncpy(c->ip_address, ip,   sizeof(c->ip_address) - 1);
    strncpy(c->port,       port, sizeof(c->port) - 1);
    return c;
}

int rdmamig_client_connect(rdmamig_client *c) {
    if (resolve_and_create_endpoint(c) != 0) return -1;

    if (rdma_connect(c->id, NULL) != 0) {
        RMIG_LOG(RDMAMIG_LOG_WARNING,
                 "rdma_connect to %s:%s failed: %s",
                 c->ip_address, c->port, strerror(errno));
        return -1;
    }
    return 0;
}

struct rdma_cm_id *rdmamig_client_cm_id(rdmamig_client *c) {
    return c->id;
}

int rdmamig_client_post_write(rdmamig_buffer *b, char *local_addr,
                              uint64_t remote_addr, uint32_t remote_key,
                              size_t len)
{
    /* Note: we look up the QP from the buffer's cm_id rather than threading
     * a client pointer through the call -- the buffer must live on the same
     * cm_id as the client (otherwise the lkey is invalid anyway). */
    struct ibv_sge sge = {
        .addr   = (uint64_t)(uintptr_t) local_addr,
        .length = (uint32_t) len,
        .lkey   = b->mr ? b->mr->lkey : 0,
    };
    struct ibv_send_wr wr = {
        .wr_id      = 0,
        .sg_list    = &sge,
        .num_sge    = 1,
        .next       = NULL,
        .opcode     = IBV_WR_RDMA_WRITE,
        .send_flags = IBV_SEND_SIGNALED,
        .wr.rdma    = { .remote_addr = remote_addr, .rkey = remote_key },
    };
    struct ibv_send_wr *bad_wr;
    return ibv_post_send(b->id->qp, &wr, &bad_wr);
}

int rdmamig_client_wait_send(rdmamig_client *c) {
    struct ibv_wc wc;
    int n;

    /* Spin until a completion arrives. Returns >0 (= number of completions
     * polled, normally 1) on success, <0 on QP error. */
    while ((n = rdma_get_send_comp(c->id, &wc)) == 0) {
        /* spin */
    }
    if (n < 0) {
        RMIG_LOG(RDMAMIG_LOG_WARNING, "rdma_get_send_comp failed: %s", strerror(errno));
    }
    return n;
}

int rdmamig_client_poll_send(rdmamig_client *c, struct ibv_wc *wc, int max) {
    /* Non-blocking: poll up to `max` send completions off the QP's send CQ.
     * Returns the number polled (0 if none ready yet), or -1 if a completion
     * reported a failure status. Used to reap pipelined post_write WRs in
     * batches instead of blocking per-write. post_write signals every WR, so
     * the number of completions equals the number of writes posted. */
    if (c == NULL || c->id == NULL || c->id->send_cq == NULL) return -1;
    int n = ibv_poll_cq(c->id->send_cq, max, wc);
    if (n < 0) {
        RMIG_LOG(RDMAMIG_LOG_WARNING, "ibv_poll_cq(send) failed");
        return -1;
    }
    for (int i = 0; i < n; i++) {
        if (wc[i].status != IBV_WC_SUCCESS) {
            RMIG_LOG(RDMAMIG_LOG_WARNING,
                "send completion error: status=%d (%s) wr_id=%llu",
                wc[i].status, ibv_wc_status_str(wc[i].status),
                (unsigned long long) wc[i].wr_id);
            return -1;
        }
    }
    return n;
}
