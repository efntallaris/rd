/*
 * rdma_server.c
 *
 * Recipient side of the RDMA migration channel: bind a passive endpoint on
 * a port, accept the first donor connection that comes in, and expose the
 * resulting cm_id so the recipient command handlers can register slot
 * landing buffers against it.
 *
 *   rdmamig_server_create(port)   -- bind, start the accept thread.
 *   rdmamig_server_cm_id(s)       -- the accepted-connection cm_id, NULL
 *                                    until a donor calls INIT-CLIENT.
 *
 * Acceptance happens off the main thread (listenThread) because rdma_get_request
 * is blocking. Once a peer connects, the main thread can dereference s->id
 * (= the accepted cm_id) and start registering buffers against it.
 */

/* gai_strerror() requires either _GNU_SOURCE or _POSIX_C_SOURCE >= 200112L
 * with -std=c11; we use the GNU feature macro for simplicity. */
#define _GNU_SOURCE
#include "internal.h"
#include "zmalloc.h"
#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------------- *
 * Endpoint setup helpers
 * ------------------------------------------------------------------------- */

static void init_addrinfo_hints(struct rdma_addrinfo *hints) {
    memset(hints, 0, sizeof(*hints));
    hints->ai_flags      = RAI_PASSIVE;     /* we want a bind-style address */
    hints->ai_port_space = RDMA_PS_TCP;
    hints->ai_qp_type    = IBV_QPT_RC;
}

static void init_qp_init_attr(struct ibv_qp_init_attr *attr) {
    memset(attr, 0, sizeof(*attr));
    attr->cap.max_send_wr  = MAX_SEND_WR;
    attr->cap.max_recv_wr  = MAX_RECV_WR;
    attr->cap.max_send_sge = MAX_SEND_SGE;
    attr->cap.max_recv_sge = MAX_RECV_SGE;
    attr->sq_sig_all       = 0;
    attr->qp_type          = IBV_QPT_RC;
}

/* Resolve a passive address for `s->serverPort` and bring up a listening
 * endpoint. On success s->listen_id is non-NULL; on failure it stays NULL
 * and the caller bails out before starting the accept thread. */
static void create_listening_endpoint(rdmamig_server *s) {
    int err;

    s->res       = NULL;
    s->listen_id = NULL;

    init_addrinfo_hints(&s->hints);
    init_qp_init_attr(&s->attr);

    err = rdma_getaddrinfo(NULL, s->serverPort, &s->hints, &s->res);
    if (err) {
        RMIG_LOG(RDMAMIG_LOG_WARNING,
                 "rdma_getaddrinfo(passive,%s): %d (%s)",
                 s->serverPort, err, gai_strerror(err));
        s->res = NULL;
        return;
    }

    err = rdma_create_ep(&s->listen_id, s->res, NULL, &s->attr);
    if (err) {
        RMIG_LOG(RDMAMIG_LOG_WARNING, "rdma_create_ep: %s", strerror(errno));
        rdma_freeaddrinfo(s->res);
        s->res = NULL;
        return;
    }

    rdma_freeaddrinfo(s->res);
    s->res = NULL;
}

/* ------------------------------------------------------------------------- *
 * Accept loop (runs in the listen thread)
 * ------------------------------------------------------------------------- */

/* Block until a donor connects; `s->id` is the resulting cm_id. */
static void wait_for_first_request(rdmamig_server *s) {
    if (rdma_get_request(s->listen_id, &s->id) != 0) {
        RMIG_LOG(RDMAMIG_LOG_WARNING, "rdma_get_request: %s", strerror(errno));
    }
}

static void accept_pending_connection(rdmamig_server *s) {
    if (rdma_accept(s->id, &s->conn_param) != 0) {
        RMIG_LOG(RDMAMIG_LOG_WARNING, "rdma_accept: %s", strerror(errno));
    }
}

static void *listen_thread_main(void *data) {
    rdmamig_server *s = data;

    RMIG_LOG(RDMAMIG_LOG_NOTICE, "rdma_server: listen_thread entered, calling rdma_listen on port %s", s->serverPort);
    if (rdma_listen(s->listen_id, MAX_CONNECTIONS) != 0) {
        RMIG_LOG(RDMAMIG_LOG_WARNING, "rdma_listen: %s", strerror(errno));
        return NULL;
    }
    RMIG_LOG(RDMAMIG_LOG_NOTICE, "rdma_server: listening on port %s, waiting for first donor", s->serverPort);
    wait_for_first_request(s);
    RMIG_LOG(RDMAMIG_LOG_NOTICE, "rdma_server: donor connection request received, calling rdma_accept");
    accept_pending_connection(s);
    RMIG_LOG(RDMAMIG_LOG_NOTICE, "rdma_server: donor connection accepted, cm_id=%p", (void*)s->id);
    return NULL;
}

/* Returns 0 on success, -1 if pthread_create failed (caller should bail). */
static int spawn_listen_thread(rdmamig_server *s) {
    int rc = pthread_create(&s->listen_thread, NULL, listen_thread_main, s);
    if (rc != 0) {
        RMIG_LOG(RDMAMIG_LOG_WARNING, "pthread_create failed: %d", rc);
        return -1;
    }
    return 0;
}

/* ------------------------------------------------------------------------- *
 * Public API
 * ------------------------------------------------------------------------- */

rdmamig_server *rdmamig_server_create(const char *port) {
    rdmamig_server *s = zmalloc(sizeof(*s));
    if (s == NULL) return NULL;
    memset(s, 0, sizeof(*s));
    strncpy(s->serverPort, port, sizeof(s->serverPort) - 1);

    create_listening_endpoint(s);
    if (s->listen_id == NULL) {
        /* Most common cause: rdma_getaddrinfo can't find a passive RDMA
         * address (no IB device, no IP on the IPoIB interface, etc.).
         * Don't return a half-initialized handle. */
        zfree(s);
        return NULL;
    }

    if (spawn_listen_thread(s) != 0) {
        rdma_destroy_ep(s->listen_id);
        zfree(s);
        return NULL;
    }
    return s;
}

struct rdma_cm_id *rdmamig_server_cm_id(rdmamig_server *s) {
    return s->id;
}
