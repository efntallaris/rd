#ifndef __RDMAS_H
#define __RDMAS_H
#include <netdb.h>
#include <errno.h>
#include <getopt.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include "rdma_buffer.h"
#include "zmalloc.h"
#define MAX_CONNECTIONS 0
/* Capacity of the completion queue (CQ) */
#define CQ_CAPACITY (21800)
/* MAX SGE capacity */
#define MAX_SGE (1)
/* MAX work requests */
#define MAX_WR (21800)

#define MAX_SEND_WR (21800);
#define MAX_SEND_SGE (1);
#define MAX_RECV_WR (21800);
#define MAX_RECV_SGE (1);


#define SG_SIG_ALL (0);

struct rdma_server_info {
    char dummy[10];
    char serverPort[60];
    struct rdma_addrinfo        hints;
    struct rdma_addrinfo        *res;
    struct ibv_qp_init_attr     attr;
    struct rdma_event_channel   *cm_channel;
    struct rdma_cm_id           *listen_id;
    struct rdma_cm_id           *id;
    struct rdma_conn_param      conn_param;

    pthread_t  		    listen_thread;


    struct {
        void (*dummy_func)();
        void (*create_event_channel)(struct rdma_server_info *);
        void (*create_id)(struct rdma_server_info *, enum rdma_port_space);
        void (*init_qp_attr)(struct rdma_server_info *);
        void (*init_hints)(struct rdma_server_info *);
        void (*create_ep)(struct rdma_server_info *);
        void (*get_request)(struct rdma_server_info *);
        void (*accept_connection)(struct rdma_server_info *);
        void (*start_listen)(struct rdma_server_info *);
        void (*start_listen_thread)(struct rdma_server_info *);
        void (*init_conn_param)(struct rdma_server_info *);
	void (*teardown_client_resources)(struct rdma_server_info *);
    } server_ops;

    struct {
        void (*rdma_send)(struct rdma_server_info *, struct rdma_buffer_info *,  size_t );
        void (*rdma_recv)(struct rdma_server_info *, struct rdma_buffer_info *,  size_t );
	void (*wait_for_recv_completion)(struct rdma_server_info *);
	void (*wait_for_send_completion)(struct rdma_server_info *);

    } buffer_ops;
};


void create_event_channel_server(struct rdma_server_info *se) ;
void create_id_server(struct rdma_server_info *se, enum rdma_port_space ps) ;
void init_qp_attr_server(struct rdma_server_info *se) ;

void init_hints_server(struct rdma_server_info *se) ;

void create_ep_server(struct rdma_server_info *se) ;

void get_request(struct rdma_server_info *se) ;

void accept_connection(struct rdma_server_info *se) ;

void init_conn_param_server(struct rdma_server_info *se) ;

void start_listen(struct rdma_server_info *se) ;
void teardown_client_resources(struct rdma_server_info *);




void *listenThread(void *data) ;

void start_listen_thread(struct rdma_server_info *se) ;

void rdma_send(struct rdma_server_info *s, struct rdma_buffer_info *b, size_t size) ;
void rdma_recv(struct rdma_server_info *s, struct rdma_buffer_info *b, size_t size) ;

void wait_for_recv_completion(struct rdma_server_info *s) ;
void wait_for_send_completion(struct rdma_server_info *s) ;

struct rdma_server_info *init_rdma_server(char *port) ;

#endif /* __RDMAS_H */
