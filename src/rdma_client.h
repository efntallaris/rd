
#ifndef __RDMAC_H
#define __RDMAC_H
#include <netdb.h>
#include <errno.h>
#include <getopt.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include "rdma_buffer.h"

#include "zmalloc.h"
/* Capacity of the completion queue (CQ) */
#define CQ_CAPACITY (21800)
/* MAX SGE capacity */
#define MAX_SGE (1)
/* MAX work requests */
#define MAX_WR (4096)

#define MAX_SEND_WR (21800);
#define MAX_SEND_SGE (1);
#define MAX_RECV_WR (21800);
#define MAX_RECV_SGE (1);


#define SG_SIG_ALL (0);
struct rdma_client_info {
    struct rdma_cm_id *id;
    struct rdma_addrinfo hints;
    struct ibv_qp_init_attr attr;
    struct rdma_addrinfo *res;

    char ip[60];
    char ip_address[60];
    char port[60];
    struct {
        void (*dummy_func)();
        int (*rd_connect)(struct rdma_client_info *);
        void (*create_ep)(struct rdma_client_info *);
        void (*init_qp_attr)(struct rdma_client_info *);
        void (*init_hints)(struct rdma_client_info *);
        int (*RDMA_connect_to_server)(struct rdma_client_info *);
        void (*destroy_cq)(struct rdma_client_info *);
    } connect_ops;

    struct {

        void (*rdma_send)(struct rdma_client_info *, struct rdma_buffer_info *, size_t);
        void (*rdma_recv)(struct rdma_client_info *, struct rdma_buffer_info *, size_t);
        void (*post_read)(struct rdma_buffer_info *);
        /* This will do ptr+offset and will read n bytes from the offset*/
        void (*post_read_from_offset)(struct rdma_buffer_info *, size_t, size_t );
        void (*post_write)(struct rdma_buffer_info *);
	int (*post_write_at_address)(struct rdma_buffer_info *, char *local_addr, uint64_t remote_addr, uint32_t remote_key, size_t len);
        void (*wait_for_recv_completion)(struct rdma_client_info *);
        int (*wait_for_send_completion)(struct rdma_client_info *);
	int (*wait_for_send_completion_non_blocking)(struct rdma_client_info *c);
        struct ibv_wc *(*wait_for_send_completion_with_wc)(struct rdma_client_info *);
	void (*wait_for_multiple_send_completions)(int , struct rdma_client_info *);
	char *(*get_wc_error_string)(enum ibv_wc_status);

    } buffer_ops;


};

void init_hints_client( struct rdma_client_info *ci);

void init_qp_attr_client( struct rdma_client_info *ci);

void create_ep_client( struct rdma_client_info *ci);

int rd_connect( struct rdma_client_info *ci);

int RDMA_connect_to_server( struct rdma_client_info *ci);

void post_read_client(struct rdma_buffer_info *b);

void post_read_from_offset(struct rdma_buffer_info *b, size_t offset, size_t n);

void post_write_client(struct rdma_buffer_info *b);

int post_write_at_address_client(struct rdma_buffer_info *b, char *local_addr, uint64_t remote_addr, uint32_t remote_key, size_t len);

void rdma_send_client(struct rdma_client_info *c, struct rdma_buffer_info *b, size_t size);

void rdma_recv_client(struct rdma_client_info *c, struct rdma_buffer_info *b,  size_t size);

void wait_for_recv_completion_client(struct rdma_client_info *c);

int wait_for_send_completion_client(struct rdma_client_info *c);
char * get_wc_error_string(enum ibv_wc_status);

struct ibv_wc *wait_for_send_completion_with_wc_client(struct rdma_client_info *c);

int wait_for_send_completion_with_wc_client_non_blocking(struct rdma_client_info *c);

void wait_for_multiple_send_completions_client(int number_of_completions, struct rdma_client_info *c);

struct rdma_client_info *init_rdma_client(char *ip, char *port);

#endif /* __RDMAC_H */
