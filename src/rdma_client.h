#ifndef RDMA_CLIENT_H
#define RDMA_CLIENT_H
#include <netdb.h>
#include <errno.h>
#include <getopt.h>
#include "rdma_buffer.h"
#include "rdma_common.h"
#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h> // Include the header file that declares close()
#include <infiniband/verbs.h>


struct rdma_client_info {
    char server_ip[60];
    char server_port[60];
    struct rdma_context context;

    struct {
        void (*dummy_func)();
        int (*rd_connect)(struct rdma_client_info *);
        //void (*create_ep)(struct rdma_client_info *);
        //void (*init_qp_attr)(struct rdma_client_info *);
        //void (*init_hints)(struct rdma_client_info *);
        //int (*RDMA_connect_to_server)(struct rdma_client_info *);
        void (*destroy_cq)(struct rdma_client_info *);
    } connect_ops;

    struct {

//        void (*rdma_send)(struct rdma_client_info *, struct rdma_buffer_info *, size_t);
//        void (*rdma_recv)(struct rdma_client_info *, struct rdma_buffer_info *, size_t);
//        void (*post_read)(struct rdma_buffer_info *);
//        /* This will do ptr+offset and will read n bytes from the offset*/
//        void (*post_read_from_offset)(struct rdma_buffer_info *, size_t, size_t );
//        void (*post_write)(struct rdma_buffer_info *);
//	int (*post_write_at_address)(struct rdma_buffer_info *, char *local_addr, uint64_t remote_addr, uint32_t remote_key, size_t len);
//        void (*wait_for_recv_completion)(struct rdma_client_info *);
//        int (*wait_for_send_completion)(struct rdma_client_info *);
	int (*wait_for_send_completion_non_blocking)(struct rdma_client_info *c);
        struct ibv_wc *(*wait_for_send_completion_with_wc)(struct rdma_client_info *);
//	void (*wait_for_multiple_send_completions)(int , struct rdma_client_info *);
//	char *(*get_wc_error_string)(enum ibv_wc_status);
//
    } buffer_ops;


};


void init_client(struct rdma_client_info *se);
struct rdma_client_info *init_rdma_client(const char *server_ip, const char *server_port);
int rd_connect(struct rdma_client_info *ci);

struct ibv_wc *wait_for_send_completion_with_wc_client(struct rdma_client_info *c);
int wait_for_send_completion_with_wc_client_non_blocking(struct rdma_client_info *c);
#endif /* RDMA_CLIENT_H */
