#include "rdma_client.h"
#include "rdma_common.h"
#include "zmalloc.h"

void init_client(struct rdma_client_info *se) {
	build_context(&se->context);
	createQueuePair(&se->context);
	client_exh_data(&se->context, se->server_ip, se->server_port);
	modify_qp_state_rtr(&se->context);
	modify_qp_state_rts(&se->context);
}


static struct rdma_client_info default_client_ops= {
	.connect_ops={
		//        .create_ep = create_ep_client,
		//        .init_qp_attr = init_qp_attr_client,
		//        .init_hints = init_hints_client,
		//        .RDMA_connect_to_server = RDMA_connect_to_server,
		.rd_connect = rd_connect,
		//        .destroy_cq = destroy_cq,
	},
	.buffer_ops={
		//        .rdma_send = rdma_send_client,
		//        .rdma_recv = rdma_recv_client,
		//        .post_read = post_read_client,
		//        .post_read_from_offset = post_read_from_offset,
		//        .post_write_at_address = post_write_at_address_client,
		//        .post_write = post_write_client,
		//        .wait_for_send_completion = wait_for_send_completion_client,
		.wait_for_send_completion_non_blocking = wait_for_send_completion_with_wc_client_non_blocking,
		//        .wait_for_recv_completion = wait_for_recv_completion_client,
		.wait_for_send_completion_with_wc = wait_for_send_completion_with_wc_client,
		//        .wait_for_multiple_send_completions = wait_for_multiple_send_completions_client,
		//
	}
};

struct rdma_client_info *init_rdma_client(const char *server_ip, const char *server_port){
	struct rdma_client_info *c = (struct rdma_client_info *) zmalloc(sizeof(struct rdma_client_info));
	strncpy(c->server_ip, server_ip, 60);
	strncpy(c->server_port, server_port, 60);
	printf("haha %s %s\n", c->server_ip, server_ip);

	c->connect_ops = default_client_ops.connect_ops;
	c->buffer_ops = default_client_ops.buffer_ops;
	return c;
}

int rd_connect(struct rdma_client_info *ci) {
	FILE *debug_file = fopen("/tmp/rdma_debug.log", "a");
	if (!debug_file) {
		perror("Error opening debug file");
		return 0;
	}

	build_context(&ci->context);
	createQueuePair(&ci->context);
	client_exh_data(&ci->context, ci->server_ip, ci->server_port);
	modify_qp_state_rtr(&ci->context);
	modify_qp_state_rts(&ci->context);

	fclose(debug_file);
	return 1;
}

struct ibv_wc *wait_for_send_completion_with_wc_client(struct rdma_client_info *c) {
	int ret;
	struct ibv_wc *wc;
	wc = (struct ibv_wc *) zmalloc(sizeof(struct ibv_wc));
	int result;

	do {
		result = ibv_poll_cq(c->context.cq, 1, wc);
	} while (result == 0);
	return wc;
}


int wait_for_send_completion_with_wc_client_non_blocking(struct rdma_client_info *c){
    int ret;
    struct ibv_wc *wc;
    wc = (struct ibv_wc *) zmalloc(sizeof(struct ibv_wc));
    ret = ibv_poll_cq(c->context.cq, 1, wc);
    free(wc);
    return ret;
}

