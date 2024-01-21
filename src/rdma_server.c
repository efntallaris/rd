#include "rdma_server.h"
#include <stdlib.h>
#include <errno.h>
#include <stdio.h>

void create_event_channel_server(struct rdma_server_info *se) {
	se->cm_channel = rdma_create_event_channel();
	if (!se->cm_channel) {
		printf("STRATOS rdma_create_event_channel ERROR");
	}
	return;
}

void create_id_server(struct rdma_server_info *se, enum rdma_port_space ps) {
	int err;
	err = rdma_create_id(se->cm_channel, &(se->listen_id), NULL, ps);
	if (err) {
		FILE *debug_file = fopen("/tmp/rdma_debug.log", "a");
		if (!debug_file) {
			perror("Error opening debug file");
			return;
		}

		fprintf(debug_file, "rdma_create_id() ERROR: %d\n", err);
	}
	printf("RDMA CREATE ID SUCCESS \n");
	return;
}

void init_qp_attr_server(struct rdma_server_info *se) {
	memset(&(se->attr), 0, sizeof se->attr);
	se->attr.cap.max_send_wr = MAX_SEND_WR;
	se->attr.cap.max_recv_wr = MAX_RECV_WR;
	se->attr.cap.max_send_sge = MAX_SEND_SGE;
	se->attr.cap.max_recv_sge = MAX_RECV_SGE;
	se->attr.sq_sig_all = SG_SIG_ALL;
	se->attr.qp_type = IBV_QPT_RC;
}

void init_hints_server(struct rdma_server_info *se) {
	memset(&(se->hints), 0, sizeof(struct rdma_addrinfo));
	se->hints.ai_flags = RAI_PASSIVE;
	se->hints.ai_port_space = RDMA_PS_TCP;
	se->hints.ai_qp_type = IBV_QPT_RC;
}

void create_ep_server(struct rdma_server_info *se) {
	int err;
	err = rdma_getaddrinfo(NULL, se->serverPort, &(se->hints), &(se->res));
	if(err) {
		printf("STRATOS rdma_getaddrinfo error");
		FILE *debug_file = fopen("/tmp/rdma_debug.log", "a");
		if (!debug_file) {
			perror("Error opening debug file");
			return;
		}

		fprintf(debug_file, "rdma_getaddrinfo() ERROR: %d\n", err);
	}
	err = rdma_create_ep(&(se->listen_id), se->res, NULL, &(se->attr));
	if(err) {
		printf("STRATOS rdma_create_ep ERROR");
		FILE *debug_file = fopen("/tmp/rdma_debug.log", "a");
		if (!debug_file) {
			perror("Error opening debug file");
			return;
		}

		fprintf(debug_file, "rdma_create_ep() ERROR: %d\n", err);
		return ;
	}
	rdma_freeaddrinfo(se->res);
}

void get_request(struct rdma_server_info *se) {
	int ret;
	ret = rdma_get_request(se->listen_id, &(se->id));
	if(ret) {
		printf("rdma_get_request_error");
	}
}

void accept_connection(struct rdma_server_info *se) {
	int ret;
	ret = rdma_accept(se->id, &(se->conn_param));
	if(ret) {
		FILE *debug_file = fopen("/tmp/rdma_debug.log", "a");
		if (!debug_file) {
			perror("Error opening debug file");
			return;
		}

		fprintf(debug_file, "rdma_accept() ERROR: %s\n", strerror(errno));
		fclose(debug_file);
		printf("STRATOS rdma_accept_error");
	}
	printf("STRATOS CLIENT ACCEPTED\n");
}

void init_conn_param_server(struct rdma_server_info *se) {
	memset(&(se->conn_param), 0, sizeof(struct rdma_conn_param));
	se->conn_param.responder_resources = 32;
	se->conn_param.initiator_depth = 32;

}

void start_listen(struct rdma_server_info *se) {
	se->server_ops.create_event_channel(se);
	se->server_ops.create_id(se, RDMA_PS_TCP);
	se->server_ops.init_qp_attr(se);
	se->server_ops.init_hints(se);
	se->server_ops.create_ep(se);
	se->server_ops.start_listen_thread(se);
}




void *listenThread(void *data) {

	int err;
	struct rdma_server_info *se = (struct rdma_server_info *) data;
	err = rdma_listen(se->listen_id, MAX_CONNECTIONS);
	if(err) {
		printf("STRATOS rdma_listen() error");
		FILE *debug_file = fopen("/tmp/rdma_debug.log", "a");
		if (!debug_file) {
			perror("Error opening debug file");
			return;
		}

		fprintf(debug_file, "rdma_listen() ERROR: %d\n", err);
		printf("STRATOS rdma_accept_error");
	}
	se->server_ops.get_request(se);
	se->server_ops.init_conn_param(se);
	se->server_ops.accept_connection(se);


	return NULL;
}

void start_listen_thread(struct rdma_server_info *se) {
	int rc;
	rc = pthread_create(&(se->listen_thread), NULL, listenThread, (void*)se);
	if(rc)			/* could not create thread */
	{
		printf("\n ERROR: return code from pthread_create is %d \n", rc);
		exit(1);
	}

}

void rdma_send(struct rdma_server_info *s, struct rdma_buffer_info *b, size_t size) {
	int ret;
	ret = rdma_post_send(s->id, NULL, b->buffer_ops.getBufferPointer(b), size, b->buffer_ops.getMR(b), 0);
	if(ret) {
		printf("STRATOS rdma_post_send Error\n");
	}

}
void rdma_recv(struct rdma_server_info *s, struct rdma_buffer_info *b, size_t size) {

	int ret;
	ret = rdma_post_recv(s->id, NULL, b->buffer_ops.getBufferPointer(b), size, b->buffer_ops.getMR(b));
	if(ret) {
		printf("STRATOS rdma_post_send Error\n");
	}

}

void wait_for_recv_completion(struct rdma_server_info *s) {

	int ret;
	struct ibv_wc wc;
	while ((ret = rdma_get_recv_comp(s->id, &wc)) == 0);
	if (ret < 0) {
		printf("rdma_get_recv_comp error");
	}
	return;

}
void wait_for_send_completion(struct rdma_server_info *s) {

	int ret;
	struct ibv_wc wc;
	while ((ret = rdma_get_send_comp(s->id, &wc)) == 0);
	if (ret < 0) {
		printf("rdma_get_send_comp error");
	}
	return;

}

static struct rdma_server_info default_server_ops= {
	.server_ops={
		.create_event_channel = create_event_channel_server,
		.create_id = create_id_server,
		.init_qp_attr = init_qp_attr_server,
		.init_hints = init_hints_server,
		.create_ep = create_ep_server,
		.get_request = get_request,
		.accept_connection = accept_connection,
		.init_conn_param = init_conn_param_server,
		.start_listen = start_listen,
		.start_listen_thread= start_listen_thread,
		.teardown_client_resources = teardown_client_resources,
	},
	.buffer_ops = {
		.rdma_send = rdma_send,
		.rdma_recv = rdma_recv,
		.wait_for_send_completion = wait_for_send_completion,
		.wait_for_recv_completion = wait_for_recv_completion,
	}
};


struct rdma_server_info *init_rdma_server(char *port) {
	struct rdma_server_info *s = (struct rdma_server_info *) malloc(sizeof(struct rdma_server_info));
	strcpy(s->serverPort, port);
	s->server_ops = default_server_ops.server_ops;
	s->buffer_ops = default_server_ops.buffer_ops;
	s->server_ops.start_listen(s);
	return s;
}

void teardown_client_resources(struct rdma_server_info *se) {
	// Transition the QP to the Error state
	struct ibv_qp_attr attr;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_ERR;
	if (se->id && se->id->qp) {
		ibv_modify_qp(se->id->qp, &attr, IBV_QP_STATE);
	}

	// Destroy the QP
	if (se->id && se->id->qp) {
		ibv_destroy_qp(se->id->qp);
	}

	//    // TODO: Deregister Memory Regions if they were registered
	//
	//    // TODO: Destroy the Completion Queue if it's not shared with other QPs
	//    
	//    // Disconnect and destroy the RDMA CM ID for the client
	//    if (se->id) {
	//        rdma_disconnect(se->id);
	//        rdma_destroy_id(se->id);
	//    }

	// TODO: Handle any other cleanup tasks specific to the application's logic
}
