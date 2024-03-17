#include "rdma_server.h"
#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h> // Include the header file that declares close()
#include "rdma_common.h"

void start_listen(struct rdma_server_info *se) {
	int rc;
	rc = pthread_create(&(se->listen_thread), NULL, listenThread, (void*)se);
	if(rc)			/* could not create thread */
	{
		printf("\n ERROR: return code from pthread_create is %d \n", rc);
		exit(1);
	}
}



void *listenThread(void *data) {
	int err;
	struct rdma_server_info *se = (struct rdma_server_info *) data;
	// build_context(&se->context);
	// createQueuePair(&se->context);
	// server_exh_data(&se->context, se->server_port);
	// modify_qp_state_rtr(&se->context);
	// modify_qp_state_rts(&se->context);
	return NULL;
}

void start_listen_thread(struct rdma_server_info *se) {
	int rc;
	// rc = pthread_create(&(se->listen_thread), NULL, listenThread, (void*)se);
	// if(rc)			/* could not create thread */
	// {
	// 	printf("\n ERROR: return code from pthread_create is %d \n", rc);
	// 	exit(1);
	// }

}


static struct rdma_server_info default_server_ops= {
	.server_ops={
		.start_listen = start_listen,
		.start_listen_thread= start_listen_thread,
		//todo destroy ctx
	},
	.buffer_ops = {
//		.rdma_send = rdma_send,
//		.rdma_recv = rdma_recv,
//		.wait_for_send_completion = wait_for_send_completion,
//		.wait_for_recv_completion = wait_for_recv_completion,
	}
};




struct rdma_server_info *init_rdma_server(char *server_port){
	struct rdma_server_info *s = (struct rdma_server_info *) malloc(sizeof(struct rdma_server_info));
	strncpy(s->server_port, server_port, sizeof(server_port));

	printf("%s server port\n", s->server_port);
	s->server_ops = default_server_ops.server_ops;
	s->buffer_ops = default_server_ops.buffer_ops;
	s->server_ops.start_listen(s);


}
