
#include "rdma_client.h"

void init_hints_client( struct rdma_client_info *ci) {
    memset(&(ci->hints), 0, sizeof(struct rdma_addrinfo));
    ci->hints.ai_port_space = RDMA_PS_TCP;
    ci->hints.ai_qp_type = IBV_QPT_RC;
}

void init_qp_attr_client( struct rdma_client_info *ci) {
    memset(&(ci->attr), 0, sizeof(struct rdma_addrinfo));
    ci->attr.cap.max_send_wr = MAX_SEND_WR;
    ci->attr.cap.max_recv_wr = MAX_RECV_WR;
    ci->attr.cap.max_send_sge = MAX_SEND_SGE;
    ci->attr.cap.max_recv_sge = MAX_RECV_SGE;
    ci->attr.sq_sig_all = SG_SIG_ALL;
    ci->attr.qp_type = IBV_QPT_RC;
}


void create_ep_client(struct rdma_client_info *ci) {
    FILE *debug_file = fopen("/tmp/rdma_debug.log", "a");
    if (!debug_file) {
        perror("Error opening debug file");
        return;
    }

    int err;
    err = rdma_getaddrinfo(ci->ip_address, ci->port, &(ci->hints), &(ci->res));

    if(err) {
        fprintf(debug_file, "rdma_getaddrinfo error: %d\n", err);
        fclose(debug_file);
        return;
    } else {
        fprintf(debug_file, "rdma_getaddrinfo success\n");
    }

    err = rdma_create_ep(&(ci->id), ci->res, NULL, &(ci->attr));
    if(err) {
        fprintf(debug_file, "rdma_create_ep error: %d\n", err);
        fclose(debug_file);
        return;
    } else {
        fprintf(debug_file, "rdma_create_ep success\n");
    }

    rdma_freeaddrinfo(ci->res);
    fclose(debug_file);
}

int rd_connect(struct rdma_client_info *ci) {
    FILE *debug_file = fopen("/tmp/rdma_debug.log", "a");
    if (!debug_file) {
        perror("Error opening debug file");
        return 0;
    }

    int ret;
    ret = rdma_connect(ci->id, NULL);
    if(ret) {
        fprintf(debug_file, "rdma_connect error: %d\n", ret);
        fclose(debug_file);
        return 0;
    } else {
        fprintf(debug_file, "rdma_connect success\n");
    }

    fclose(debug_file);
    return 1;
}

int RDMA_connect_to_server( struct rdma_client_info *ci) {

    ci->connect_ops.init_hints(ci);
    ci->connect_ops.init_qp_attr(ci);
    ci->connect_ops.create_ep(ci);
    return ci->connect_ops.rd_connect(ci);
}

void post_read_client(struct rdma_buffer_info *b) {
    struct ibv_sge sge;
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr;

    int ret;
    sge.addr = (uint64_t)(uintptr_t)
               b->buffer;
    sge.length = (uint32_t) b->size;
    sge.lkey = b->buffer_ops.getMR(b) ? b->buffer_ops.getMR(b)->lkey : 0;
    wr.wr_id = 0;
    wr.sg_list = &sge;
    wr.next = NULL;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = b->remote_data.buf_va;
    wr.wr.rdma.rkey = b->remote_data.buf_rkey;


    ret = ibv_post_send(b->id->qp, &wr, &bad_wr);
    if (ret) {
        printf("STRATOS ERROR post_read -> ibv_post_sent\n");
    }
}

void post_read_from_offset(struct rdma_buffer_info *b, size_t offset, size_t n)
{
    struct ibv_sge sge;
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr;

    int ret;
    sge.addr = (uint64_t)(uintptr_t)
               b->buffer + offset;
    sge.length = (uint32_t) n;
    sge.lkey = b->buffer_ops.getMR(b) ? b->buffer_ops.getMR(b)->lkey : 0;
    wr.wr_id = 0;
    wr.sg_list = &sge;
    wr.next = NULL;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = b->remote_data.buf_va + offset;
    wr.wr.rdma.rkey = b->remote_data.buf_rkey;


    ret = ibv_post_send(b->id->qp, &wr, &bad_wr);
    if (ret) {
        printf("STRATOS ERROR post_read -> ibv_post_sent\n");
    }
}


int post_write_at_address_client(struct rdma_buffer_info *b, char *local_addr, uint64_t remote_addr, uint32_t remote_key, size_t len) {
    struct ibv_sge sge;
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr;

    int ret;
    sge.addr = (uint64_t)(uintptr_t) local_addr;
    sge.length = (uint32_t) len;
    sge.lkey = b->buffer_ops.getMR(b) ? b->buffer_ops.getMR(b)->lkey : 0;
    wr.wr_id = 0;
    wr.sg_list = &sge;
    wr.next = NULL;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = remote_key;
    return ibv_post_send(b->id->qp, &wr, &bad_wr);

}

void post_write_client(struct rdma_buffer_info *b) {
    struct ibv_sge sge;
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr;

    int ret;
    sge.addr = (uint64_t)(uintptr_t)
               b->buffer_ops.getBufferPointer(b);
    sge.length = (uint32_t) b->buffer_ops.getSize(b);
    sge.lkey = b->buffer_ops.getMR(b) ? b->buffer_ops.getMR(b)->lkey : 0;
    wr.wr_id = 0;
    wr.sg_list = &sge;
    wr.next = NULL;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = b->remote_data.buf_va;
    wr.wr.rdma.rkey = b->remote_data.buf_rkey;


    ret = ibv_post_send(b->id->qp, &wr, &bad_wr);
    if (ret) {
        printf("STRATOS ERROR post_read -> ibv_post_sent\n");
    }
}


void rdma_send_client(struct rdma_client_info *c, struct rdma_buffer_info *b, size_t size) {
    int ret;
    ret = rdma_post_send(c->id, NULL, b->buffer_ops.getBufferPointer(b), size, b->buffer_ops.getMR(b), 0);
    if(ret) {
        printf("STRATOS rdma_post_send Error\n");
    }

}


void rdma_recv_client(struct rdma_client_info *c, struct rdma_buffer_info *b,  size_t size) {

    int ret;
    ret = rdma_post_recv(c->id, NULL, b->buffer_ops.getBufferPointer(b), size, b->buffer_ops.getMR(b));
    if(ret) {
        printf("STRATOS rdma_post_send Error\n");
    }

}

void wait_for_recv_completion_client(struct rdma_client_info *c) {

    struct ibv_wc wc;
    int ret;
    while ((ret = rdma_get_recv_comp(c->id, &wc)) == 0);
    if (ret < 0) {
        printf("rdma_get_recv_comp error");
    }
    return;

}

void destroy_cq(struct rdma_client_info *c) {

    ibv_destroy_cq(c->id->send_cq);
    ibv_destroy_cq(c->id->recv_cq);
    return;

}

void wait_for_multiple_send_completions_client(int number_of_completions, struct rdma_client_info *c) {
    struct ibv_cq *cq;
    void *context;
    int ret;
    int total_received_completions;
    struct ibv_wc wc;

    do {
        ret = ibv_poll_cq(c->id->send_cq, 1, &wc);
        if (ret)
            break;

        ret = ibv_req_notify_cq(c->id->send_cq, 0);
        if (ret)
            return rdma_seterrno(ret);

        ret = ibv_poll_cq(c->id->send_cq, 1, &wc);

	total_received_completions = ret;
	
        if (ret)
            break;

        ret = ibv_get_cq_event(c->id->send_cq_channel, &cq, &context);
        if (ret)
            //return ret;
	    break;

        assert(cq == c->id->send_cq && context == c->id);
        ibv_ack_cq_events(c->id->send_cq, total_received_completions); 
    } while (1);

//    return (ret < 0) ? rdma_seterrno(ret) : ret;
    return;

}

int wait_for_send_completion_client(struct rdma_client_info *c) {

    int ret;
    struct ibv_wc wc;
    while ((ret = rdma_get_send_comp(c->id, &wc)) == 0);
    if (ret < 0) {
        printf("rdma_get_send_comp error");
    }
    return ret;

}


struct ibv_wc *wait_for_send_completion_with_wc_client(struct rdma_client_info *c) {

    int ret;
    struct ibv_wc *wc;
    wc = (struct ibv_wc *) malloc(sizeof(struct ibv_wc));
    while ((ret = rdma_get_send_comp(c->id, wc)) == 0);
    if (ret < 0) {
        printf("rdma_get_send_comp error");
    }
    return wc;

}

int wait_for_send_completion_with_wc_client_non_blocking(struct rdma_client_info *c){
    int ret;
    struct ibv_wc *wc;
    wc = (struct ibv_wc *) malloc(sizeof(struct ibv_wc));
    ret = ibv_poll_cq(c->id->send_cq, 1, wc);
    free(wc);
    return ret;

}


static struct rdma_client_info default_client_ops= {
    .connect_ops={
        .create_ep = create_ep_client,
        .init_qp_attr = init_qp_attr_client,
        .init_hints = init_hints_client,
        .RDMA_connect_to_server = RDMA_connect_to_server,
        .rd_connect = rd_connect,
        .destroy_cq = destroy_cq,
    },
    .buffer_ops={
        .rdma_send = rdma_send_client,
        .rdma_recv = rdma_recv_client,
        .post_read = post_read_client,
        .post_read_from_offset = post_read_from_offset,
        .post_write_at_address = post_write_at_address_client,
        .post_write = post_write_client,
        .wait_for_send_completion = wait_for_send_completion_client,
	.wait_for_send_completion_non_blocking = wait_for_send_completion_with_wc_client_non_blocking,
        .wait_for_recv_completion = wait_for_recv_completion_client,
        .wait_for_send_completion_with_wc = wait_for_send_completion_with_wc_client,
        .wait_for_multiple_send_completions = wait_for_multiple_send_completions_client,

    }
};


struct rdma_client_info *init_rdma_client(char *ip, char *port) {
    struct rdma_client_info *c = (struct rdma_client_info *) malloc(sizeof(struct rdma_client_info));
    strcpy(c->ip_address, ip);
    strcpy(c->port, port);
    c->connect_ops = default_client_ops.connect_ops;
    c->buffer_ops = default_client_ops.buffer_ops;
    return c;
}

