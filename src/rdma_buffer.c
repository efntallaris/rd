#include "server.h"
#include "rdma_buffer.h"

struct ibv_mr *getMR(struct rdma_buffer_info *b){
	return b->mr;
}


char *getBufferPointer(struct rdma_buffer_info *b){
	return b->buffer;
}

size_t getSize(struct rdma_buffer_info *b){
	return b->size;
}

void allocateBuffer(struct rdma_buffer_info *b){
	b->buffer = (char *) malloc(b->size);
	b->mr = ibv_reg_mr(b->pd, (void *)b->buffer, b->size, b->buffer_access);
}

void registerBuffer(struct rdma_buffer_info *b){
	b->mr = ibv_reg_mr(b->pd, (void *)b->buffer, b->size, b->buffer_access);
}

void setSize(struct rdma_buffer_info *b, size_t newSize){
	b->size = newSize;
}


uint32_t getRemoteKey(struct rdma_buffer_info *b){
	return b->buffer_ops.getMR(b)->rkey;
}


void addBufferDataToBuffer(struct rdma_buffer_info *b){
	b->remote_data.buf_va = (uintptr_t)b->buffer_ops.getBufferPointer(b);
	b->remote_data.buf_rkey = b->buffer_ops.getRemoteKey(b);
	memcpy(b->buffer_ops.getBufferPointer(b), &(b->remote_data), sizeof(b->remote_data));
}


static struct rdma_buffer_info default_buffer_ops = {
	.buffer_ops = {
		.getMR = getMR, 	
		.getBufferPointer = getBufferPointer,
		.getSize = getSize,
		.allocateBuffer = allocateBuffer,
		.registerBuffer = registerBuffer,
		.getRemoteKey = getRemoteKey,
		.addBufferDataToBuffer = addBufferDataToBuffer,
		.setRemoteData = setRemoteData,
	}

};


struct rdma_buffer_info *init_rdma_buffer(struct ibv_qp *qp, struct ibv_pd *pd, char *buffer, size_t size, int access){
	struct rdma_buffer_info *newBuffer = (struct rdma_buffer_info *) zmalloc(sizeof(struct rdma_buffer_info));
	if(newBuffer == NULL){
		serverLog(LL_WARNING, "STRATOS SOMETHING WENT WRONG init_rdma_buffer");

	}
	newBuffer->buffer_ops = default_buffer_ops.buffer_ops;
	newBuffer->qp = qp;
	newBuffer->pd = pd;
	newBuffer->buffer = buffer;
	newBuffer->size = size;
	newBuffer->buffer_access = access;
	if(access == 0){
		newBuffer->buffer_access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
	}
	if(access == 10){
		newBuffer->buffer_access = 0;
	}


	newBuffer->buffer_ops.registerBuffer(newBuffer);
	if(newBuffer->mr == NULL){
		return NULL;
	}
	return newBuffer;
}


void setRemoteData(struct rdma_buffer_info *b, uint64_t buf_va, uint32_t buf_rkey){
	b->remote_data.buf_va = buf_va;
	b->remote_data.buf_rkey = buf_rkey; 
}
