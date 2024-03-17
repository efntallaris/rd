#ifndef __RDMAB_H 
#define __RDMAB_H
#include <netdb.h>
#include <errno.h>
#include <getopt.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>

#include "zmalloc.h"

struct rdma_buffer_info {
    char 		dummy[10];
    struct ibv_qp	*qp;
    struct ibv_pd	*pd;
    char 		*buffer;
    size_t		size;
    struct ibv_mr	*mr;
    int			buffer_access;
    struct {

	    uint64_t		buf_va;
	    uint32_t		buf_rkey;
    }remote_data;
    
    struct {

	    struct ibv_mr *(*getMR)(struct rdma_buffer_info *);
	    char *(*getBufferPointer)(struct rdma_buffer_info *);
	    size_t (*getSize)(struct rdma_buffer_info *);
	    void (*allocateBuffer)(struct rdma_buffer_info *);
	    void (*registerBuffer)(struct rdma_buffer_info *);
	    void (*setSize)(struct rdma_buffer_info *, size_t);
	    void (*setBufferData)(struct rdma_buffer_info *, uint64_t, uint32_t);
	    uint32_t (*getRemoteKey)(struct rdma_buffer_info *);
	    void (*addBufferDataToBuffer)(struct rdma_buffer_info *);
	    void (*setRemoteData)(struct rdma_buffer_info *, uint64_t , uint32_t );
    } buffer_ops;

};

struct ibv_mr *getMR(struct rdma_buffer_info *b);

char *getBufferPointer(struct rdma_buffer_info *b);

size_t getSize(struct rdma_buffer_info *b);

void allocateBuffer(struct rdma_buffer_info *b);

void registerBuffer(struct rdma_buffer_info *b);

void setSize(struct rdma_buffer_info *b, size_t newSize);

uint32_t getRemoteKey(struct rdma_buffer_info *b);

void addBufferDataToBuffer(struct rdma_buffer_info *b);

void setRemoteData(struct rdma_buffer_info *b, uint64_t buf_va, uint32_t buf_rkey);

struct rdma_buffer_info *init_rdma_buffer(struct ibv_qp *qp, struct ibv_pd *pd, char *buffer, size_t size, int access);
#endif /* __RDMAB_H */
