#ifndef ALLOCATOR_H_
#define ALLOCATOR_H_

#include <stdio.h>

#include "server.h"

#define BLOCK_SIZE_BYTES 5000000 

typedef struct allocated_block alloc_bloc_t;
typedef struct r_allocator r_allocator_t;

// typedef struct allocated_block {
//     size_t  bytes_total_in_use;
//     size_t  bytes_free;
//     char    *block_start;
//     struct allocated_block *next;
// } alloc_bloc_t;

void r_allocator_init();

/* 
* Allocates a new empty block for the given slot. 
* If slot already has block, it will add the new block at the front of the block list
* returns a pointer at the beginning of the newly allocated block (including the block buffer and the block metadata)
*/
void * r_allocator_alloc_new_empty_block(int slot);

// returns a pointer on the inserted data
// void * r_allocator_insert_data(int slot, void *data, size_t size, int *allocated_new_block);

/* 
* creates a new segment in a block that has enough space for the segment
* segment layout: <seg header> <payload> <padding> <seg footer>
* seg header/footer: 4bytes that contain the size of the segment and a flag (lowest bit) indicating if segment is free/used
* padding: is used to align segments at 8bytes addresses (so the last 2 bits are always 0. We use these bits as flags)
* payload layout: <key meta size> <key meta> <key size> <key> <value meta size> <value meta> <value size> <value>
* returns pointer at the header of the segment
*/
void * r_allocator_insert_kv(int slot, 
                            void *key, size_t key_size, 
                            void *value, size_t value_size,
                            robj *key_meta, size_t key_meta_size,
                            robj *value_meta, size_t value_meta_size,
                            int *allocated_new_block,
                            robj **ptr_key_meta,
                            robj **ptr_val_meta);

// @DEPRECATED
// get the pointer to the data
// WARNING: data are not cloned, the caller is responsible for any changes on the data
// void * r_allocator_get_data(void *ptr);

// @DEPRECATE ??????
unsigned int r_allocator_get_slot_num_blocks(int slot);

/* 
* returns the head of the block list of the slot
*/
alloc_bloc_t * r_allocator_get_block_list_for_slot(int slot);

/*
* free the segment of the whole K-V
* parameter: pointer to the key metadata
*/
void r_allocator_free_kv(int slot, void *key_meta_ptr);

/* free ALL blocks of the slot */
void free_slot(int slot);

/* free all blocks of all slots */
void r_allocator_free_world();

/* prints num of blocks for the slot and for each slot print the num of used/free segments and bytes */
void r_allocator_print_stats_for_slot(int slot);


/* get total execution time of allocate_new_empty_block in microsec */
long get_empty_blocks_alloc_exec_time();

/* * * * * * * * * * * * * * * * * * * * * * * * * */
/* * * * * * * * * *  ITERATOR * * * * * * * * * * */
/* * * * * * * * * * * * * * * * * * * * * * * * * */
typedef void * (*getNextFunction)(int slot, robj **key_meta, robj **value_meta);

typedef struct segment_iterator {
	alloc_bloc_t	*cur_block;
	void 			*cur_segment;   //points to current segment (after the seg header)
	getNextFunction	getNext;
} segment_iterator_t;

segment_iterator_t * create_iterator_for_slot(int slot);


/////////// DEBUG
void traverse_print_slot_blocks(int slot);
void freelist_print(int slot);
// void freelist_print_debug(int slot);
alloc_bloc_t * get_block_from_ptr(int slot, void *ptr);
void * get_buffer_from_block(void *blk);

#endif
