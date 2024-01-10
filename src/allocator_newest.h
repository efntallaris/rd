#ifndef ALLOCATOR_H_
#define ALLOCATOR_H_

#include "server.h"
#include <stdio.h>

/* BLOCK_SIZE_BYTES must be 8-aligned aligned e.g. 120, 128, 136 but NOT 130 */
//#define BLOCK_SIZE_BYTES 5*1024*1024*1024//1024//192//4000000//256//224//1024//256
//#define BLOCK_SIZE_BYTES 5242880//1024//192//4000000//256//224//1024//256
#define BLOCK_SIZE_BYTES 500*1024//1024//192//4000000//256//224//1024//256

typedef struct allocated_block alloc_bloc_t;
typedef struct r_allocator r_allocator_t;

typedef struct slot_stats {
    unsigned int    blocks;
    unsigned int    bytes_used;
    unsigned int    bytes_free;
} slot_stats_t;


void r_allocator_init();

/* 
* Allocates a new empty block for the given slot. 
* If slot already has block, it will add the new block at the front of the block list
* returns a pointer at the beginning of the newly allocated block (including the block buffer and the block metadata)
*/
void * r_allocator_alloc_new_empty_block(int slot);

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

/* 
* returns the head of the block list of the slot
*/
alloc_bloc_t * r_allocator_get_block_list_for_slot(int slot);

/*
* parm number_of_results is populated with the number of the slot blocks (the size of array the function returs)
* return a pointer to an array that contains pointers to the block buffers of the slot
*/
char ** r_allocator_get_block_buffers_for_slot(int slot, int *number_of_results);

/*
* free the segment of the whole K-V
* parameter: pointer to the key metadata
*/
void r_allocator_free_kv(int slot, void *key_meta_ptr);

/* free ALL blocks of the slot */
void free_slot(int slot);

/* free all blocks of all slots */
void r_allocator_free_world();

/* Locks the current allocated blocks for the slot 
* i.e., no more writes are applied in the free segments of the existing blocks
* Future writes will be performed in newly allocated blocks */
void r_allocator_lock_slot_blocks(int slot);

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
void print_block_buffer(void *buffer_start);
void freelist_print(int slot);
// void freelist_print_debug(int slot);
alloc_bloc_t * get_block_from_ptr(int slot, void *ptr);
void * get_buffer_from_block(void *blk);

// TESTING
unsigned int r_allocator_get_slot_num_blocks(int slot);
slot_stats_t collect_stats_for_slot(int slot);
size_t calculate_required_space_for_segment( 
                            size_t key_size, 
                            size_t value_size,
                            size_t key_meta_size,
                            size_t value_meta_size
                            );

#endif
