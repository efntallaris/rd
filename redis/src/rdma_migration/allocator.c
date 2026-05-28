/*
 * alloc_explicitly.c
 *
 * Antonis Papaioannou
 * 2021
 *
 * Allocate blocks of 256 KB for each slot
 * Each block contains ENTRIES of application data.
 * Entries must be aligned to doubleword (8 bytes) boundaries so the last bits of the size is always 0.
 * We use the lowest bits as flags indicating if the segment is allocated (last bit) and 
 * if the prev segment is also allocated (1 bit before last) (see the masks defined with macros).
 * If the last bit is 0, the segment is free
 * if 1 bit befora last is 0, the prev segment is free
 * For each entry (called segment) we add a header and boundary footer allowing for coalescing.
 * We also use dummy headers at the front and at the end of each block marking the block boundaries.
 * Minimum entry size is 24 bytes which include (bytes): 
 * 4 (segment header) + 8 (next ptr (if segment is free)) + 8 (fprev prt (if seg is free)) + 4 (seg footer)
 */

/********************************************* ENTRY INFO ****************************************************
 * HEADER:       4 bytes
 * NEXT POINTER: 8 bytes
 * PREV POINTER: 8 bytes
 * FOOTER:       4 bytes
 
 < Allocated Entry >
 
            +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 Header :   |                              size of the entry   (4 bytes)                           |  |  | A|
    bp ---> +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
            |                                                                                               |
            |                              Payload and padding                                              |
            .               <key meta> <value meta> <key size> <key> <value size> <value>                   .
            .                                                                                               .
            .                                                                                               .
            +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 Footer :   |                              size of the entry   (4 bytes)                           |  |  | A|
            +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 
 
 < Free Entry >
 
            +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 Header :   |                              size of the entry    (4bytes)                           |  |  | A|
    fp ---> +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
            |                        pointer to next free entry in free list   (8 bytes)                    |
            |                                                                                               |
fp+DSIZE--> +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
            |                        pointer to previous free entry in free list   (8 bytes)                |
            |                                                                                               |
            +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 Footer :   |                              size of the entry   (4 bytes)                           |  |  | A|
            +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
***************************************** END OF ENTRY INFO **************************************************/

#include "allocator.h"
#include "internal.h"   /* RMIG_LOG → rmig_logger (wired to _serverLog) */
#include "zmalloc.h"
#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* The original 6.2.4 source uses GCC void* arithmetic and a few stylistic
 * patterns that 8.6.2's strict warning set rejects. Suppressing them here
 * keeps the allocator a verbatim port; the rdma_migration RDMA layer
 * (rdma_buffer/client/server.c) doesn't need this. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpointer-arith"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"
#pragma GCC diagnostic ignored "-Wformat"
#pragma GCC diagnostic ignored "-Wstrict-prototypes"
#pragma GCC diagnostic ignored "-Wmissing-prototypes"
#pragma GCC diagnostic ignored "-Wmissing-declarations"
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic ignored "-Wbuiltin-declaration-mismatch"

typedef unsigned int u_int;
typedef unsigned long long u_llong;

/* Single word (4) aligned or double word (8) aligned */
#define ALIGNMENT     8       /* DSIZE alignment for 64-bit machines */

/* ## GENERAL MACROS ## */

/* Basic constants and macros */
#define WSIZE               4       /* Word and header/footer size (bytes) */
#define DSIZE               8       /* Doubleword size (bytes) */
#define OVERHEAD            8       /* Needed for prologue alignment (header and footer words??) */
#define MIN_SEGMENT_SIZE    24       /* Next & prev ptr's 8 bytes for 64-bit */

/* Determine larger variable, given two */
#define MAX(x, y) ((x) > (y)? (x) : (y))  

/* Rounds up to the nearest multiple of ALIGNMENT */
#define ALIGN(p) (((size_t)(p) + (ALIGNMENT-1)) & ~0x7)

/* Pack a size and allocated bit into a word */
#define PACK(size, alloc)  ((size) | (alloc))

/* Read and write a word at address p */
#define GET(p)           (*(u_int *)(p))
#define PUT(p, val)      (*(u_int *)(p) = (val))
#define PUT_WTAG(p, val) (*(u_int *)(p) = (val) | GET_TAG(p))

/* Read the size and allocated fields from address p */ 
#define GET_SIZE(p)  (GET(p) & ~0x7)
#define GET_ALLOC(p) (GET(p) & 0x1)

/* Read the free block's reallocation tag */
#define GET_TAG(p)   (GET(p) & 0x2)

/* Set/remove the free block's reallocation tag */
#define SET_TAG(p)   (GET(p) |= 0x2) // in realloc
#define REMOVE_TAG(p) (GET(p) &= ~0x2)

/* Given block ptr bp, compute address of its header and footer */
#define HDRP(bp)       ( (void *)(bp) - WSIZE ) // move pointer 1 word back
//get the size of the segment. size includes the header and footer (of size 1 word each).
//bp points at the beginning of data (after header). Moving bp + size bytes we end up at the end of the header
// of the next segment. We go 1 word back = at the end of the footer.
// We go another word back to go to the beginning of the footer)
#define FTRP(bp)       ( (void *)(bp) + GET_SIZE(HDRP(bp)) - DSIZE )

/* Given block ptr bp, compute address of next & previous segments */
// #define NEXT_SEGMENT(bp)  ( (void *)(bp) + GET_SIZE(((void *)(bp) - WSIZE)) )
#define NEXT_SEGMENT(bp)  ( (void *)(bp) + GET_SIZE(HDRP(bp)) )     // goes over the header of the next segment
#define PREV_SEGMENT(bp)  ( (void *)(bp) - GET_SIZE(((void *)(bp) - DSIZE)) )   // goes over the header of the prev segment

/* ## EXPLICIT FREE LIST MACROS ## */

/* Compute address of next & prev free block entries */
#define NEXT_FREEP(bp) ( *(void **)bp )
#define PREV_FREEP(bp) ( *(void **)(bp + DSIZE) )

#define SLOTS 30000
#define ENTRY_HEADER_SIZE WSIZE //this is 4 bytes used to store the size of the sub-fields in the segment
#define KEY_META_SIZE sizeof(robj)
#define VAL_META_SIZE sizeof(robj)

struct allocated_block {
    size_t  bytes_total_in_use;
    size_t  bytes_free;
    char    *block_start;
    struct  allocated_block *next;
    struct  allocated_block *prev;
    size_t  segments_used;     //TODO: DEBUG remove in production
    size_t  segments_free;     //TODO: DEBUG remove in production
};

struct r_allocator {
    alloc_bloc_t    *slot_blocks[SLOTS];    // array with lists of blocks. A list per slot
    alloc_bloc_t    *slot_blocks_tail[SLOTS];   // points at the last block of the list for each slot
    unsigned int    slot_blocks_num[SLOTS]; // number of blocks per slot
    void            *free_list[SLOTS];      // array with containing the free lists per slot
    // void            *alternative_free_list[SLOTS];  //DEBUG TODO: remove when in production
    unsigned char   slot_locked[SLOTS];     // flag indicating if the slot index is locked for migration
    unsigned int    total_blocks_num;  // all blocks of all slots
    unsigned long   bytes_size; // total allocated bytes for all slots. Includes headers sizes
    pthread_mutex_t mutexes[SLOTS]; // there is a mutex for each slot to synch slot_lock and writes
};

r_allocator_t r_allocator;

static int r_allocator_skip_lock_when_idle = 0;
static unsigned long long r_allocator_locks_taken = 0;
static unsigned long long r_allocator_locks_skipped = 0;

int r_allocator_set_skip_lock_when_idle(int enable) {
    int prev = r_allocator_skip_lock_when_idle;
    r_allocator_skip_lock_when_idle = enable ? 1 : 0;
    return prev;
}
unsigned long long r_allocator_get_locks_taken(void)   { return r_allocator_locks_taken; }
unsigned long long r_allocator_get_locks_skipped(void) { return r_allocator_locks_skipped; }


/* * * * * * * * * * * * * * * * * * * * * * * * * */
/* * * * * * * * FREE LIST HANDLERS * * * * * * * * */
/* * * * * * * * * * * * * * * * * * * * * * * * * */

/*
* internal use
* insert at the front of the list
* parameter-1: the slot that the segment belongs to
* parameter-2: pointer at the free segment *after* the segment header
* TODO: keep list sorted for best-fit insertions
*/
void freelist_insert_segment(int slot, void *segment)
{
    assert(segment != NULL);
    // printf("%s:%d %s() +++++++++++insert %u\n", __FILE__, __LINE__, __func__, GET_SIZE(HDRP(segment)));
    
    // void **free_list = &(r_allocator.free_list[slot]);
    // void *free_list = r_allocator.free_list[slot];

    if ( r_allocator.free_list[slot] ) {
        // printf("%s:%d %s() freelist not empty\n", __FILE__, __LINE__, __func__);
        /* previous free list start now needs prev ptr */
        PREV_FREEP(r_allocator.free_list[slot]) = segment; 
        /* set next free ptr to old start of list */
        NEXT_FREEP(segment) = r_allocator.free_list[slot]; 
    } else {
        // printf("%s:%d %s() freelist IS empty\n", __FILE__, __LINE__, __func__);
        NEXT_FREEP(segment) = NULL;
    }

    /* new first free block in list */
    PREV_FREEP(segment) = NULL; 
    /* reset start of free list */
    // *(void **)(free_list) = segment;
    r_allocator.free_list[slot] = segment;
    // printf(">>>>>>%p\n", r_allocator.free_list[slot]);
}

//DEBUG
// void freelist_insert_segment_debug(int slot, void *segment)
// {
//     // printf("%s:%d %s() +++++++++++insert %u\n", __FILE__, __LINE__, __func__, GET_SIZE(HDRP(segment)));
    
//     // void **free_list = &(r_allocator.free_list[slot]);
//     // void *free_list = r_allocator.free_list[slot];

//     if ( r_allocator.alternative_free_list[slot] ) {
//         // printf("%s:%d %s() freelist not empty\n", __FILE__, __LINE__, __func__);
//         /* previous free list start now needs prev ptr */
//         PREV_FREEP(r_allocator.alternative_free_list[slot]) = segment; 
//         /* set next free ptr to old start of list */
//         NEXT_FREEP(segment) = r_allocator.alternative_free_list[slot]; 
//     } else {
//         // printf("%s:%d %s() freelist IS empty\n", __FILE__, __LINE__, __func__);
//         NEXT_FREEP(segment) = NULL;
//     }

//     /* new first free block in list */
//     PREV_FREEP(segment) = NULL; 
//     /* reset start of free list */
//     // *(void **)(free_list) = segment;
//     r_allocator.alternative_free_list[slot] = segment;
//     // printf(">>>>>>%p\n", r_allocator.free_list[slot]);
// }

//internal use
// param-1: the slot the pointer belongs
// param-2: ptr on the segment after semgnet hdr i.e. the "next" field
void freelist_remove_segment(int slot, void *fp)
{
    // printf("%s:%d %s() --------remove %u\n", __FILE__, __LINE__, __func__, GET_SIZE(HDRP(fp)));
    // void *free_list = r_allocator.free_list[slot];

    void *prev = PREV_FREEP(fp); 
    void *next = NEXT_FREEP(fp);

    /* If there's a previous free block... */
    if (prev)  {
        NEXT_FREEP(PREV_FREEP(fp)) = NEXT_FREEP(fp); 
    } else { /* If not, next free block is the new start */
        // free_list = NEXT_FREEP(fp); 
        r_allocator.free_list[slot] = NEXT_FREEP(fp);
    }
    /* If there's a next free block... */
    if (next)  { 
        PREV_FREEP(NEXT_FREEP(fp)) = PREV_FREEP(fp);
    }
}

/* 
 * Internal use
 * find_fit - Find a fit for a block with asize bytes 
 */
void *freelist_find_fit(int slot, size_t asize)
{
    // printf("%s:%d %s() start\n", __FILE__, __LINE__, __func__);
    void *free_list = r_allocator.free_list[slot];
    void *fp = free_list;
    /* Search free list for big enough block */
    while (fp) { 
        if (asize <= GET_SIZE(HDRP(fp))) {
            return fp;
        }
        fp = NEXT_FREEP(fp);
    }

    // printf("%s:%d %s() end\n", __FILE__, __LINE__, __func__);
    return NULL; /* no fit */
}

// DEBUG
void freelist_print(int slot)
{
    // printf("%s:%d %s() start\n", __FILE__, __LINE__, __func__);
    void *fp = r_allocator.free_list[slot];
    /* Search free list for big enough block */
    printf("***********freelist (size,alloc): ");
    while (fp) { 
        printf("(%u,%u),", GET_SIZE(HDRP(fp)), GET_ALLOC(HDRP(fp)) );
        fp = NEXT_FREEP(fp);
    }
    printf("\n");
    // printf("%s:%d %s() end\n", __FILE__, __LINE__, __func__);
}

// void freelist_print_debug(int slot)
// {
//     // printf("%s:%d %s() start\n", __FILE__, __LINE__, __func__);
//     void *fp = r_allocator.alternative_free_list[slot];
//     /* Search free list for big enough block */
//     printf("^^^^^^^^^^^^alternative freelist (size,alloc): ");
//     while (fp) { 
//         printf("(%u,%u),", GET_SIZE(HDRP(fp)), GET_ALLOC(HDRP(fp)) );
//         fp = NEXT_FREEP(fp);
//     }
//     printf("\n");
//     // printf("%s:%d %s() end\n", __FILE__, __LINE__, __func__);
// }

/* * * * * * * * * * * * * * * * * * * * * * * * * */
/* * * * * * * * INTERNAL FUNCTIONS * * * * * *  * */
/* * * * * * * * * * * * * * * * * * * * * * * * * */

// Internal use
// Write prologue + initial free-segment header/footer + epilogue into the
// supplied chunk (must be at least WSIZE + BLOCK_SIZE_BYTES + WSIZE bytes),
// zero-init the alloc_bloc_t bookkeeping fields, and attach the chunk.
// Does NOT allocate either the bookkeeping struct or the chunk — caller does.
static void init_bloc_layout(alloc_bloc_t *new_block, char *block_start)
{
    new_block->block_start = block_start;

    // create the dummy/prologue header at the front of the block
    PUT(new_block->block_start, PACK(0, 1));   /* New prologue/dummy header */

    //put header and boundary footer to the new empty block
    /* Initialize free block header/footer and the epilogue header */
    void *fist_segment_ptr = new_block->block_start + WSIZE;
    PUT(fist_segment_ptr, PACK(BLOCK_SIZE_BYTES, 0));
    char *footer_start_ptr = fist_segment_ptr + BLOCK_SIZE_BYTES - WSIZE;
    PUT(footer_start_ptr, PACK(BLOCK_SIZE_BYTES, 0));
    void *dummy_hdr_ptr = fist_segment_ptr + BLOCK_SIZE_BYTES;    //dummy header is at the end of the "useful" block size
    // set the flags in the last dummy header as "used" with size 0
    // this indicates that we reached the end of the block
    PUT(dummy_hdr_ptr, PACK(0, 1));   /* New epilogue/dummy header */

    // Header/footer will be overwritten when allocator puts data into segments.
    // Only when segments are written we assume that header/footer is occupied and
    // maintain the corresponding counters
    new_block->bytes_free = BLOCK_SIZE_BYTES;
    new_block->bytes_total_in_use = 0;

    new_block->next = NULL;
    new_block->prev = NULL;

    //DEBUG
    new_block->segments_free = 0;
    new_block->segments_used = 0;
}

// Internal use
// Append a fully-initialized alloc_bloc_t to slot's block list (creating the
// list head if empty). Increments slot_blocks_num.
static void link_block_into_slot(int slot, alloc_bloc_t *new_blk)
{
    if (r_allocator.slot_blocks[slot] == NULL) {
        r_allocator.slot_blocks[slot] = new_blk;
        r_allocator.slot_blocks_tail[slot] = new_blk;
    } else {
        r_allocator.slot_blocks_tail[slot]->next = new_blk;
        new_blk->prev = r_allocator.slot_blocks_tail[slot];
        r_allocator.slot_blocks_tail[slot] = new_blk;
    }
    r_allocator.slot_blocks_num[slot]++;
}

// internal use
// returns a pointer at the beggining of the new block
// The caller is responsible to add the new empty block in the free list
alloc_bloc_t * allocate_new_bloc()
{
    // printf("%s:%d %s() start\n", __FILE__, __LINE__, __func__);
    alloc_bloc_t *new_block = (alloc_bloc_t *) zmalloc(sizeof(alloc_bloc_t));
    if (new_block == NULL) {
        fprintf(stderr, "%s:%d %s() allocation ERROR!\n", __FILE__, __LINE__, __func__);
        exit(1);
    }

    // each block has 2 extra hidden dummy header:
    // one at the end of the block used to indicate the end of the block and
    // one at the front of the block used when coalescing (the prev segment of the 1st segment is the prologue)
    // So allocate additional space for the dummy headers
    // The useful block size for the user payload is BLOCK_SIZE_BYTES
    char *block_start = (char *) zmalloc(WSIZE + BLOCK_SIZE_BYTES + WSIZE);
    if (block_start == NULL) {
        fprintf(stderr, "%s:%d %s() allocation ERROR!\n", __FILE__, __LINE__, __func__);
        exit(1);
    }
    // printf("%s:%d %s() new block @: %p buf: %p\n", __FILE__, __LINE__, __func__, new_block, block_start);

    init_bloc_layout(new_block, block_start);
    return new_block;
}


// Internal use
// moves pointer after its has written data
// param is double pointer because we want to move the pointer as we write data
// we want the pointer movement to be visible to the caller
// if we had simple pointer, the movement is not visible to the caller, so we woule move pointer at the caller
// returns the position (pointer) where header+data are written
void * add_data_with_header(char **segment, char *data, size_t size)
{
    // printf("%s:%d:%s() start\n", __FILE__, __LINE__, __func__);
    char *return_ptr = *(char **)(segment);

    // memcpy(*(char **)(segment), &size, sizeof(size_t));
    // *(char **)(segment) += sizeof(size);
    memcpy(*(char **)(segment), &size, ENTRY_HEADER_SIZE);
    *(char **)(segment) += ENTRY_HEADER_SIZE;

    memcpy(*(char **)segment, data, size);
    *(char **)(segment) += size;

    // printf("%s:%d:%s() bytes consumed: %lu\n", __FILE__, __LINE__, __func__, (*segment) - return_ptr);

    return return_ptr;
}

// Internal use
// moves pointer after its has written data
// param is double pointer because we want to move the pointer as we write data
// we want the pointer movement to be visible to the caller
// if we had simple pointer, the movement is not visible to the caller, so we woule move pointer at the caller
// returns the position (pointer) where data are written
void * add_data_no_header(char **segment, char *data, size_t size)
{
    // printf("%s:%d:%s() start\n", __FILE__, __LINE__, __func__);
    char *return_ptr = *(char **)(segment);

    memcpy(*(char **)segment, data, size);
    *(char **)(segment) += size;
    
    return return_ptr;
}

// Internal use
// parameter: pointer *after the header* of the segment that will be deleted.
void coalesce(int slot, void *segment)
{
    void *next_sgmt = NEXT_SEGMENT(segment);
    u_int next_allocated = GET_ALLOC(HDRP(next_sgmt));

    u_int prev_allocated = 1;
    void *prev_sgmt = PREV_SEGMENT(segment);
    if (prev_sgmt == segment) { //there is no prev segment
        prev_allocated = 1; //if no prev segment exists, we assume it is a used segment
    } else {
        prev_allocated = GET_ALLOC(HDRP(prev_sgmt));
    }

    size_t size = GET_SIZE(HDRP(segment));

    void *final_free_sgmt = NULL;

    if ( prev_allocated && next_allocated ){
        // printf("%s:%d %s() No coalesce!\n", __FILE__, __LINE__, __func__);
        final_free_sgmt = segment;
    } else if (prev_allocated && !next_allocated) {
        // printf("%s:%d %s() Coalesce with NEXT\n", __FILE__, __LINE__, __func__);
        final_free_sgmt = segment;
        size += GET_SIZE(HDRP(next_sgmt));
        freelist_remove_segment(slot, next_sgmt);
    } else if (!prev_allocated && next_allocated) {
        // printf("%s:%d %s() Coalesce with PREV\n", __FILE__, __LINE__, __func__);
        final_free_sgmt = prev_sgmt;
        size += GET_SIZE(HDRP(prev_sgmt));
        freelist_remove_segment(slot, prev_sgmt);
    } else {
        // printf("%s:%d %s() Coalesce with PREV + NEXT\n", __FILE__, __LINE__, __func__);
        size += GET_SIZE(HDRP(prev_sgmt)) + GET_SIZE(HDRP(next_sgmt));
        final_free_sgmt = prev_sgmt;
        freelist_remove_segment(slot, next_sgmt);
        freelist_remove_segment(slot, prev_sgmt);
    }

    PUT_WTAG(HDRP(final_free_sgmt), PACK(size, 0));
    PUT_WTAG(FTRP(final_free_sgmt), PACK(size, 0));

    // check if the whole block is empty
    // after coalescing (merging) continuous empty segments, check if the next/prev headers
    // are the dummy prologue/epilogue headers
    void *new_next_sgmt = NEXT_SEGMENT(final_free_sgmt);
    int is_next_epilogue = (GET_SIZE(HDRP(new_next_sgmt)) == 0) ? 1 : 0;
    void *new_prev_sgmt = PREV_SEGMENT(final_free_sgmt);
    int is_prev_prologue = 0;
    if ( new_prev_sgmt == final_free_sgmt ) {
        is_prev_prologue = 1;
    }

    // if the free segments is between dummy prologue/epilogue headers,
    // this means the whole block is empty. We can delete the block
    if (is_next_epilogue && is_prev_prologue) {
        // printf("%s:%d %s() ########> Delete whole block\n", __FILE__, __LINE__, __func__);
        alloc_bloc_t *blk = get_block_from_ptr(slot, final_free_sgmt);
        assert(blk != NULL);
        alloc_bloc_t *prev_blk = blk->prev;
        alloc_bloc_t *next_blk = blk->next;

        if (prev_blk != NULL) {
            prev_blk->next = next_blk;
        } else { // if prev is null, this is the fist block of the list. Update head
            //if blk was the only block of the list, then next=NULL --> slot block list will be null
            r_allocator.slot_blocks[slot] = next_blk;
        }

        if (next_blk != NULL) {
            next_blk->prev = prev_blk;
        } else { //if this is the last block in list, update tail pointer
            //if this is the only block in the list, then the prev_blk pointer
            // will be NULL. So the tail ptr will also be NULL
            r_allocator.slot_blocks_tail[slot] = prev_blk;
        }

        // delete block
        zfree(blk->block_start);
        zfree(blk);

        //update block counter for the block
        r_allocator.slot_blocks_num[slot] -= 1;
    } else {
        // if we don't delete the whole block, then we have to add the free segment in the free list
        freelist_insert_segment(slot, final_free_sgmt);
    }
}

/* * * * * * * * * * * * * * * * * * * * * * * * * */
/* * * * * * *  PUBLIC API FUNCTIONS   * * * * * * */
/* * * * * * * * * * * * * * * * * * * * * * * * * */

void r_allocator_init()
{
    // printf("%s:%d %s() START\n", __FILE__, __LINE__, __func__);
    //util_reset_logs();

    for (int i = 0; i < SLOTS; ++i) {
        r_allocator.slot_blocks[i] = NULL; 
        r_allocator.slot_blocks_tail[i] = NULL;
        r_allocator.slot_blocks_num[i] = 0;
        r_allocator.free_list[i] = NULL;
        // r_allocator.alternative_free_list[i] = NULL;
        r_allocator.slot_locked[i] = 0;
        pthread_mutex_init(&r_allocator.mutexes[i], NULL);
    }
    // printf("%s:%d %s() spot 2\n", __FILE__, __LINE__, __func__);
    // r_allocator.total_blocks_num     = 0;
    r_allocator.bytes_size    = 0;
    // printf("%s:%d %s() END\n", __FILE__, __LINE__, __func__);
}

/* 
* Allocates a new empty block for the given slot. 
* If slot already has block, it will add the new block at the front of the block list
* Does not affect the free list of the slot (does not add the new empty block to the free list)
* The new block (including data buffer and metadata fields) is appended in the slot's block list in the allocator
* returns a pointer at the data buffer of the block (EXCLUDING block metadata)
*/
void * r_allocator_alloc_new_empty_block(int slot)
{
    // struct timeval start, end;
    // gettimeofday(&start, NULL);

    alloc_bloc_t *new_blk = allocate_new_bloc();
    link_block_into_slot(slot, new_blk);

    // gettimeofday(&end, NULL);
    // long seconds = (end.tv_sec - start.tv_sec);
    // long micros = ((seconds * 1000000) + end.tv_usec) - (start.tv_usec);
    // duration_alloc_empty_block += micros;

    // calls_alloc_empty_blocks++;

    return new_blk->block_start;
}

/* Aqueduct PREP fast-path: the caller (recipient REGISTER-BLOCK-SLOTS
 * worker) has already allocated a contiguous mmap'd pool and registered it
 * with one ibv_reg_mr. It hands us a stride-sized sub-pointer per slot;
 * we initialize the prologue/epilogue/header/footer in place and thread the
 * block into the slot's list. The allocator does NOT take ownership of
 * block_ptr — the pool's lifetime is owned by the caller (today the pool
 * is leaked alongside the existing per-block leak at cluster_rdma.c). */
void * r_allocator_register_existing_block(int slot, void *block_ptr)
{
    if (block_ptr == NULL) return NULL;

    alloc_bloc_t *new_block = (alloc_bloc_t *) zmalloc(sizeof(alloc_bloc_t));
    if (new_block == NULL) {
        fprintf(stderr, "%s:%d %s() bookkeeping zmalloc failed\n", __FILE__, __LINE__, __func__);
        return NULL;
    }
    init_bloc_layout(new_block, (char *) block_ptr);
    link_block_into_slot(slot, new_block);
    return block_ptr;
}

size_t r_allocator_block_stride_bytes(void)
{
    return (size_t) WSIZE + (size_t) BLOCK_SIZE_BYTES + (size_t) WSIZE;
}

// public API
/* returns pointer at the header of the segment
* creates a new segment in a block that has enough space for the segment
* segment layout: <seg header> <payload> <padding> <seg footer>
* seg header/footer: 4bytes that contain the size of the segment and a flag (lowest bit) indicating if segment is free/used
* padding: is used to align segments at 8bytes addresses (so last 2 bits are always 0. We use these bits as flags)
* payload layout: <key meta> <value meta> <key size> <key> <value size> <value>
*/
void * r_allocator_insert_kv(int slot, 
                            void *key, size_t key_size, 
                            void *value, size_t value_size,
                            robj *key_meta, size_t key_meta_size,
                            robj *value_meta, size_t value_meta_size,
                            int *allocated_new_block,
                            robj **ptr_key_meta,
                            robj **ptr_val_meta)
{
    //TODO: optimize: instead of ENTRY_HEADER_SIZE use WSIZE to write the size of each item in the segment
    // size_t total_data_size = key_size + value_size + key_meta_size + value_meta_size + (4*ENTRY_HEADER_SIZE);
    size_t total_data_size = key_size + value_size + key_meta_size + value_meta_size + (2*ENTRY_HEADER_SIZE);
    size_t final_size_aligned = MAX( ALIGN(total_data_size) + OVERHEAD, MIN_SEGMENT_SIZE );
    //////
    // size_t padding_size = final_size_aligned - (total_data_size + 2*WSIZE);
    // printf("\n\n%s:%d %s() key=%s ks %zu, vs %zu, kms: %zu, vms %zu, 2*header: %u, total data size %zu, HDR+FTR: %u aligned size: %zu padding: %zu\n", 
    //         __FILE__, __LINE__, __func__, (char*) key, key_size, value_size, key_meta_size, 
    //         value_meta_size, (2*ENTRY_HEADER_SIZE), total_data_size, (2*WSIZE), final_size_aligned, padding_size);
    ///////
    assert(BLOCK_SIZE_BYTES >= final_size_aligned && "Reguested K-V size larger than allocator block size");

    *allocated_new_block = 0;

    // Acquire the lock for the slot. We ensure that write will be complete when we
    // release the lock and lock_slot can acquire it.
    // Optimization: when r_allocator_skip_lock_when_idle is set and the slot is not
    // currently locked-for-migration, skip the mutex. UNSAFE for concurrent intra-slot
    // writers (free-list corruption possible) — only use when intra-slot collisions are
    // statistically rare (e.g. YCSB across 16384 slots).
    /* === LOCKED PHASE: reserve a segment ===
     * Critical work: freelist scan, block allocation, freelist insert/remove,
     * header/footer marking. These touch shared state (free_list, block list).
     * Once we mark the header as "allocated", the segment is uniquely ours
     * and other inserters will skip it — payload memcpy can run unlocked. */
    int needs_lock = !r_allocator_skip_lock_when_idle || r_allocator.slot_locked[slot];
    if (needs_lock) {
        pthread_mutex_lock(&r_allocator.mutexes[slot]);
        r_allocator_locks_taken++;
    } else {
        r_allocator_locks_skipped++;
    }

    char * free_segment = freelist_find_fit(slot, final_size_aligned);
    if (!free_segment) {
        char * block_data_buffer = r_allocator_alloc_new_empty_block(slot);
        *allocated_new_block = 1;
        void *first_segment_ptr = block_data_buffer + WSIZE;
        freelist_insert_segment(slot, (first_segment_ptr + WSIZE));
        free_segment = (first_segment_ptr + WSIZE);
    }

    freelist_remove_segment(slot, free_segment);

    size_t free_segment_size = GET_SIZE(HDRP(free_segment));
    u_int dif = free_segment_size - final_size_aligned;

    /* If enough space: split and put the remainder back on the free list. */
    if (dif >= MIN_SEGMENT_SIZE) {
        PUT(HDRP(free_segment), PACK(final_size_aligned, 1));
        PUT(FTRP(free_segment), PACK(final_size_aligned, 1));

        char *next_segment = NEXT_SEGMENT(free_segment);
        PUT_WTAG(HDRP(next_segment), PACK(dif, 0));
        PUT_WTAG(FTRP(next_segment), PACK(dif, 0));

        freelist_insert_segment(slot, next_segment);
    } else {
        PUT_WTAG(HDRP(free_segment), PACK(free_segment_size, 1));
        PUT_WTAG(FTRP(free_segment), PACK(free_segment_size, 1));
    }

    if (needs_lock) {
        pthread_mutex_unlock(&r_allocator.mutexes[slot]);
    }

    /* === UNLOCKED PHASE: payload write ===
     * The segment is now allocator-owned by this thread (header marked in-use,
     * removed from free list). Other threads' freelist_find_fit cannot return it.
     * The four memcpys below are the bulk of the per-insert wall-clock cost
     * (~1 KB total for typical YCSB values) — they don't need the lock. */
    void *return_ptr = HDRP(free_segment);

    key_meta->data_offset = key_meta_size + value_meta_size + ENTRY_HEADER_SIZE;
    (*ptr_key_meta) = add_data_no_header(&free_segment, (void *) key_meta, key_meta_size);

    value_meta->data_offset = value_meta_size + ENTRY_HEADER_SIZE + key_size + ENTRY_HEADER_SIZE;
    (*ptr_val_meta) = add_data_no_header(&free_segment, (void *) value_meta, value_meta_size);

    add_data_with_header(&free_segment, key, key_size);
    add_data_with_header(&free_segment, value, value_size);

    /* Atomic: bytes_size is read by stats; don't need the per-slot mutex for it. */
    __atomic_add_fetch(&r_allocator.bytes_size, final_size_aligned, __ATOMIC_RELAXED);

    return return_ptr;
}

/* public API
 * Insert a kvobj-shaped payload (single robj header with embedded key and
 * embedded value sds, mirroring kvobjCreateEmbedString's jemalloc layout) into
 * an r_allocator segment for `slot`. Returns the kvobj pointer, suitable for
 * direct insertion into the keyspace dict.
 *
 * Segment payload layout:
 *   [robj kvobj header 24B] [1B key-hdr-size] [sds key hdr+bytes+\0] [sds value hdr+bytes+\0]
 *
 * The returned kvobj has:
 *   - type        = OBJ_STRING
 *   - encoding    = OBJ_ENCODING_R_ALLOCATOR (free path routes to r_allocator_free_kv)
 *   - iskvobj     = 1, metabits = 0 (no expire/module metadata in this commit)
 *   - refcount    = 1, lru = 0
 *   - data_offset = R_ALLOC_PACK_DATA_OFFSET(slot, offset-to-value-sds-bytes)
 *   - ptr         = value sds bytes (sds-shaped; sdslen() / sdsfree-equivalents
 *                   need only the [-1] flag byte the embedded sds header writes)
 *
 * The lock-then-unlock-then-payload pattern from r_allocator_insert_kv is
 * preserved: the segment is reserved under the slot mutex and the payload write
 * runs unlocked once the header is marked in-use. */
kvobj * r_allocator_insert_kvobj(int slot, sds key, sds value, int *allocated_new_block)
{
    size_t key_len = sdslen(key);
    size_t val_len = sdslen(value);

    /* AqRaft Patch 23: defensive validation. The follower has been seen
     * crashing in this function after applying a raft-replicated SET — if
     * the raft entry deserialization produced an sds with a corrupt length
     * header, the math below overflows and writes through a garbage pointer.
     * Reject anything implausible up front and log it. */
    if (key_len == 0 || key_len > (1ULL << 28) ||
        val_len == 0 || val_len > (1ULL << 28) ||
        slot < 0 || slot >= SLOTS) {
        RMIG_LOG(RDMAMIG_LOG_WARNING,
            "r_alloc_insert REFUSED: slot=%d key_len=%zu val_len=%zu (suspicious — likely "
            "corrupt sds header on raft-replicated SET)",
            slot, key_len, val_len);
        return NULL;
    }

    char key_sds_type = sdsReqType(key_len);
    size_t key_sds_size = sdsReqSize(key_len, key_sds_type);
    /* Mirror kvobjCreateEmbedString: value sds is always SDS_TYPE_8. */
    size_t val_sds_size = sdsReqSize(val_len, SDS_TYPE_8);

    size_t payload_size = sizeof(robj) + 1 + key_sds_size + val_sds_size;
    size_t final_size_aligned = MAX(ALIGN(payload_size) + OVERHEAD, MIN_SEGMENT_SIZE);
    assert(BLOCK_SIZE_BYTES >= final_size_aligned && "kvobj segment exceeds block size");

    *allocated_new_block = 0;

    /* LOCKED PHASE — same pattern as r_allocator_insert_kv. */
    pthread_mutex_lock(&r_allocator.mutexes[slot]);

    char *free_segment = freelist_find_fit(slot, final_size_aligned);
    if (!free_segment) {
        char *block_data_buffer = r_allocator_alloc_new_empty_block(slot);
        *allocated_new_block = 1;
        void *first_segment_ptr = block_data_buffer + WSIZE;
        freelist_insert_segment(slot, (first_segment_ptr + WSIZE));
        free_segment = (first_segment_ptr + WSIZE);
    }
    freelist_remove_segment(slot, free_segment);

    size_t free_segment_size = GET_SIZE(HDRP(free_segment));
    u_int dif = free_segment_size - final_size_aligned;
    if (dif >= MIN_SEGMENT_SIZE) {
        PUT(HDRP(free_segment), PACK(final_size_aligned, 1));
        PUT(FTRP(free_segment), PACK(final_size_aligned, 1));
        char *next_segment = NEXT_SEGMENT(free_segment);
        PUT_WTAG(HDRP(next_segment), PACK(dif, 0));
        PUT_WTAG(FTRP(next_segment), PACK(dif, 0));
        freelist_insert_segment(slot, next_segment);
    } else {
        PUT_WTAG(HDRP(free_segment), PACK(free_segment_size, 1));
        PUT_WTAG(FTRP(free_segment), PACK(free_segment_size, 1));
    }

    pthread_mutex_unlock(&r_allocator.mutexes[slot]);

    /* UNLOCKED PHASE — payload write into the now-owned segment. */
    kvobj *kv = (kvobj *) free_segment;
    /* OBJ_STRING == 0; defined in server.h, not visible to this sub-library.
     * Hardcoded to avoid pulling server.h into the rdma_migration layer. */
    kv->type = 0 /* OBJ_STRING */;
    kv->encoding = OBJ_ENCODING_R_ALLOCATOR;
    kv->refcount = 1;
    kv->iskvobj = 1;
    kv->metabits = 0;
    kv->lru = 0;

    char *data = (char *)(kv + 1);
    *data++ = sdsHdrSize(key_sds_type);
    sdsnewplacement(data, key_sds_size, key_sds_type, (const char *)key, key_len);
    data += key_sds_size;

    sds val_sds = sdsnewplacement(data, val_sds_size, SDS_TYPE_8, (const char *)value, val_len);
    kv->ptr = val_sds;

    /* val_sds points at the sds bytes (past the sds header). Offset from kv. */
    unsigned value_offset = (unsigned)((char *)val_sds - (char *)kv);
    assert(value_offset <= R_ALLOC_OFFSET_MASK && "r_allocator kvobj value offset overflow");
    kv->data_offset = R_ALLOC_PACK_DATA_OFFSET(slot, value_offset);

    __atomic_add_fetch(&r_allocator.bytes_size, final_size_aligned, __ATOMIC_RELAXED);
    return kv;
}

/* public API
 * Given an r_allocator-backed robj/kvobj, return the pointer to pass to
 * r_allocator_free_kv (start of the segment payload, right after the seg
 * header). For kvobjs with metabits=0 this is the kvobj address itself;
 * with future metadata prefixes it walks back via kvobjGetAllocPtr. */
void * r_allocator_seg_for_robj(robj *o) {
    if (o->iskvobj) return kvobjGetAllocPtr((kvobj *) o);
    return o;
}

// public API
/*
* returns the head of the block list of the slot
* this is the list of the whole blocks not just the internal buffers of the blocks
*/
alloc_bloc_t * r_allocator_get_block_list_for_slot(int slot)
{
    return (r_allocator.slot_blocks[slot]);
}

/* public API
 * Walks every USED segment in `block_start` (init_bloc_layout layout: prologue
 * at offset 0, headers from WSIZE on, terminated by a size=0 epilogue). Calls
 * `cb` with each used segment's payload pointer and payload size. */
void r_allocator_walk_used_segments(
    char *block_start,
    void (*cb)(void *seg_payload, size_t seg_payload_size, void *user),
    void *user)
{
    if (block_start == NULL || cb == NULL) return;
    void *seg = block_start + WSIZE + WSIZE;
    /* Hard cap to prevent runaway in case of corrupted size headers (free-
     * segment metadata layout differs subtly between r_allocator versions
     * and the recipient's freelist state is stale post-RDMA-WRITE). */
    char *block_end = block_start + BLOCK_SIZE_BYTES;
    int iters = 0;
    while ((char *) seg < block_end && GET_SIZE(HDRP(seg)) != 0) {
        unsigned seg_full = GET_SIZE(HDRP(seg));
        if (seg_full < MIN_SEGMENT_SIZE) break;  /* corrupt size — bail */
        if (GET_ALLOC(HDRP(seg))) {
            size_t payload = (size_t) seg_full - 2 * WSIZE;
            cb(seg, payload, user);
        }
        seg = NEXT_SEGMENT(seg);
        if (++iters > 100000) break;  /* hard cap */
    }
}

/* AqRaft Patch 17: sanitize a donor-shipped block.
 *
 * Donor-shipped 2 MiB blocks contain segments that look free (alloc-bit = 0)
 * to the recipient, but the inline freelist pointers (PREV_FREEP/NEXT_FREEP
 * stored in free-segment payloads) refer to the DONOR's address space. When
 * the recipient later runs coalesce on a SET-overwrite of a migrated key,
 * coalesce checks the neighbor's alloc bit; if it reads 0 (free), it calls
 * freelist_remove_segment(slot, neighbor) which dereferences the donor's
 * PREV_FREEP/NEXT_FREEP -> SIGSEGV in coalesce+0x1d0.
 *
 * Walk every segment in this block once and flip alloc-bit to 1 on header +
 * footer for any segment whose alloc-bit is currently 0. After sanitize all
 * neighbors appear "allocated" so coalesce skips the merge and never derefs
 * the stale donor freelist pointers. Idempotent. Same iteration scheme as
 * r_allocator_walk_used_segments (hard-bounded by block_end + 100K iteration
 * cap to survive corrupted sizes). Trade-off: the recipient never reclaims
 * the donor's free space in this block — but free space in migrated blocks
 * is already abandoned via r_allocator_reset_freelist_for_slot, so no
 * behavior change. */
void r_allocator_sanitize_imported_block(char *block_start)
{
    if (block_start == NULL) return;
    void *seg = block_start + WSIZE + WSIZE;     /* first segment payload */
    char *block_end = block_start + BLOCK_SIZE_BYTES;
    /* The actual allocation extends WSIZE past block_end (the epilogue
     * header, see init_bloc_layout). FTRP(seg) for the initial whole-block
     * free segment lives at offset BLOCK_SIZE_BYTES, and writing 4 bytes
     * there ends at BLOCK_SIZE_BYTES + WSIZE — still inside the allocation.
     * Use block_alloc_end as the strict bound for writes. */
    char *block_alloc_end = block_start + BLOCK_SIZE_BYTES + WSIZE;
    int iters = 0;
    while ((char *) seg < block_end && GET_SIZE(HDRP(seg)) != 0) {
        unsigned size = GET_SIZE(HDRP(seg));
        /* Sanity: keep walker's existing checks. Sizes are 8-byte aligned
         * by ALIGN in r_allocator_insert_kvobj, so a non-aligned size is
         * almost certainly garbage. */
        if (size < MIN_SEGMENT_SIZE) break;
        if (size % ALIGNMENT != 0) break;
        if (size > (unsigned) BLOCK_SIZE_BYTES) break;
        char *ftr = (char *) seg + size - DSIZE;  /* FTRP(seg) */
        /* Only write if BOTH header and footer fit inside the allocation.
         * (HDRP(seg) = (char*)seg - WSIZE is always within block_alloc_end
         * because seg < block_end.) */
        if (ftr < block_start || ftr + WSIZE > block_alloc_end) break;
        if (!GET_ALLOC(HDRP(seg))) {
            PUT_WTAG(HDRP(seg), PACK(size, 1));
            PUT_WTAG(ftr, PACK(size, 1));
        }
        seg = NEXT_SEGMENT(seg);
        if (++iters > 100000) break;              /* hard cap */
    }
}

/* public API
 * Reset slot's freelist head to NULL. Called after RDMA-receiving a donor
 * block so future r_allocator_insert_kvobj allocations don't try to use stale
 * free-list pointers into the now-occupied block. */
void r_allocator_reset_freelist_for_slot(int slot)
{
    if (slot < 0 || slot >= SLOTS) return;
    pthread_mutex_lock(&r_allocator.mutexes[slot]);
    r_allocator.free_list[slot] = NULL;
    pthread_mutex_unlock(&r_allocator.mutexes[slot]);
}

/* public API
* parm number_of_results is populated with the number of the slot blocks (the size of array the function returs)
* return a pointer to an array that contains pointers to the block buffers of the slot
*/
char ** r_allocator_get_block_buffers_for_slot(int slot, int *number_of_results)
{
    assert(number_of_results != NULL);

    // int block_num = r_allocator.slot_blocks_num[slot];
    *(number_of_results) = r_allocator.slot_blocks_num[slot];
    // char **block_buffers = (char **) zmalloc(block_num * sizeof(alloc_bloc_t *));
    char **block_buffers = (char **) zmalloc((*number_of_results) * sizeof(alloc_bloc_t *));

    alloc_bloc_t *curr = r_allocator.slot_blocks[slot];

    int index = 0;
    while (curr != NULL) {
        assert(curr->block_start != NULL);  //TODO: remove for production
        block_buffers[index++] = curr->block_start;
        curr = curr->next;
    }

    return block_buffers;
}

/* Public API
* Locks the current allocated blocks for the slot 
* i.e., no more writes are applied in the free segments of the existing blocks
* Future writes will be performed in newly allocated blocks
* Slot mutex guarantees that there are no active writes when we reset the free list
*/
void r_allocator_lock_slot_blocks(int slot) 
{
    // Acquire lock so no writes will be performed while we reset the free list
    pthread_mutex_lock(&r_allocator.mutexes[slot]);
    // 1. lock slot = mark as being transfered
    r_allocator.slot_locked[slot] = 1;

    // 2. reset free list for block
    r_allocator.free_list[slot] = NULL;

    pthread_mutex_unlock(&r_allocator.mutexes[slot]);
}

// public API
/*
* free the segment of the whole K-V
* parameter: pointer to the key metadata
*/
void r_allocator_free_kv(int slot, void *key_meta_ptr)
{
    // the layout of the segment is:
    // <segment header> <key meta> <value meta> <key size> <key> <value size> <value>
    // OR (for r_allocator-backed kvobjs): <seg header> <kvobj robj> <key-hdr-size 1B> <embedded key sds> <embedded value sds>
    // In both cases, key_meta_ptr is the pointer right AFTER the segment header.
    void *segment = key_meta_ptr;

    /* Lock the slot for the duration of mark-free + coalesce: both mutate
     * shared freelist state. Same mutex used by r_allocator_insert_kv, so
     * concurrent insert+free on the same slot serialise correctly. Required
     * for bio lazyfree thread and eviction to call us safely. */
    pthread_mutex_lock(&r_allocator.mutexes[slot]);

    // if segment already marked as free, do nothing. Return with a warning msg
    if (GET_ALLOC(HDRP(segment)) == 0) {
        fprintf(stderr, "%s:%d %s() WARN: trying to free already free memory segment\n", __FILE__, __LINE__, __func__);
        pthread_mutex_unlock(&r_allocator.mutexes[slot]);
        return;
    }

    /* AqRaft Patch 25: foreign-segment guard. On the recipient (and its
     * chain followers) migrated kvobjs are installed directly out of the
     * RDMA landing pool / donor-shipped block, NOT out of an r_allocator-
     * managed block. Those kvobjs still carry encoding=OBJ_ENCODING_R_ALLOCATOR,
     * so a client SET that overwrites a migrated key routes the old value
     * here. If we proceed, coalesce() inserts the landing-pool segment into
     * this slot's freelist (allocator.c:535) — or, if it mis-detects the
     * block as empty, tries to zfree() the landing pool (allocator.c:528).
     * Either way a later r_allocator_insert_kvobj picks up the foreign
     * segment via freelist_find_fit and SIGSEGVs on its stale pointers.
     *
     * Guard: if the segment isn't inside any block this allocator owns for
     * the slot, just orphan it. The landing pool's lifetime is owned by the
     * chain session, not by r_allocator, so there is nothing to free here. */
    if (get_block_from_ptr(slot, segment) == NULL) {
        pthread_mutex_unlock(&r_allocator.mutexes[slot]);
        return;
    }

    size_t size = GET_SIZE(HDRP(segment));

    PUT_WTAG(HDRP(segment), PACK(size, 0));
    PUT_WTAG(FTRP(segment), PACK(size, 0));

    //if slot is not locked, then coalsce, else not
    if (r_allocator.slot_locked[slot] == 0) {
        coalesce(slot, segment);
    }

    __atomic_sub_fetch(&r_allocator.bytes_size, size, __ATOMIC_RELAXED);

    pthread_mutex_unlock(&r_allocator.mutexes[slot]);
}

// public API
/*
* free all blocks of slot
*/
void free_slot(int slot)
{
    alloc_bloc_t *slot_blocklist = r_allocator.slot_blocks[slot];
    while (slot_blocklist != NULL) {
        alloc_bloc_t *del = slot_blocklist;
        slot_blocklist = slot_blocklist->next;
        // printf("%s:%d %s() block @ %p \tblock start @ %p\n", __FILE__, __LINE__, __func__, del, del->block_start);
        zfree(del->block_start);
        // printf("%s:%d %s() while spot 2\n", __FILE__, __LINE__, __func__);
        zfree(del);
    }
    r_allocator.slot_blocks[slot] = NULL;

    //reset free list
    r_allocator.free_list[slot] = NULL;
    // printf("%s:%d %s() END\n", __FILE__, __LINE__, __func__);
}

void r_allocator_free_world()
{
    // printf("%s:%d %s() FREE WORLD\n", __FILE__, __LINE__, __func__);
    for (int i = 0; i < SLOTS; ++i) {
        alloc_bloc_t *block_list = r_allocator.slot_blocks[i];
        if (block_list == NULL) {
            continue;
        }

        while (block_list != NULL) {
            zfree(block_list->block_start);
            alloc_bloc_t *tmp = block_list;
            block_list = block_list->next;
            zfree(tmp);
        }
    }
    // printf("\n==== FREE END ====\n");
}


/* * * * * * * * * * * * * * * * * * * * * * * * * */
/* * * * * * * * DEBUG FUNCTIONS * * * * * * * * * */
/* * * * * * * * * * * * * * * * * * * * * * * * * */
void print_full_kv_segment_filename(void *ptr, FILE *file)
{
    fprintf(file, "(%u)", GET_SIZE(HDRP(ptr)));
    fprintf(file, "|");

    fprintf(file, "%zu,", KEY_META_SIZE);
    ptr += KEY_META_SIZE;

    fprintf(file, "%zu,", VAL_META_SIZE);
    ptr += VAL_META_SIZE;

    size_t key_size = 0;
    memcpy(&key_size, ptr, ENTRY_HEADER_SIZE);
    fprintf(file, "%zu:", key_size);
    ptr += ENTRY_HEADER_SIZE; 
    char *key = (char *) zmalloc(key_size + 1);
    if (!key) {
        perror("Failed to allocate memory for key");
        return;
    }
    memcpy(key, ptr, key_size);
    key[key_size] = '\0';
    fprintf(file, "%s,", key);
    ptr += key_size;
    zfree(key);

    size_t val_size = 0;
    memcpy(&val_size, ptr, ENTRY_HEADER_SIZE);
    fprintf(file, "%zu:", val_size);
    ptr += ENTRY_HEADER_SIZE; 
    char *val = (char *) zmalloc(val_size + 1);
    if (!val) {
        perror("Failed to allocate memory for value");
        return;
    }
    memcpy(val, ptr, val_size);
    val[val_size] = '\0';
    fprintf(file, "%s", val);
    ptr += val_size;
    zfree(val);

    fprintf(file, "|");
}


// print segment that contains K-V
void print_full_kv_segment(void *ptr)
{
    //ptr points to: <key meta> <value meta> <key size (ENTRY_HAEDER_SIZE)> <key> <value size (ENTRY_HAEDER_SIZE)> <value>
    printf("(%u)", GET_SIZE(HDRP(ptr)));
    printf("|");

    // KEY META
    // robj *key_meta = (robj *) zmalloc(meta_object_size);
    // memcpy(key_meta, ptr, meta_object_size);
    // printf("%d,", key_meta->data_offset);
    printf("%zu,", KEY_META_SIZE);
    ptr += KEY_META_SIZE;
    // zfree(key_meta);

    // VALUE META
    // robj *value_meta = (robj *) zmalloc(meta_object_size);
    // memcpy(value_meta, ptr, meta_object_size);
    // printf("%d,", value_meta->data_offset);
    printf("%zu,", VAL_META_SIZE);
    ptr += VAL_META_SIZE;
    // zfree(value_meta);
    
    // KEY
    size_t key_size = 0;
    memcpy(&key_size, ptr, ENTRY_HEADER_SIZE);
    printf("%zu:", key_size);
    ptr += ENTRY_HEADER_SIZE; 
    char *key = (char *) zmalloc(key_size+1);
    memcpy(key, ptr, key_size);
    key[key_size] = '\0';
    printf("%s,", key);
    ptr += key_size;
    zfree(key);

    //VALUE
    size_t val_size = 0;
    memcpy(&val_size, ptr, ENTRY_HEADER_SIZE);
    printf("%zu:", val_size);
    ptr += ENTRY_HEADER_SIZE; 
    char *val = (char *) zmalloc(val_size+1);
    memcpy(val, ptr, val_size);
    val[val_size] = '\0';
    printf("%s", val);
    ptr += val_size;
    zfree(val);

    printf("|");
}

void print_empty_kv_segment_filename(void *ptr, FILE *file)
{
    // Implementation for printing empty KV segments
    // For now, let's assume it just prints a placeholder text
    fprintf(file, "Empty KV Segment");
}

void print_empty_kv_segment(void *ptr)
{
    size_t segment_size = GET_SIZE(HDRP(ptr));
    printf("| EMPTY SEG of %zu bytes|", segment_size);
}


// DEBUG. Given a pointer and the slot id get the block that contains the pointer
//TODO: disable this in production
alloc_bloc_t * get_block_from_ptr(int slot, void *ptr)
{
    alloc_bloc_t *cur_blk = r_allocator.slot_blocks[slot];

    while (cur_blk) {
        void *blk_start = cur_blk->block_start;
        void *blk_end = blk_start + BLOCK_SIZE_BYTES;
        if ( (ptr >= blk_start) && (ptr <= blk_end) ){
            return cur_blk;
        }
        cur_blk = cur_blk->next;
    }

    return NULL;
}

// DEBUG


void traverse_print_slot_blocks_filename(int slot, const char *filename)
{
    size_t total_used_bytes = 0;
    unsigned long long total_segments = 0;
    size_t slots_bytes_free = 0;
    FILE *file = fopen(filename, "a");
    if (!file) {
        perror("Failed to open file");
        return;
    }

    fprintf(file, "%s:%d %s() DEBUG print blocks for slot %d\n", __FILE__, __LINE__, __func__, slot);
    alloc_bloc_t *slot_head = r_allocator.slot_blocks[slot];
    if (slot_head == NULL) {
        fprintf(file, "%s:%d %s() slot %d has no allocated block\n", __FILE__, __LINE__, __func__, slot);
        fclose(file);
        return;
    }

    alloc_bloc_t *cur_block = slot_head;
    while (cur_block != NULL) {


        char *ptr = WSIZE + cur_block->block_start + WSIZE;
        while (GET_SIZE(HDRP(ptr))) {
            if (GET_ALLOC(HDRP(ptr))) {
		total_used_bytes += 1112;
		total_segments += 1;
                // print_full_kv_segment_filename(ptr, file);
            } else {
                // print_empty_kv_segment_filename(ptr, file);
            }
            ptr = NEXT_SEGMENT(ptr);
            // fprintf(file, "\n");
        }
	// fprintf(file, "Bytes (u:%zu/f:%zu) Segments (u:%zu/f:%zu)\n", 
 //                cur_block->bytes_total_in_use, cur_block->bytes_free,
 //                cur_block->segments_used, cur_block->bytes_free);
	slots_bytes_free = 3984588;
        cur_block = cur_block->next;
    }
    float total_blocks = (float)total_used_bytes / slots_bytes_free;
    // Calculate the fractional part of the last block
    float fractional_part = total_blocks - (size_t)total_blocks;

    // Calculate the remaining free bytes in the last block
    size_t remaining_free_bytes = (size_t)((1.0f - fractional_part) * slots_bytes_free);

    fprintf(file, "Total Blocks:%f, Total Segments:%zu, Last Slot unused bytes:%zu. Actual bytes free per block:%zu, total bytes of slot:%zu\n", total_blocks, total_segments, remaining_free_bytes, slots_bytes_free, total_used_bytes);
    fprintf(file, "\n");

    fclose(file);
}


void traverse_print_slot_blocks(int slot)
{
    printf("%s:%d %s() DEBUG print blocks for slot %d\n", __FILE__, __LINE__, __func__, slot);
    alloc_bloc_t *slot_head = r_allocator.slot_blocks[slot];
    if (slot_head == NULL) {
        printf("%s:%d %s() slot %d has no allocated block\n", __FILE__, __LINE__, __func__, slot);
        return;
    }

    alloc_bloc_t *cur_block = slot_head;
    while (cur_block != NULL) {
        printf("Bytes (u:%zu/f:%zu) Segments (u:%zu/f:%zu)\n", 
                cur_block->bytes_total_in_use, cur_block->bytes_free,
                cur_block->segments_used, cur_block->bytes_free);
        // the first WSIZE bytes is the prologue/dummy header. We add more WSIZE to go over the header of the 1st useful segment
        char *ptr = WSIZE + cur_block->block_start + WSIZE;
        while ( GET_SIZE(HDRP(ptr)) ) {
            if ( GET_ALLOC(HDRP(ptr)) ) {
                print_full_kv_segment(ptr);
            } else {
                print_empty_kv_segment(ptr);
            }
            ptr = NEXT_SEGMENT(ptr);
            printf("\n");
        }
        cur_block = cur_block->next;
        printf("\n");
    }
}

//DEBUG
// param: pointer at the start of the block buffer
void print_block_buffer(void *buffer_start)
{
    char *ptr = WSIZE + buffer_start + WSIZE;
    while ( GET_SIZE(HDRP(ptr)) ) {
        if ( GET_ALLOC(HDRP(ptr)) ) {
            print_full_kv_segment(ptr);
        } else {
            print_empty_kv_segment(ptr);
        }
        ptr = NEXT_SEGMENT(ptr);
        printf("\n");
    }
}

// get the starting position of the buffer of a block
void * get_buffer_from_block(void *blk)
{
    return ((alloc_bloc_t *)blk)->block_start;
}

// DEBUG
// scans block and updates used/free bytes of block
// param: pointer to the block (not just its internal data buffer)
void update_block_stats(alloc_bloc_t *blk)
{
    //reset counters before scan (returns actual stats even
    // in case of multiple calls)
    blk->bytes_total_in_use = 0;
    blk->bytes_free         = 0;
    blk->segments_used      = 0;
    blk->segments_free      = 0;

    char *ptr = WSIZE + blk->block_start + WSIZE;
    while ( GET_SIZE(HDRP(ptr)) ) {
        if ( GET_ALLOC(HDRP(ptr)) ) {
            blk->bytes_total_in_use += GET_SIZE(HDRP(ptr));
            blk->segments_used++;
        } else {
            blk->bytes_free += GET_SIZE(HDRP(ptr));
            blk->segments_free++;
        }
        ptr = NEXT_SEGMENT(ptr);
    }
}

//DEBUG
// scan free list and return the number of free segments
// across all blocks of the slot
size_t free_list_size(int slot)
{
    // printf("%s:%d %s() start\n", __FILE__, __LINE__, __func__);
    size_t free_segments = 0;
    void *free_list = r_allocator.free_list[slot];
    void *fp = free_list;
    /* Search free list for big enough block */
   while (fp) { 
       free_segments++;
        fp = NEXT_FREEP(fp);
    }

    // printf("%s:%d %s() end\n", __FILE__, __LINE__, __func__);
    return free_segments;
}

// DEBUG
// iterate though all blocks and update block stats (bytes used/free)
// while gathering cummulative stats fot the slot
slot_stats_t update_slot_stats(int slot) 
{
    // printf("%s:%d %s() DEBUG update block stats for slot %d\n", __FILE__, __LINE__, __func__, slot);
    slot_stats_t stats = {
        .slot_id = slot,
        .blocks = 0,
        .bytes_free = 0,
        .bytes_used =0,
        .segments_used = 0,
        .segments_free = 0,
        .freelist_len = 0
    };
    alloc_bloc_t *slot_head = r_allocator.slot_blocks[slot];
    if (slot_head == NULL) {
        // printf("%s:%d %s() slot %d has no allocated block\n", __FILE__, __LINE__, __func__, slot);
        return stats;
    }

    alloc_bloc_t *cur_block = slot_head;
    while (cur_block != NULL) {
        update_block_stats(cur_block);

        stats.blocks += 1;
        stats.bytes_used += cur_block->bytes_total_in_use;
        stats.bytes_free += cur_block->bytes_free;
        stats.segments_used += cur_block->segments_used;
        stats.segments_free += cur_block->segments_free;

        cur_block = cur_block->next;
    }

    stats.freelist_len = free_list_size(slot);
    return stats;
}

void print_slot_stats(slot_stats_t slot_stats)
{
    printf("Stats for slot: %d:\n \
	blocks: \t%d\n \
	bytes used: \t%d\n \
	bytes free: \t%d\n \
    segments used: \t%d\n \
    segments free: \t%d\n \
	free list len: \t%d\n",
    slot_stats.slot_id,
	slot_stats.blocks,
	slot_stats.bytes_used,
	slot_stats.bytes_free,
    slot_stats.segments_used,
    slot_stats.segments_free,
	slot_stats.freelist_len);
}

/* public API
 * Emit one redis-log line summarising a slot's allocator stats (blocks,
 * used / free segments and bytes, freelist length). Reuses
 * update_slot_stats() so the figures stay consistent with the printf
 * variant above. Not called from any production path; provided for
 * ad-hoc inspection. */
void r_allocator_log_slot_stats(int slot)
{
    slot_stats_t s = update_slot_stats(slot);
    RMIG_LOG(RDMAMIG_LOG_NOTICE,
        "r_allocator slot=%u blocks=%u segs_used=%u segs_free=%u "
        "bytes_used=%u bytes_free=%u freelist_len=%u",
        s.slot_id, s.blocks, s.segments_used, s.segments_free,
        s.bytes_used, s.bytes_free, s.freelist_len);
}

/* public API
 * Emit one stats line for every slot that currently has at least one block
 * allocated. Skips empty slots so a fresh-cluster snapshot has zero output.
 * Ends with a summary line giving the count of populated slots. */
void r_allocator_log_all_slot_stats(void)
{
    int populated = 0;
    for (int slot = 0; slot < SLOTS; slot++) {
        if (r_allocator.slot_blocks[slot] != NULL) {
            r_allocator_log_slot_stats(slot);
            populated++;
        }
    }
    RMIG_LOG(RDMAMIG_LOG_NOTICE,
        "r_allocator all-slots summary: populated=%d (of %d)",
        populated, SLOTS);
}

/* public API
 * Walk a slot's blocks; for each allocated segment, treat it as a kvobj
 * (the OBJ_ENCODING_R_ALLOCATOR layout produced by r_allocator_insert_kvobj)
 * and emit one redis-log line per key plus a final total. Caller must
 * ensure the slot is quiescent — no internal locking. */
void r_allocator_log_slot_keys(int slot)
{
    alloc_bloc_t *blk = r_allocator.slot_blocks[slot];
    unsigned long long counted = 0;
    while (blk != NULL) {
        /* First payload byte: skip the block's prologue dummy word plus the
         * first segment's header word — same offset as create_iterator_for_slot. */
        char *seg = (char *)blk->block_start + WSIZE + WSIZE;
        while (GET_SIZE(HDRP(seg)) > 0) { /* epilogue terminates the walk */
            if (GET_ALLOC(HDRP(seg))) {
                kvobj *kv = (kvobj *) seg;
                sds k = kvobjGetKey(kv);
                size_t klen = sdslen(k);
                size_t vlen = (kv->ptr != NULL) ? sdslen((sds) kv->ptr) : 0;
                RMIG_LOG(RDMAMIG_LOG_NOTICE,
                    "r_allocator slot=%d key='%.*s' key_len=%zu val_len=%zu enc=%u",
                    slot, (int) klen, k, klen, vlen, kv->encoding);
                counted++;
            }
            seg = NEXT_SEGMENT(seg);
        }
        blk = blk->next;
    }
    RMIG_LOG(RDMAMIG_LOG_NOTICE,
        "r_allocator slot=%d total_keys=%llu", slot, counted);
}



// TESTING
// give a KV, return the required segment space for the KV
// including key, value, metadata and alignment padding
size_t calculate_required_space_for_segment( 
                            size_t key_size, 
                            size_t value_size,
                            size_t key_meta_size,
                            size_t value_meta_size
                            ) 
{
    size_t total_data_size = key_size + value_size + key_meta_size + value_meta_size + (2*ENTRY_HEADER_SIZE);
    size_t final_size_aligned = MAX( ALIGN(total_data_size) + OVERHEAD, MIN_SEGMENT_SIZE );

    return final_size_aligned;
}

// TESTING seriliaze and write to file slot blocks
// return num of bytes written (0 in case there are no allocated blocks for slot) 
// or -1 in case of error
int dump_slot(int slot, char *fileName)
{
    alloc_bloc_t *slot_head = r_allocator.slot_blocks[slot];
    if (slot_head == NULL) {
        printf("%s:%d %s() slot %d has no allocated block\n", __FILE__, __LINE__, __func__, slot);
        return 0;
    }

    FILE *data_file = fopen(fileName, "wb");
    if (data_file == NULL) {
        fprintf(stderr, "ERROR opening file %s for writing data for slot %d\n", fileName, slot);
        return -1;
    }

    int bytes_written = 0;
    alloc_bloc_t *cur_block = slot_head;
    while (cur_block != NULL) {
        // printf("%s:%d %s() write block", __FILE__, __LINE__, __func__);
        bytes_written += fwrite(cur_block->block_start, 1, BLOCK_SIZE_BYTES, data_file);
        cur_block = cur_block->next;
        // printf(" (total written: %d)\n", bytes_written);
    }
    
    fclose(data_file);
    return bytes_written;
}

// TESTING load data from file
// return the num of bytes read
int load_slot_from_file(int slot, char *fileName)
{
    FILE *f = fopen(fileName, "rb");
    if (f == NULL) {
        fprintf(stderr, "ERROR opening file %s for reading data for slot %d\n", fileName, slot);
        return -1;
    }

    int bytes_read = 0;
    char *tmp_block = (char *) zmalloc(BLOCK_SIZE_BYTES);
    while (fread(tmp_block, 1, BLOCK_SIZE_BYTES, f) == BLOCK_SIZE_BYTES) {
        bytes_read += BLOCK_SIZE_BYTES;
        // printf("%s:%d %s() read block (total bytes read: %d)\n", __FILE__, __LINE__, __func__, bytes_read);
        char *block_start = r_allocator_alloc_new_empty_block(slot);
        memcpy(block_start, tmp_block, BLOCK_SIZE_BYTES);
    }

    fclose(f);
    zfree(tmp_block);
    return bytes_read;
}

/* * * * * * * * * * * * * * * * * * * * * * * * * */
/* * * * * * * * * *  ITERATOR  * * * * * * * * * */
/* * * * * * * * * * * * * * * * * * * * * * * * * */

#define MAX_SLOTS 19000

segment_iterator_t *iterators_per_slot[MAX_SLOTS] = {NULL};

//TODO: init iterator in r_allocator_init
//segment_iterator_t *iter = NULL;



static segment_iterator_t *get_iterator(int slot) {
    if (slot < 0 || slot >= MAX_SLOTS) {
        fprintf(stderr, "%s:%d %s() ERROR! Invalid slot number: %d\n", __FILE__, __LINE__, __func__, slot);
        exit(-1);
    }
    return iterators_per_slot[slot];
}
//will appear as public API through the struct field that points to this function
// return ptr at the segment after its header or NULL if no KV left for the slot
void * _getNext(int slot, robj **key_meta, robj **value_meta)
{
    segment_iterator_t *iter = get_iterator(slot);
    if (iter == NULL) {
        fprintf(stderr, "%s:%d %s() ERROR! iterator has not been initialized\n", __FILE__, __LINE__, __func__);
        exit(-1);
    }
    if (iter->cur_block == NULL) {
        return NULL;
    }

    while (iter->cur_block != NULL) {
        while ( GET_SIZE(HDRP(iter->cur_segment)) ) {   //if size of segment is 0, we reached the end of the block (the dummy/epilogue header)
            // printf("\n%s:%d %s() current segment ptr: %p\n", __FILE__, __LINE__, __func__, iter->cur_segment);
            void *return_segment = NULL;
            if ( GET_ALLOC(HDRP(iter->cur_segment)) ) {
                // print_full_kv_segment(iter->cur_segment);
                // printf("\n");
                return_segment = iter->cur_segment;
                *key_meta = (iter->cur_segment);
                *value_meta = (iter->cur_segment) + KEY_META_SIZE;
                // printf("\n%s:%d %s() km ptr: %p  vm ptr: %p\n", __FILE__, __LINE__, __func__, (*key_meta), (*value_meta));
                //TODO: update used/free bytes
                iter->cur_block->bytes_total_in_use += GET_SIZE(HDRP(iter->cur_segment));
                iter->cur_block->bytes_free -= GET_SIZE(HDRP(iter->cur_segment));
            } else {
                freelist_insert_segment(slot, iter->cur_segment);
            }
            iter->cur_segment = NEXT_SEGMENT(iter->cur_segment);
            // if we visit an non-empty segment, return_segment is non-null. stop moving and return this segment
            if (return_segment != NULL) {
                return return_segment;
            }
        }
        // printf("\n");
        iter->cur_block = iter->cur_block->next;
        if (iter->cur_block == NULL) {
            return NULL;
        }
        //if we move to the next block, set the current segment ot the 1st segment of the current block
        // the first WSIZE bytes is the prologue/dummy header. We add more WSIZE to go over the header of the 1st useful segment
        iter->cur_segment = WSIZE + iter->cur_block->block_start + WSIZE;
    }

	return NULL;
}

segment_iterator_t * create_iterator_for_slot(int slot)
{
	// segment_iterator_t *iter = (segment_iterator_t *)zmalloc(sizeof(segment_iterator_t));
//    if (iter == NULL) {
//        iter = (segment_iterator_t *)zmalloc(sizeof(segment_iterator_t));
//    }

  if (iterators_per_slot[slot] == NULL) {
        iterators_per_slot[slot] = (segment_iterator_t *)zmalloc(sizeof(segment_iterator_t));
  }
  segment_iterator_t *iter = iterators_per_slot[slot];
	iter->cur_block = r_allocator_get_block_list_for_slot(slot);
	if (iter->cur_block == NULL) {
		iter->cur_segment = NULL;
	} else {
        // the first WSIZE bytes is the prologue/dummy header. We add more WSIZE to go over the header of the 1st useful segment
		iter->cur_segment = WSIZE + ( (iter->cur_block)->block_start) + WSIZE; 
	}
	iter->getNext = _getNext;
	return iter;
}

#pragma GCC diagnostic pop
