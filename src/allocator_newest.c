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

#include "server.h"
#include "allocator.h"


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

#define SLOTS 16384
#define ENTRY_HEADER_SIZE WSIZE //this is 4 bytes used to store the size of the sub-fields in the segment
#define KEY_META_SIZE sizeof(robj)
#define VAL_META_SIZE sizeof(robj)

struct allocated_block {
    size_t  bytes_total_in_use;
    size_t  bytes_free;
    char    *block_start;
    struct allocated_block *next;
    struct allocated_block *prev;
};

struct r_allocator {
    alloc_bloc_t    *slot_blocks[SLOTS];    // array with lists of blocks. A list per slot
    unsigned int    slot_blocks_num[SLOTS]; // number of blocks per slot
    void            *free_list[SLOTS];      // array with containing the free lists per slot
    // void            *alternative_free_list[SLOTS];  //DEBUG TODO: remove when in production
    unsigned char   slot_locked[SLOTS];     // flag indicating if the slot index is locked for migration
    unsigned int    total_blocks_num;  // all blocks of all slots
    unsigned long   bytes_size; // total allocated bytes for all slots. Includes headers sizes
};

r_allocator_t r_allocator;

//DEBUG
long duration_alloc_empty_block = 0;
unsigned int calls_alloc_empty_blocks = 0;

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

// internal use
// returns a pointer at the beggining of the new block
// The caller is responsible to add the new empty block in the free list
alloc_bloc_t * allocate_new_bloc()
{
    // printf("%s:%d %s() start\n", __FILE__, __LINE__, __func__);
    alloc_bloc_t *new_block = (alloc_bloc_t *) malloc(sizeof(alloc_bloc_t));
    if (new_block == NULL) {
        fprintf(stderr, "%s:%d %s() allocation ERROR!\n", __FILE__, __LINE__, __func__);
        exit(1);
    }

    // each block has 2 extra hidden dummy header:
    // one at the end of the block used to indicate the end of the block and
    // one at the front of the block used when coalescing (the prev segment of the 1st segment is the prologue)
    // So allocate additional space for the dummy headers
    // The useful block size for the user payload is BLOCK_SIZE_BYTES
    new_block->block_start = (char *) malloc(WSIZE + BLOCK_SIZE_BYTES + WSIZE);
    if (new_block->block_start == NULL) {
        fprintf(stderr, "%s:%d %s() allocation ERROR!\n", __FILE__, __LINE__, __func__);
        exit(1);
    }
    // printf("%s:%d %s() new block @: %p buf: %p\n", __FILE__, __LINE__, __func__, new_block, new_block->block_start);    

    // create the dummy/prologue header at the front of the block
    PUT(new_block->block_start, PACK(0, 1));   /* New prologue/dummy header */ 

    //put header and boundary footer to the new empty block
    /* Initialize free block header/footer and the epilogue hceader */
    void *fist_segment_ptr = new_block->block_start + WSIZE;
    PUT(fist_segment_ptr, PACK(BLOCK_SIZE_BYTES, 0));
    char *footer_start_ptr = fist_segment_ptr + BLOCK_SIZE_BYTES - WSIZE;
    PUT(footer_start_ptr, PACK(BLOCK_SIZE_BYTES, 0));
    // printf("%s:%d %s() header size %u footer size %u\n", __FILE__, __LINE__, __func__, GET_SIZE(new_block->block_start), GET_SIZE(footer_start_ptr));
    void *dummy_hdr_ptr = fist_segment_ptr + BLOCK_SIZE_BYTES;    //dummy header is at the end of the "useful" block size
    // set the flags in the last dummy header as "used" with size 0
    // this indicates that we reached the end of the block
    PUT(dummy_hdr_ptr, PACK(0, 1));   /* New epilogue/dummy header */
    // printf("%s:%d %s() dummy header %p\n", __FILE__, __LINE__, __func__, dummy_hdr_ptr);

    // Header/footer will be overwritten when allocator puts data into segments. 
    // Only when segments are written we assume that header/footer is occupied and
    // maintain the corresponding counters
    new_block->bytes_free = BLOCK_SIZE_BYTES;
    new_block->bytes_total_in_use = 0;

    new_block->next = NULL;
    new_block->prev = NULL;

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
        }

        // delete block
        free(blk->block_start);
        free(blk);

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
        r_allocator.slot_blocks_num[i] = 0;
        r_allocator.free_list[i] = NULL;
        // r_allocator.alternative_free_list[i] = NULL;
        r_allocator.slot_locked[i] = 0;
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
* returns a pointer at the beginning of the newly allocated block (including the block buffer and the block metadata)
*/
void * r_allocator_alloc_new_empty_block(int slot)
{
    // struct timeval start, end;
    // gettimeofday(&start, NULL);

    alloc_bloc_t *new_blk = allocate_new_bloc();
    if (r_allocator.slot_blocks[slot] != NULL) {
        new_blk->next = r_allocator.slot_blocks[slot];
        r_allocator.slot_blocks[slot]->prev = new_blk;
    }

    r_allocator.slot_blocks[slot] = new_blk;

    // r_allocator.total_blocks_num++;
    r_allocator.slot_blocks_num[slot]++;

    // gettimeofday(&end, NULL);
    // long seconds = (end.tv_sec - start.tv_sec);
    // long micros = ((seconds * 1000000) + end.tv_usec) - (start.tv_usec);
    // duration_alloc_empty_block += micros;

    // calls_alloc_empty_blocks++;

    return new_blk->block_start;
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
    // size_t padding_size = final_size_aligned - (total_data_size + 2*WSIZE);
    // printf("\n\n%s:%d %s() key=%s ks %zu, vs %zu, kms: %zu, vms %zu, 2*header: %u, total data size %zu, HDR+FTR: %u aligned size: %zu padding: %zu\n", 
    //         __FILE__, __LINE__, __func__, (char*) key, key_size, value_size, key_meta_size, 
    //         value_meta_size, (2*ENTRY_HEADER_SIZE), total_data_size, (2*WSIZE), final_size_aligned, padding_size);
    assert(BLOCK_SIZE_BYTES >= final_size_aligned && "Reguested K-V size larger than allocator block size");

    *allocated_new_block = 0;

    char * free_segment = freelist_find_fit(slot, final_size_aligned);
    if (!free_segment) {
        // printf("%s:%d %s() not found any free entry to fit\n", __FILE__, __LINE__, __func__);
        alloc_bloc_t *new_blk = allocate_new_bloc();
        // if slot already has allocated block, the new block will be the first 
        // and its next points to the rest of the list
        if (r_allocator.slot_blocks[slot] != NULL) {
            new_blk->next = r_allocator.slot_blocks[slot];
            r_allocator.slot_blocks[slot]->prev = new_blk;
        }
        r_allocator.slot_blocks[slot] = new_blk;
        // r_allocator.total_blocks_num += 1;
        r_allocator.slot_blocks_num[slot]++;
        *allocated_new_block = 1;
        void *first_segment_ptr = new_blk->block_start + WSIZE; // the first WSIZE bytes is the prologue/dummy header
        // freelist_insert_segment(slot, (new_blk->block_start + WSIZE) );
        // free_segment = (new_blk->block_start + WSIZE);
        freelist_insert_segment(slot, (first_segment_ptr + WSIZE) );
        free_segment = (first_segment_ptr + WSIZE);
    }
    
    freelist_remove_segment(slot, free_segment);

    // get free segment header
    size_t free_segment_size = GET_SIZE(HDRP(free_segment));
    u_int dif = free_segment_size - final_size_aligned;
    // if we won't split the free segment then bytes used will be equal to the size of the free segment
    u_int bytes_used = final_size_aligned;  

    // printf("%s:%d %s() free_seg_size: %zu diff: %u  segment@: %p\n", __FILE__, __LINE__, __func__, free_segment_size, dif, free_segment);

    /* If enough space: change header & footer, split, then coalesce */
    if (dif >= MIN_SEGMENT_SIZE) { 
        /* Setup allocated block (header and footer) */
        PUT(HDRP(free_segment), PACK(final_size_aligned, 1));
        PUT(FTRP(free_segment), PACK(final_size_aligned, 1)); 

        /* add new empty block (the remaining space of the segment) */
        char *next_segment = NEXT_SEGMENT(free_segment);
        /* Enough space for another free block, so setup new free block */
        PUT_WTAG(HDRP(next_segment), PACK(dif, 0));
        PUT_WTAG(FTRP(next_segment), PACK(dif, 0));

        freelist_insert_segment(slot, next_segment);
    } else { /* If remaining space isn't enough, don't split free block */
        PUT_WTAG(HDRP(free_segment), PACK(free_segment_size, 1));
        PUT_WTAG(FTRP(free_segment), PACK(free_segment_size, 1)); 
        bytes_used = GET_SIZE(HDRP(free_segment));
    }

    //DEBUG
    // char *next_segment = NEXT_SEGMENT(free_segment);
    // printf("%s:%d %s() free_segment: %p  next seg: %p\n",  __FILE__, __LINE__, __func__, free_segment, next_segment);
    // printf("%s:%d %s() free_segment header: size = %u alloc: %u\n\t\t\t\t\t\t\t  footer: size = %u aloc: %u\n",
    //             __FILE__, __LINE__, __func__, 
    //             GET_SIZE(HDRP(free_segment)), GET_ALLOC(FTRP(free_segment)),   // header
    //             GET_SIZE(FTRP(free_segment)), GET_ALLOC(HDRP(free_segment)));  // footer

    // printf("%s:%d %s() next_segment header: size = %u alloc: %u\n\t\t\t\t\t\t\t  footer: size = %u aloc: %u\n",
    //         __FILE__, __LINE__, __func__, 
    //         GET_SIZE(HDRP(next_segment)), GET_ALLOC(FTRP(next_segment)),   // haeder
    //         GET_SIZE(FTRP(next_segment)), GET_ALLOC(HDRP(next_segment)));  // footer

    //DEBUG find the corresponding block and update used bytes counter
    alloc_bloc_t *blk = get_block_from_ptr(slot, free_segment);
    assert(blk != NULL);
    blk->bytes_total_in_use += GET_SIZE(HDRP(free_segment));
    blk->bytes_free -= GET_SIZE(HDRP(free_segment));

    void *return_ptr = HDRP(free_segment);

    // write <key meta size> (without header)
    // modify the offset in key metadata to the actual key
    // key_meta->data_offset = key_meta_size + ENTRY_HEADER_SIZE;
    key_meta->data_offset = key_meta_size + value_meta_size + ENTRY_HEADER_SIZE;
    (*ptr_key_meta) = add_data_no_header(&free_segment, (void *) key_meta, key_meta_size);    

    // modify the offset back to actual value in the value metadata obj
    // value_meta->data_offset = value_meta_size + ENTRY_HEADER_SIZE;
    value_meta->data_offset = value_meta_size + ENTRY_HEADER_SIZE + key_size + ENTRY_HEADER_SIZE;
    // write <value meta> (without header)
    (*ptr_val_meta) = add_data_no_header(&free_segment, (void *) value_meta, value_meta_size);
   
    // write <key size> <key>
    add_data_with_header(&free_segment, key, key_size);

    // write <value size> <value>
    add_data_with_header(&free_segment, value, value_size);

    r_allocator.bytes_size += final_size_aligned;

    return return_ptr;
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
* parm number_of_results is populated with the number of the slot blocks (the size of array the function returs)
* return a pointer to an array that contains pointers to the block buffers of the slot
*/
char ** r_allocator_get_block_buffers_for_slot(int slot, int *number_of_results)
{
    assert(number_of_results != NULL);

    // int block_num = r_allocator.slot_blocks_num[slot];
    *(number_of_results) = r_allocator.slot_blocks_num[slot];
    // char **block_buffers = (char **) malloc(block_num * sizeof(alloc_bloc_t *));
    char **block_buffers = (char **) malloc((*number_of_results) * sizeof(alloc_bloc_t *));

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
*/
void r_allocator_lock_slot_blocks(int slot) 
{
    // 1. lock slot = mark as being transfered
    r_allocator.slot_locked[slot] = 1;

    // 2. reset free list for block
    r_allocator.free_list[slot] = NULL;
}


void r_allocator_print_stats_for_slot(int slot)
{
    // int blocks_num = r_allocator.total_blocks_num;
    size_t slot_blocks_num = r_allocator.slot_blocks_num[slot];
    alloc_bloc_t *cur_block = r_allocator.slot_blocks[slot];
    
    serverLog(LL_WARNING, "+++++++Slot %d has %zu block: \n", slot, slot_blocks_num);
    // LOG_PRINT("Slot %d has %zu block: ", slot, slot_blocks_num);
    while (cur_block != NULL) {
        // the first WSIZE bytes is the prologue/dummy header. We add more WSIZE to go over the header of the 1st useful segment
        char *cur_segment = WSIZE + cur_block->block_start + WSIZE;
        size_t cur_block_kv_segments_cnt = 0;
        size_t cur_block_free_segments_cnt = 0;
        while ( GET_SIZE(HDRP(cur_segment)) ) {   //if size of segment is 0, we reached the end of the block (the dummy/epilogue header)

            if ( GET_ALLOC(HDRP(cur_segment)) ) {
                cur_block_kv_segments_cnt++;
            } else {
                cur_block_free_segments_cnt++;
            }
            cur_segment = NEXT_SEGMENT(cur_segment);
        }
        serverLog(LL_WARNING, "|Su:%zu, Sf:%zu, Bu:%zu, Bf:%zu|, \n", 
                     cur_block_kv_segments_cnt, cur_block_free_segments_cnt, 
                     cur_block->bytes_total_in_use, cur_block->bytes_free);
        serverLog(LL_WARNING, "|Su:%zu, Sf:%zu, Bu:%zu, Bf:%zu|, ", 
                     cur_block_kv_segments_cnt, cur_block_free_segments_cnt, 
                     cur_block->bytes_total_in_use, cur_block->bytes_free);
        cur_block = cur_block->next;
    }
    // printf("\n");
    // LOG_PRINT_RAW("\n");
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
    // we have to add to the free list the pointer after the segment header
    void *segment = key_meta_ptr;

    // if segment already marked as free, do nothing. Return with a warning msg
    if (GET_ALLOC(HDRP(segment)) == 0) {
        fprintf(stderr, "%s:%d %s() WARN: trying to free already free memory segment\n", __FILE__, __LINE__, __func__);
        return;
    }

    //DEBUG fing the corresponding block and update used bytes counters
    //TODO: remove for production
    size_t size = GET_SIZE(HDRP(segment));
    alloc_bloc_t *blk = get_block_from_ptr(slot, key_meta_ptr);
    assert(blk != NULL);
    blk->bytes_total_in_use -= size;
    blk->bytes_free         += size;

    // mark this segment as free and then try to coalesce
    // if we use the old pointer and try to delete the already deleted key in this segment
    // we can detect the double free action (see a few lines above)
    PUT_WTAG(HDRP(segment), PACK(size, 0));
    PUT_WTAG(FTRP(segment), PACK(size, 0));
    
    // char *del_key = key_meta_ptr + KEY_META_SIZE + VALUE_META_SIZE + ENTRY_HEADER_SIZE;
    // printf("%s:%d %s() delete: %s\n", __FILE__, __LINE__, __func__, del_key);

    //if slot is not locked, then coalsce, else not
    if (r_allocator.slot_locked[slot] == 0) {
        coalesce(slot, segment);
    }
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
        free(del->block_start);
        // printf("%s:%d %s() while spot 2\n", __FILE__, __LINE__, __func__);
        free(del);
    }
    r_allocator.slot_blocks[slot] = NULL;
    printf("%s:%d %s() END\n", __FILE__, __LINE__, __func__);
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
            free(block_list->block_start);
            alloc_bloc_t *tmp = block_list;
            block_list = block_list->next;
            free(tmp);
        }
    }
    // printf("\n==== FREE END ====\n");
}


/* * * * * * * * * * * * * * * * * * * * * * * * * */
/* * * * * * * * DEBUG FUNCTIONS * * * * * * * * * */
/* * * * * * * * * * * * * * * * * * * * * * * * * */

long get_empty_blocks_alloc_exec_time() 
{
    //LOG_PRINT("Total execution time of allocate_new_empty_blocks: %ld micros for %u calls\n", duration_alloc_empty_block, calls_alloc_empty_blocks);
    return duration_alloc_empty_block;
}


// print segment that contains K-V
void print_full_kv_segment(void *ptr)
{
    //ptr points to: <key meta> <value meta> <key size (ENTRY_HAEDER_SIZE)> <key> <value size (ENTRY_HAEDER_SIZE)> <value>
    serverLog(LL_WARNING, "(%u)", GET_SIZE(HDRP(ptr)));
    serverLog(LL_WARNING, "|");

    // KEY META
    // robj *key_meta = (robj *) malloc(meta_object_size);
    // memcpy(key_meta, ptr, meta_object_size);
    // printf("%d,", key_meta->data_offset);
    serverLog(LL_WARNING, "%zu,", KEY_META_SIZE);
    ptr += KEY_META_SIZE;
    // free(key_meta);

    // VALUE META
    // robj *value_meta = (robj *) malloc(meta_object_size);
    // memcpy(value_meta, ptr, meta_object_size);
    // printf("%d,", value_meta->data_offset);
    serverLog(LL_WARNING, "%zu,", VAL_META_SIZE);
    ptr += VAL_META_SIZE;
    // free(value_meta);
    
    // KEY
    size_t key_size = 0;
    memcpy(&key_size, ptr, ENTRY_HEADER_SIZE);
    serverLog(LL_WARNING, "%zu:", key_size);
    ptr += ENTRY_HEADER_SIZE; 
    char *key = (char *) malloc(key_size+1);
    memcpy(key, ptr, key_size);
    key[key_size] = '\0';
    serverLog(LL_WARNING, "%s,", key);
    ptr += key_size;
    free(key);

    //VALUE
    size_t val_size = 0;
    memcpy(&val_size, ptr, ENTRY_HEADER_SIZE);
    serverLog(LL_WARNING, "%zu:", val_size);
    ptr += ENTRY_HEADER_SIZE; 
    char *val = (char *) malloc(val_size+1);
    memcpy(val, ptr, val_size);
    val[val_size] = '\0';
    serverLog(LL_WARNING, "%s", val);
    ptr += val_size;
    free(val);

    serverLog(LL_WARNING, "|");
}

void print_empty_kv_segment(void *ptr)
{
    size_t segment_size = GET_SIZE(HDRP(ptr));
    serverLog(LL_WARNING, "| EMPTY SEG of %zu bytes|", segment_size);
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
void traverse_print_slot_blocks(int slot)
{
    serverLog(LL_WARNING, "%s:%d %s() DEBUG print blocks for slot %d\n", __FILE__, __LINE__, __func__, slot);
    alloc_bloc_t *slot_head = r_allocator.slot_blocks[slot];
    if (slot_head == NULL) {
        serverLog(LL_WARNING, "%s:%d %s() slot %d has no allocated block\n", __FILE__, __LINE__, __func__, slot);
        return;
    }

    alloc_bloc_t *cur_block = slot_head;
    while (cur_block != NULL) {
        serverLog(LL_WARNING, "(u:%zu/f:%zu) \n", cur_block->bytes_total_in_use, cur_block->bytes_free);
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

// TESTING
unsigned int r_allocator_get_slot_num_blocks(int slot) 
{
    return r_allocator.slot_blocks_num[slot];
}

// TESTING
slot_stats_t collect_stats_for_slot(int slot) 
{
    // printf("%s:%d %s() DEBUG print blocks for slot %d\n", __FILE__, __LINE__, __func__, slot);
    slot_stats_t stats = {
        .blocks = 0,
        .bytes_free = 0,
        .bytes_used =0
    };
    alloc_bloc_t *slot_head = r_allocator.slot_blocks[slot];
    if (slot_head == NULL) {
        // printf("%s:%d %s() slot %d has no allocated block\n", __FILE__, __LINE__, __func__, slot);
        return stats;
    }

    alloc_bloc_t *cur_block = slot_head;
    while (cur_block != NULL) {
        // printf("(u:%zu/f:%zu) \n", cur_block->bytes_total_in_use, cur_block->bytes_free);
        stats.blocks += 1;
        stats.bytes_used += cur_block->bytes_total_in_use;
        stats.bytes_free += cur_block->bytes_free;
        // the first WSIZE bytes is the prologue/dummy header. We add more WSIZE to go over the header of the 1st useful segment
        char *ptr = WSIZE + cur_block->block_start + WSIZE;
        while ( GET_SIZE(HDRP(ptr)) ) {
            ptr = NEXT_SEGMENT(ptr);
        }
        cur_block = cur_block->next;
    }
    return stats;
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

/* * * * * * * * * * * * * * * * * * * * * * * * * */
/* * * * * * * * * *  ITERATOR  * * * * * * * * * */
/* * * * * * * * * * * * * * * * * * * * * * * * * */

//TODO: init iterator in r_allocator_init
segment_iterator_t *iter = NULL;

//will appear as public API through the struct field that points to this function
// return ptr at the segment after its header or NULL if no KV left for the slot
void * _getNext(int slot, robj **key_meta, robj **value_meta)
{
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
                //print_full_kv_segment(iter->cur_segment);
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
        printf("\n");
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
	// segment_iterator_t *iter = (segment_iterator_t *)malloc(sizeof(segment_iterator_t));
    if (iter == NULL) {
        iter = (segment_iterator_t *)malloc(sizeof(segment_iterator_t));
    }
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
