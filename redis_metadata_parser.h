/**
 * Redis Metadata Buffer Parser - C Header
 * 
 * This library provides functions to parse Redis response buffers that contain
 * both metadata and data when the read-response-metadata feature is enabled.
 */

#ifndef REDIS_METADATA_PARSER_H
#define REDIS_METADATA_PARSER_H

#include <stdint.h>
#include <stddef.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Data type constants */
#define REDIS_TYPE_STRING 0
#define REDIS_TYPE_HASH   1
#define REDIS_TYPE_LIST   2
#define REDIS_TYPE_SET    3
#define REDIS_TYPE_ZSET   4
#define REDIS_TYPE_STREAM 5

/* Parse result codes */
typedef enum {
    REDIS_PARSE_OK = 0,
    REDIS_PARSE_NO_METADATA,
    REDIS_PARSE_INVALID_ARGS,
    REDIS_PARSE_BUFFER_TOO_SMALL,
    REDIS_PARSE_BUFFER_TRUNCATED,
    REDIS_PARSE_SIZE_MISMATCH,
    REDIS_PARSE_MEMORY_ERROR
} redis_parse_result_t;

/* Metadata structure */
typedef struct {
    char *key;                    /* Redis key name (null-terminated) */
    char *field;                  /* Hash field name (null-terminated, or NULL for non-hash) */
    uint8_t data_type;           /* Data type (REDIS_TYPE_*) */
    uint32_t data_size;          /* Size of actual data */
    uint64_t access_timestamp;   /* Unix timestamp of access */
    uint64_t keyspace_hits;      /* Server keyspace hits */
    uint64_t keyspace_misses;    /* Server keyspace misses */
    uint32_t total_size;         /* Total buffer size */
    uint32_t metadata_size;      /* Metadata section size */
} redis_metadata_t;

/* Complete response structure */
typedef struct {
    redis_metadata_t metadata;   /* Metadata information */
    uint8_t *data;               /* Actual data bytes */
    size_t data_size;            /* Size of data */
} redis_response_t;

/* Function declarations */

/**
 * Initialize a metadata structure
 * @param metadata Pointer to metadata structure
 */
void redis_metadata_init(redis_metadata_t *metadata);

/**
 * Free resources in a metadata structure
 * @param metadata Pointer to metadata structure
 */
void redis_metadata_free(redis_metadata_t *metadata);

/**
 * Initialize a response structure
 * @param response Pointer to response structure
 */
void redis_response_init(redis_response_t *response);

/**
 * Free resources in a response structure
 * @param response Pointer to response structure
 */
void redis_response_free(redis_response_t *response);

/**
 * Get human-readable data type name
 * @param data_type Data type constant
 * @return String representation of data type
 */
const char* redis_data_type_name(uint8_t data_type);

/**
 * Parse a Redis metadata buffer
 * 
 * @param buffer Raw buffer from Redis
 * @param buffer_size Size of the buffer
 * @param response Output response structure (caller must free with redis_response_free)
 * @return REDIS_PARSE_OK on success, error code on failure
 */
redis_parse_result_t redis_parse_metadata_buffer(
    const uint8_t *buffer,
    size_t buffer_size,
    redis_response_t *response
);

/**
 * Get string representation of parse result
 * @param result Parse result code
 * @return String description of result
 */
const char* redis_parse_result_string(redis_parse_result_t result);

#ifdef __cplusplus
}
#endif

#endif /* REDIS_METADATA_PARSER_H */ 