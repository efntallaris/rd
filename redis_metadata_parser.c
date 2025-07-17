/**
 * Redis Metadata Buffer Parser - C Implementation
 * 
 * This library provides functions to parse Redis response buffers that contain
 * both metadata and data when the read-response-metadata feature is enabled.
 */

#include "redis_metadata_parser.h"
#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <endian.h>

#define MAGIC_HEADER "RDMT"
#define MAGIC_SIZE 4

/**
 * Initialize a metadata structure
 */
void redis_metadata_init(redis_metadata_t *metadata) {
    if (!metadata) return;
    
    memset(metadata, 0, sizeof(redis_metadata_t));
}

/**
 * Free resources in a metadata structure
 */
void redis_metadata_free(redis_metadata_t *metadata) {
    if (!metadata) return;
    
    if (metadata->key) {
        free(metadata->key);
        metadata->key = NULL;
    }
    if (metadata->field) {
        free(metadata->field);
        metadata->field = NULL;
    }
}

/**
 * Initialize a response structure
 */
void redis_response_init(redis_response_t *response) {
    if (!response) return;
    
    memset(response, 0, sizeof(redis_response_t));
    redis_metadata_init(&response->metadata);
}

/**
 * Free resources in a response structure
 */
void redis_response_free(redis_response_t *response) {
    if (!response) return;
    
    redis_metadata_free(&response->metadata);
    if (response->data) {
        free(response->data);
        response->data = NULL;
    }
    response->data_size = 0;
}

/**
 * Get human-readable data type name
 */
const char* redis_data_type_name(uint8_t data_type) {
    switch (data_type) {
        case REDIS_TYPE_STRING: return "string";
        case REDIS_TYPE_HASH:   return "hash";
        case REDIS_TYPE_LIST:   return "list";
        case REDIS_TYPE_SET:    return "set";
        case REDIS_TYPE_ZSET:   return "zset";
        case REDIS_TYPE_STREAM: return "stream";
        default:                return "unknown";
    }
}

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
) {
    if (!buffer || !response || buffer_size < MAGIC_SIZE) {
        return REDIS_PARSE_INVALID_ARGS;
    }
    
    // Initialize response
    redis_response_init(response);
    
    // Check magic header
    if (memcmp(buffer, MAGIC_HEADER, MAGIC_SIZE) != 0) {
        return REDIS_PARSE_NO_METADATA;
    }
    
    if (buffer_size < 20) { // Minimum header size
        return REDIS_PARSE_BUFFER_TOO_SMALL;
    }
    
    const uint8_t *ptr = buffer;
    
    // Skip magic header
    ptr += MAGIC_SIZE;
    
    // Parse header fields
    uint32_t total_size = ntohl(*(uint32_t*)ptr);
    ptr += 4;
    
    uint32_t metadata_size = ntohl(*(uint32_t*)ptr);
    ptr += 4;
    
    uint32_t data_size = ntohl(*(uint32_t*)ptr);
    ptr += 4;
    
    // Validate buffer size
    if (total_size != buffer_size) {
        return REDIS_PARSE_SIZE_MISMATCH;
    }
    
    // Parse key length
    if (ptr + 4 > buffer + buffer_size) {
        return REDIS_PARSE_BUFFER_TRUNCATED;
    }
    uint32_t key_len = ntohl(*(uint32_t*)ptr);
    ptr += 4;
    
    // Check if we have field length (hash format)
    uint32_t field_len = 0;
    size_t remaining_before_strings = metadata_size - (ptr - buffer);
    size_t min_remaining = 4 + 8 + 8 + 8 + 1; // data_len + timestamp + hits + misses + type
    
    if (remaining_before_strings >= min_remaining + 4) {
        if (ptr + 4 > buffer + buffer_size) {
            return REDIS_PARSE_BUFFER_TRUNCATED;
        }
        field_len = ntohl(*(uint32_t*)ptr);
        ptr += 4;
    }
    
    // Parse data length
    if (ptr + 4 > buffer + buffer_size) {
        return REDIS_PARSE_BUFFER_TRUNCATED;
    }
    uint32_t data_len = ntohl(*(uint32_t*)ptr);
    ptr += 4;
    
    if (data_len != data_size) {
        return REDIS_PARSE_SIZE_MISMATCH;
    }
    
    // Parse timestamp and stats
    if (ptr + 24 > buffer + buffer_size) {
        return REDIS_PARSE_BUFFER_TRUNCATED;
    }
    
    uint64_t access_timestamp = be64toh(*(uint64_t*)ptr);
    ptr += 8;
    
    uint64_t keyspace_hits = be64toh(*(uint64_t*)ptr);
    ptr += 8;
    
    uint64_t keyspace_misses = be64toh(*(uint64_t*)ptr);
    ptr += 8;
    
    // Parse data type
    if (ptr + 1 > buffer + buffer_size) {
        return REDIS_PARSE_BUFFER_TRUNCATED;
    }
    uint8_t data_type = *ptr;
    ptr += 1;
    
    // Parse key string
    if (ptr + key_len > buffer + buffer_size) {
        return REDIS_PARSE_BUFFER_TRUNCATED;
    }
    
    char *key = malloc(key_len + 1);
    if (!key) {
        return REDIS_PARSE_MEMORY_ERROR;
    }
    memcpy(key, ptr, key_len);
    key[key_len] = '\0';
    ptr += key_len;
    
    // Parse field string (if hash)
    char *field = NULL;
    if (field_len > 0) {
        if (ptr + field_len > buffer + buffer_size) {
            free(key);
            return REDIS_PARSE_BUFFER_TRUNCATED;
        }
        
        field = malloc(field_len + 1);
        if (!field) {
            free(key);
            return REDIS_PARSE_MEMORY_ERROR;
        }
        memcpy(field, ptr, field_len);
        field[field_len] = '\0';
        ptr += field_len;
    }
    
    // Parse actual data
    if (ptr + data_len > buffer + buffer_size) {
        free(key);
        if (field) free(field);
        return REDIS_PARSE_BUFFER_TRUNCATED;
    }
    
    uint8_t *data = malloc(data_len);
    if (!data) {
        free(key);
        if (field) free(field);
        return REDIS_PARSE_MEMORY_ERROR;
    }
    memcpy(data, ptr, data_len);
    
    // Fill response structure
    response->metadata.key = key;
    response->metadata.field = field;
    response->metadata.data_type = data_type;
    response->metadata.data_size = data_len;
    response->metadata.access_timestamp = access_timestamp;
    response->metadata.keyspace_hits = keyspace_hits;
    response->metadata.keyspace_misses = keyspace_misses;
    response->metadata.total_size = total_size;
    response->metadata.metadata_size = metadata_size;
    
    response->data = data;
    response->data_size = data_len;
    
    return REDIS_PARSE_OK;
}

/**
 * Get string representation of parse result
 */
const char* redis_parse_result_string(redis_parse_result_t result) {
    switch (result) {
        case REDIS_PARSE_OK:                return "Success";
        case REDIS_PARSE_NO_METADATA:      return "No metadata in buffer";
        case REDIS_PARSE_INVALID_ARGS:     return "Invalid arguments";
        case REDIS_PARSE_BUFFER_TOO_SMALL: return "Buffer too small";
        case REDIS_PARSE_BUFFER_TRUNCATED: return "Buffer truncated";
        case REDIS_PARSE_SIZE_MISMATCH:    return "Size mismatch";
        case REDIS_PARSE_MEMORY_ERROR:     return "Memory allocation error";
        default:                           return "Unknown error";
    }
} 