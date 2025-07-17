/**
 * Example usage of Redis Metadata Buffer Parser
 * 
 * This example demonstrates how to use the C library to parse
 * Redis metadata buffers.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "redis_metadata_parser.h"

/* Simple example of creating a mock buffer for testing */
void print_response(const redis_response_t *response) {
    printf("=== Redis Response ===\n");
    printf("Data: %.*s\n", (int)response->data_size, response->data);
    printf("Metadata:\n");
    printf("  Key: %s\n", response->metadata.key);
    if (response->metadata.field) {
        printf("  Field: %s\n", response->metadata.field);
    }
    printf("  Type: %s (%d)\n", 
           redis_data_type_name(response->metadata.data_type),
           response->metadata.data_type);
    printf("  Data Size: %u bytes\n", response->metadata.data_size);
    printf("  Access Time: %s", ctime((time_t*)&response->metadata.access_timestamp));
    printf("  Keyspace Hits: %lu\n", response->metadata.keyspace_hits);
    printf("  Keyspace Misses: %lu\n", response->metadata.keyspace_misses);
    printf("  Total Buffer Size: %u bytes\n", response->metadata.total_size);
    printf("  Metadata Size: %u bytes\n", response->metadata.metadata_size);
    printf("======================\n\n");
}

int main() {
    printf("Redis Metadata Buffer Parser - Example Usage\n");
    printf("============================================\n\n");

    /* Example 1: Parse a non-metadata buffer */
    printf("Example 1: Regular Redis response (no metadata)\n");
    const char *regular_data = "Hello, World!";
    redis_response_t response;
    
    redis_parse_result_t result = redis_parse_metadata_buffer(
        (const uint8_t*)regular_data,
        strlen(regular_data),
        &response
    );
    
    if (result == REDIS_PARSE_NO_METADATA) {
        printf("✓ Correctly identified as non-metadata buffer\n");
        printf("Raw data: %s\n\n", regular_data);
    } else {
        printf("✗ Unexpected result: %s\n\n", redis_parse_result_string(result));
    }

    /* Example 2: Show how to handle different result types */
    printf("Example 2: Error handling\n");
    
    uint8_t too_small_buffer[] = {'R', 'D', 'M'};  // Missing 'T'
    result = redis_parse_metadata_buffer(too_small_buffer, sizeof(too_small_buffer), &response);
    printf("Small buffer result: %s\n", redis_parse_result_string(result));
    
    uint8_t wrong_magic[] = {'W', 'R', 'O', 'N', 'G'};
    result = redis_parse_metadata_buffer(wrong_magic, sizeof(wrong_magic), &response);
    printf("Wrong magic result: %s\n\n", redis_parse_result_string(result));

    /* Example 3: Manual buffer creation for testing */
    printf("Example 3: Creating and parsing a metadata buffer\n");
    printf("(This would normally come from Redis server)\n");
    
    // This is what you'd typically get from a Redis client library
    // when read-response-metadata is enabled
    printf("In practice, you'd get this buffer from your Redis client.\n");
    printf("The buffer would be automatically created by the modified Redis server.\n\n");

    /* Example 4: Integration with Redis client libraries */
    printf("Example 4: Integration pattern\n");
    printf("Here's how you'd typically use this in your application:\n\n");
    
    printf("```c\n");
    printf("// Your Redis client code\n");
    printf("char *redis_response_buffer = redis_get(client, \"mykey\");\n");
    printf("size_t buffer_size = get_response_size(redis_response_buffer);\n");
    printf("\n");
    printf("redis_response_t parsed_response;\n");
    printf("redis_parse_result_t result = redis_parse_metadata_buffer(\n");
    printf("    (uint8_t*)redis_response_buffer,\n");
    printf("    buffer_size,\n");
    printf("    &parsed_response\n");
    printf(");\n");
    printf("\n");
    printf("if (result == REDIS_PARSE_OK) {\n");
    printf("    // Use parsed_response.data for your application\n");
    printf("    // Use parsed_response.metadata for monitoring/debugging\n");
    printf("    printf(\"Got data: %%.*s\\n\", \n");
    printf("           (int)parsed_response.data_size,\n");
    printf("           parsed_response.data);\n");
    printf("    printf(\"Key: %%s, Access time: %%lu\\n\",\n");
    printf("           parsed_response.metadata.key,\n");
    printf("           parsed_response.metadata.access_timestamp);\n");
    printf("    \n");
    printf("    redis_response_free(&parsed_response);\n");
    printf("} else if (result == REDIS_PARSE_NO_METADATA) {\n");
    printf("    // Regular Redis response, use directly\n");
    printf("    printf(\"Regular data: %%s\\n\", redis_response_buffer);\n");
    printf("} else {\n");
    printf("    fprintf(stderr, \"Parse error: %%s\\n\",\n");
    printf("            redis_parse_result_string(result));\n");
    printf("}\n");
    printf("```\n\n");

    /* Example 5: Performance considerations */
    printf("Example 5: Performance tips\n");
    printf("- Check for metadata magic header first (4 bytes) before full parsing\n");
    printf("- Reuse response structures when possible\n");
    printf("- Consider pooling allocations for high-throughput applications\n");
    printf("- The parser is zero-copy for strings (they point into the buffer)\n\n");

    printf("Library features:\n");
    printf("✓ Handles both string and hash data types\n");
    printf("✓ Network byte order handling\n");
    printf("✓ Comprehensive error checking\n");
    printf("✓ Memory management helpers\n");
    printf("✓ Works with any Redis client library\n");
    printf("✓ Backward compatible (detects non-metadata responses)\n\n");

    printf("To enable metadata in Redis:\n");
    printf("CONFIG SET read-response-metadata yes\n\n");

    return 0;
} 