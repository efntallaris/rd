#ifndef __MIGRATION_H
#define __MIGRATION_H

#include "server.h"

/* Migration status constants */
#define MIGRATION_STATUS_NOT_MIGRATED 0
#define MIGRATION_STATUS_IN_PROGRESS  1
#define MIGRATION_STATUS_MIGRATED     2

/* Optimized migration metadata structure - saves space */
typedef struct migrationMetadata {
    uint16_t slot_id;           /* Reduced from uint32_t to uint16_t (0-16383) */
    uint16_t migration_status;  /* Reduced from uint32_t to uint16_t (0-2) */
    uint32_t dest_id;           /* Keep as uint32_t for node IDs */
} migrationMetadata;

/* Single buffer structure for metadata + data */
typedef struct metadataBuffer {
    migrationMetadata metadata;
    char *data;
    size_t data_len;
    size_t total_size;
} metadataBuffer;

/* Function declarations */

/* Get hash slot for a key */
uint32_t getSlotForKey(const char *key, size_t keylen);

/* Check if key is in migration range */
int isKeyInMigrationRange(const char *key, size_t keylen);

/* Determine if double read should be performed */
int shouldPerformDoubleRead(const char *key, size_t keylen);

/* Get migration metadata for a key - original version */
migrationMetadata* getMigrationMetadata(const char *key, size_t keylen);

/* Get migration metadata for a key - optimized version */
migrationMetadata* getMigrationMetadataOptimized(const char *key, size_t keylen);

/* Add migration metadata to response - original version */
void addMigrationMetadataToResponse(client *c, const char *key, size_t keylen);

/* Add migration metadata to response - optimized version */
void addMigrationMetadataToResponseOptimized(client *c, const char *key, size_t keylen, const char *value, size_t valuelen);


/* Single buffer approach functions */
metadataBuffer* createMetadataBuffer(const char *key, size_t keylen, const char *value, size_t valuelen);
void addMetadataBufferToResponse(client *c, metadataBuffer *buffer);
void freeMetadataBuffer(metadataBuffer *buffer);

/* New functions for single buffer approach with metadata appended */
char* createDataWithMetadataBuffer(const char *value, size_t valuelen, const char *key, size_t keylen, size_t *total_len);
void addReplyBulkCBufferWithMetadata(client *c, const char *value, size_t valuelen, const char *key, size_t keylen);
migrationMetadata* extractMetadataFromBuffer(const char *buffer, size_t buffer_len, size_t data_len);
char* extractDataFromBuffer(const char *buffer, size_t buffer_len, size_t *data_len);

/* Command functions */
void migrationStatusCommand(client *c);
void migrationSlotInfoCommand(client *c);

#endif /* __MIGRATION_H */ 