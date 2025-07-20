#ifndef __MIGRATION_H
#define __MIGRATION_H

#include "server.h"

/* Migration status constants */
#define MIGRATION_STATUS_NOT_MIGRATED 0
#define MIGRATION_STATUS_IN_PROGRESS  1
#define MIGRATION_STATUS_MIGRATED     2

/* Migration metadata structure */
typedef struct migrationMetadata {
    uint32_t slot_id;
    uint32_t source_id;
    uint32_t dest_id;
    uint32_t migration_status;
} migrationMetadata;

/* Double read response structure */
typedef struct doubleReadResponse {
    robj *data;
    int double_read_performed;
    int from_source;
    migrationMetadata metadata;
} doubleReadResponse;

/* Function declarations */

/* Get hash slot for a key */
uint32_t getSlotForKey(const char *key, size_t keylen);

/* Check if key is in migration range */
int isKeyInMigrationRange(const char *key, size_t keylen);

/* Determine if double read should be performed */
int shouldPerformDoubleRead(const char *key, size_t keylen);

/* Get migration metadata for a key */
migrationMetadata* getMigrationMetadata(const char *key, size_t keylen);

/* Add migration metadata to response */
void addMigrationMetadataToResponse(client *c, const char *key, size_t keylen);

/* Add metadata to all read responses */
void addMetadataToAllReadResponses(client *c, const char *key, size_t keylen);


#endif /* __MIGRATION_H */ 