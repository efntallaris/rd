#ifndef __MIGRATION_H
#define __MIGRATION_H

#include "server.h"

/* Migration completion range structure */
typedef struct migrationRange {
    uint32_t start_slot;
    uint32_t end_slot;
    time_t migrated_at;
} migrationRange;

/* Migration metadata structure */
typedef struct migrationMetadata {
    uint32_t slot_id;           /* Hash slot ID */
    uint32_t source_id;         /* Source node ID */
    uint32_t dest_id;           /* Destination node ID */
    uint8_t migration_status;   /* 0=not migrated, 1=migrated, 2=in progress */
} migrationMetadata;

/* Migration context structure */
typedef struct migrationContext {
    list *migration_ranges;     /* List of migration completion ranges */
    migrationMetadata *metadata; /* Current migration metadata */
    uint32_t source_node_id;    /* Source node ID */
    uint32_t dest_node_id;      /* Destination node ID */
    int migration_active;       /* Whether migration is active */
    time_t migration_start;     /* When migration started */
} migrationContext;

/* Migration status constants */
#define MIGRATION_STATUS_NOT_MIGRATED 0
#define MIGRATION_STATUS_MIGRATED     1
#define MIGRATION_STATUS_IN_PROGRESS  2

/* Function declarations */
void initMigrationContext(void);
void freeMigrationContext(void);
int isKeyInMigrationRange(const char *key, size_t keylen);
int shouldPerformDoubleRead(const char *key, size_t keylen);
void addMigrationMetadataToResponse(client *c, const char *key, size_t keylen);
void addMetadataToAllReadResponses(client *c, const char *key, size_t keylen, const char *field);
void updateMigrationRange(uint32_t start_slot, uint32_t end_slot);
migrationMetadata* getMigrationMetadata(const char *key, size_t keylen);
uint32_t getSlotForKey(const char *key, size_t keylen);

/* Double read response structure */
typedef struct doubleReadResponse {
    robj *data;                 /* The actual data */
    migrationMetadata metadata; /* Migration metadata */
    int from_source;           /* Whether data came from source (1) or dest (0) */
    int double_read_performed; /* Whether double read was actually performed */
} doubleReadResponse;

/* Double read functions */
doubleReadResponse* performDoubleRead(client *c, const char *key, size_t keylen);
void addDoubleReadResponse(client *c, doubleReadResponse *response);
void freeDoubleReadResponse(doubleReadResponse *response);

#endif /* __MIGRATION_H */ 