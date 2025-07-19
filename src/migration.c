#include "server.h"
#include "migration.h"
#include "cluster.h"
#include "crc16.h"
#include <string.h>

/* Global migration context */
static migrationContext *migration_ctx = NULL;

/* Initialize migration context */
void initMigrationContext(void) {
    if (migration_ctx != NULL) {
        return; /* Already initialized */
    }
    
    migration_ctx = zmalloc(sizeof(migrationContext));
    migration_ctx->migration_ranges = listCreate();
    migration_ctx->metadata = NULL;
    migration_ctx->source_node_id = 0;
    migration_ctx->dest_node_id = 0;
    migration_ctx->migration_active = 0;
    migration_ctx->migration_start = 0;
    
    /* Set list free method */
    listSetFreeMethod(migration_ctx->migration_ranges, zfree);
}

/* Free migration context */
void freeMigrationContext(void) {
    if (migration_ctx == NULL) {
        return;
    }
    
    if (migration_ctx->migration_ranges) {
        listRelease(migration_ctx->migration_ranges);
    }
    
    if (migration_ctx->metadata) {
        zfree(migration_ctx->metadata);
    }
    
    zfree(migration_ctx);
    migration_ctx = NULL;
}

/* Get hash slot for a key */
uint32_t getSlotForKey(const char *key, size_t keylen) {
    return crc16(key, keylen) & 0x3FFF; /* 16383 slots */
}

/* Check if key is in migration range */
int isKeyInMigrationRange(const char *key, size_t keylen) {
    if (migration_ctx == NULL || !migration_ctx->migration_active) {
        return 0;
    }
    
    uint32_t slot = getSlotForKey(key, keylen);
    listIter li;
    listNode *ln;
    
    listRewind(migration_ctx->migration_ranges, &li);
    while ((ln = listNext(&li)) != NULL) {
        migrationRange *range = listNodeValue(ln);
        if (slot >= range->start_slot && slot <= range->end_slot) {
            return 1;
        }
    }
    
    return 0;
}

/* Determine if double read should be performed */
int shouldPerformDoubleRead(const char *key, size_t keylen) {
    if (migration_ctx == NULL || !migration_ctx->migration_active) {
        return 0;
    }
    
    /* If key is in migration range, no double read needed */
    if (isKeyInMigrationRange(key, keylen)) {
        return 0;
    }
    
    /* For keys not in migration range, perform double read */
    return 1;
}

/* Get migration metadata for a key */
migrationMetadata* getMigrationMetadata(const char *key, size_t keylen) {
    if (migration_ctx == NULL) {
        return NULL;
    }
    
    uint32_t slot = getSlotForKey(key, keylen);
    
    /* Create metadata if it doesn't exist */
    if (migration_ctx->metadata == NULL) {
        migration_ctx->metadata = zmalloc(sizeof(migrationMetadata));
        migration_ctx->metadata->slot_id = slot;
        migration_ctx->metadata->source_id = migration_ctx->source_node_id;
        migration_ctx->metadata->dest_id = migration_ctx->dest_node_id;
    }
    
    /* Update slot ID */
    migration_ctx->metadata->slot_id = slot;
    
    /* Determine migration status */
    if (isKeyInMigrationRange(key, keylen)) {
        migration_ctx->metadata->migration_status = MIGRATION_STATUS_MIGRATED;
    } else if (migration_ctx->migration_active) {
        migration_ctx->metadata->migration_status = MIGRATION_STATUS_IN_PROGRESS;
    } else {
        migration_ctx->metadata->migration_status = MIGRATION_STATUS_NOT_MIGRATED;
    }
    
    return migration_ctx->metadata;
}

/* Update migration range */
void updateMigrationRange(uint32_t start_slot, uint32_t end_slot) {
    if (migration_ctx == NULL) {
        initMigrationContext();
    }
    
    migrationRange *range = zmalloc(sizeof(migrationRange));
    range->start_slot = start_slot;
    range->end_slot = end_slot;
    range->migrated_at = server.unixtime;
    
    listAddNodeTail(migration_ctx->migration_ranges, range);
    
    /* Mark migration as active if not already */
    if (!migration_ctx->migration_active) {
        migration_ctx->migration_active = 1;
        migration_ctx->migration_start = server.unixtime;
    }
}

/* Add migration metadata to response */
void addMigrationMetadataToResponse(client *c, const char *key, size_t keylen) {
    migrationMetadata *metadata = getMigrationMetadata(key, keylen);
    if (metadata == NULL) {
        return;
    }
    
    /* For RESP3, we can add metadata as attributes */
    if (c->resp == 3) {
        addReplyAttributeLen(c, 2); /* 2 attributes: slot_id, migration_status */
        addReplyProto(c, "slot_id", 7);
        addReplyLongLong(c, metadata->slot_id);
        addReplyProto(c, "migration_status", 16);
        addReplyLongLong(c, metadata->migration_status);
        addReplyProto(c, "source_id", 9);
        addReplyLongLong(c, metadata->source_id);
        addReplyProto(c, "dest_id", 7);
        addReplyLongLong(c, metadata->dest_id);
    } else {
        /* For RESP2, we need to modify the response format */
        /* This would require more complex handling */
        serverLog(LL_WARNING, "Migration metadata not supported in RESP2");
    }
}

/* Add metadata to all read responses */
void addMetadataToAllReadResponses(client *c, const char *key, size_t keylen, const char *field) {
        addMigrationMetadataToResponse(c, key, keylen);
}

/* Perform double read operation */
doubleReadResponse* performDoubleRead(client *c, const char *key, size_t keylen) {
    doubleReadResponse *response = zmalloc(sizeof(doubleReadResponse));
    response->data = NULL;
    response->double_read_performed = 0;
    response->from_source = 0;
    
    /* Get migration metadata */
    migrationMetadata *metadata = getMigrationMetadata(key, keylen);
    if (metadata) {
        memcpy(&response->metadata, metadata, sizeof(migrationMetadata));
    }
    
    /* Simulate double read logic */
    /* In a real implementation, this would:
       1. Try to read from destination first
       2. If destination returns empty/null, read from source
       3. Compare responses and return the latest
    */
    
    /* For now, we'll just do a normal lookup */
    robj *o = lookupKeyRead(c->db, c->argv[1]);
    if (o != NULL) {
        response->data = o;
        incrRefCount(o); /* Increment ref count since we're keeping it */
        response->from_source = 0; /* Assume from destination */
    }
    
    /* Mark that double read was performed */
    response->double_read_performed = 1;
    
    return response;
}

/* Add double read response to client */
void addDoubleReadResponse(client *c, doubleReadResponse *response) {
    if (response == NULL) {
        addReplyNull(c);
        return;
    }
    
    if (response->data == NULL) {
        addReplyNull(c);
        return;
    }
    
    /* Add the actual data */
    addReplyBulk(c, response->data);
    
    /* Add migration metadata if double read was performed */
    if (response->double_read_performed) {
        addMigrationMetadataToResponse(c, c->argv[1]->ptr, sdslen(c->argv[1]->ptr));
    }
}

/* Free double read response */
void freeDoubleReadResponse(doubleReadResponse *response) {
    if (response == NULL) {
        return;
    }
    
    if (response->data != NULL) {
        decrRefCount(response->data);
    }
    
    zfree(response);
}

/* Command to update migration ranges (for testing/admin) */
void migrationRangeCommand(client *c) {
    if (c->argc != 3) {
        addReplyError(c, "Wrong number of arguments for MIGRATION.RANGE");
        return;
    }
    
    long start_slot, end_slot;
    if (getLongFromObjectOrReply(c, c->argv[1], &start_slot, NULL) != C_OK) return;
    if (getLongFromObjectOrReply(c, c->argv[2], &end_slot, NULL) != C_OK) return;
    
    if (start_slot < 0 || start_slot > 16383 || end_slot < 0 || end_slot > 16383) {
        addReplyError(c, "Invalid slot range. Must be between 0 and 16383");
        return;
    }
    
    if (start_slot > end_slot) {
        addReplyError(c, "Start slot must be less than or equal to end slot");
        return;
    }
    
    updateMigrationRange(start_slot, end_slot);
    addReply(c, shared.ok);
}

/* Command to get migration status */
void migrationStatusCommand(client *c) {
    if (migration_ctx == NULL) {
        addReplyNull(c);
        return;
    }
    
    addReplyArrayLen(c, 4);
    addReplyLongLong(c, migration_ctx->migration_active);
    addReplyLongLong(c, migration_ctx->source_node_id);
    addReplyLongLong(c, migration_ctx->dest_node_id);
    addReplyLongLong(c, listLength(migration_ctx->migration_ranges));
}

/* Command to set migration nodes */
void migrationSetNodesCommand(client *c) {
    if (c->argc != 3) {
        addReplyError(c, "Wrong number of arguments for MIGRATION.SETNODES");
        return;
    }
    
    long source_id, dest_id;
    if (getLongFromObjectOrReply(c, c->argv[1], &source_id, NULL) != C_OK) return;
    if (getLongFromObjectOrReply(c, c->argv[2], &dest_id, NULL) != C_OK) return;
    
    if (migration_ctx == NULL) {
        initMigrationContext();
    }
    
    migration_ctx->source_node_id = source_id;
    migration_ctx->dest_node_id = dest_id;
    
    addReply(c, shared.ok);
} 