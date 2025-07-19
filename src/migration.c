#include "server.h"
#include "migration.h"
#include "cluster.h"
#include <string.h>

/* Global migration context */
static migrationContext *migration_ctx = NULL;

/* Initialize migration context */
void initMigrationContext(void) {
    if (migration_ctx != NULL) {
        serverLog(LL_DEBUG, "Migration context already initialized");
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
    
    serverLog(LL_WARNING, "Migration context initialized successfully");
}

/* Free migration context */
void freeMigrationContext(void) {
    if (migration_ctx == NULL) {
        serverLog(LL_DEBUG, "Migration context is already NULL");
        return;
    }
    
    serverLog(LL_DEBUG, "Freeing migration context");
    
    if (migration_ctx->migration_ranges) {
        listRelease(migration_ctx->migration_ranges);
        serverLog(LL_DEBUG, "Released migration ranges list");
    }
    
    if (migration_ctx->metadata) {
        zfree(migration_ctx->metadata);
        serverLog(LL_DEBUG, "Freed migration metadata");
    }
    
    zfree(migration_ctx);
    migration_ctx = NULL;
    serverLog(LL_DEBUG, "Migration context freed successfully");
}

/* Get hash slot for a key */
uint32_t getSlotForKey(const char *key, size_t keylen) {
    uint32_t slot = keyHashSlot((char*)key, (int)keylen);
    serverLog(LL_DEBUG, "Key '%.*s' mapped to slot %u", (int)keylen, key, slot);
    return slot;
}

/* Check if key is in migration range */
int isKeyInMigrationRange(const char *key, size_t keylen) {
    if (migration_ctx == NULL || !migration_ctx->migration_active) {
        serverLog(LL_DEBUG, "Key '%.*s' not in migration range: context NULL or migration inactive", (int)keylen, key);
        return 0;
    }
    
    uint32_t slot = getSlotForKey(key, keylen);
    listIter li;
    listNode *ln;
    
    listRewind(migration_ctx->migration_ranges, &li);
    while ((ln = listNext(&li)) != NULL) {
        migrationRange *range = listNodeValue(ln);
        if (slot >= range->start_slot && slot <= range->end_slot) {
            serverLog(LL_DEBUG, "Key '%.*s' (slot %u) found in migration range [%u-%u]", 
                     (int)keylen, key, slot, range->start_slot, range->end_slot);
            return 1;
        }
    }
    
    serverLog(LL_DEBUG, "Key '%.*s' (slot %u) not found in any migration range", (int)keylen, key, slot);
    return 0;
}

/* Determine if double read should be performed */
int shouldPerformDoubleRead(const char *key, size_t keylen) {
    if (migration_ctx == NULL || !migration_ctx->migration_active) {
        serverLog(LL_DEBUG, "Double read not needed for key '%.*s': migration inactive", (int)keylen, key);
        return 0;
    }
    
    /* If key is in migration range, no double read needed */
    if (isKeyInMigrationRange(key, keylen)) {
        serverLog(LL_DEBUG, "Double read not needed for key '%.*s': key in migration range", (int)keylen, key);
        return 0;
    }
    
    /* For keys not in migration range, perform double read */
    serverLog(LL_DEBUG, "Double read needed for key '%.*s': key not in migration range", (int)keylen, key);
    return 1;
}

/* Get migration metadata for a key */
migrationMetadata* getMigrationMetadata(const char *key, size_t keylen) {
    if (migration_ctx == NULL) {
        serverLog(LL_WARNING, "Cannot get metadata for key '%.*s': migration context is NULL", (int)keylen, key);
        return NULL;
    }
    
    uint32_t slot = getSlotForKey(key, keylen);
    
    /* Create metadata if it doesn't exist */
    if (migration_ctx->metadata == NULL) {
        migration_ctx->metadata = zmalloc(sizeof(migrationMetadata));
        migration_ctx->metadata->slot_id = slot;
        migration_ctx->metadata->source_id = migration_ctx->source_node_id;
        migration_ctx->metadata->dest_id = migration_ctx->dest_node_id;
        serverLog(LL_DEBUG, "Created new migration metadata for key '%.*s' (slot %u)", (int)keylen, key, slot);
    }
    
    /* Update slot ID */
    migration_ctx->metadata->slot_id = slot;
    
    /* Determine migration status */
    if (isKeyInMigrationRange(key, keylen)) {
        migration_ctx->metadata->migration_status = MIGRATION_STATUS_MIGRATED;
        serverLog(LL_DEBUG, "Key '%.*s' (slot %u) marked as MIGRATED", (int)keylen, key, slot);
    } else if (migration_ctx->migration_active) {
        migration_ctx->metadata->migration_status = MIGRATION_STATUS_IN_PROGRESS;
        serverLog(LL_DEBUG, "Key '%.*s' (slot %u) marked as IN_PROGRESS", (int)keylen, key, slot);
    } else {
        migration_ctx->metadata->migration_status = MIGRATION_STATUS_NOT_MIGRATED;
        serverLog(LL_DEBUG, "Key '%.*s' (slot %u) marked as NOT_MIGRATED", (int)keylen, key, slot);
    }
    
    return migration_ctx->metadata;
}

/* Update migration range */
void updateMigrationRange(uint32_t start_slot, uint32_t end_slot) {
    serverLog(LL_DEBUG, "Updating migration range: slots %u-%u", start_slot, end_slot);
    
    if (migration_ctx == NULL) {
        serverLog(LL_DEBUG, "Migration context is NULL, initializing");
        initMigrationContext();
    }
    
    migrationRange *range = zmalloc(sizeof(migrationRange));
    range->start_slot = start_slot;
    range->end_slot = end_slot;
    range->migrated_at = server.unixtime;
    
    listAddNodeTail(migration_ctx->migration_ranges, range);
    serverLog(LL_DEBUG, "Added migration range [%u-%u] to list", start_slot, end_slot);
    
    /* Mark migration as active if not already */
    if (!migration_ctx->migration_active) {
        migration_ctx->migration_active = 1;
        migration_ctx->migration_start = server.unixtime;
        serverLog(LL_DEBUG, "Migration marked as active, start time: %ld", migration_ctx->migration_start);
    } else {
        serverLog(LL_DEBUG, "Migration already active, start time: %ld", migration_ctx->migration_start);
    }
}

/* Add migration metadata to response */
void addMigrationMetadataToResponse(client *c, const char *key, size_t keylen) {
    migrationMetadata *metadata = getMigrationMetadata(key, keylen);
    if (metadata == NULL) {
        serverLog(LL_WARNING, "Cannot add metadata to response for key '%.*s': metadata is NULL", (int)keylen, key);
        return;
    }
    
    serverLog(LL_WARNING, "Adding metadata to response for key '%.*s': slot=%u, status=%d, source=%u, dest=%u", 
             (int)keylen, key, metadata->slot_id, metadata->migration_status, metadata->source_id, metadata->dest_id);

    addReplyAttributeLen(c, 2); /* 2 attributes: slot_id, migration_status */
    addReplyProto(c, "slot_id", 7);
    addReplyLongLong(c, metadata->slot_id);
    addReplyProto(c, "migration_status", 16);
    addReplyLongLong(c, metadata->migration_status);
    addReplyProto(c, "source_id", 9);
    addReplyLongLong(c, metadata->source_id);
    addReplyProto(c, "dest_id", 7);
    addReplyLongLong(c, metadata->dest_id);
}

/* Add metadata to all read responses */
void addMetadataToAllReadResponses(client *c, const char *key, size_t keylen, const char *field) {
    serverLog(LL_WARNING, "Adding metadata to read response for key '%.*s'%s%s", 
             (int)keylen, key, field ? " field '" : "", field ? field : "", field ? "'" : "");
    addMigrationMetadataToResponse(c, key, keylen);
}

/* Perform double read operation */
doubleReadResponse* performDoubleRead(client *c, const char *key, size_t keylen) {
    serverLog(LL_DEBUG, "Performing double read for key '%.*s'", (int)keylen, key);
    
    doubleReadResponse *response = zmalloc(sizeof(doubleReadResponse));
    response->data = NULL;
    response->double_read_performed = 0;
    response->from_source = 0;
    
    /* Get migration metadata */
    migrationMetadata *metadata = getMigrationMetadata(key, keylen);
    if (metadata) {
        memcpy(&response->metadata, metadata, sizeof(migrationMetadata));
        serverLog(LL_DEBUG, "Got migration metadata for double read: slot=%u, status=%d", 
                 metadata->slot_id, metadata->migration_status);
    } else {
        serverLog(LL_DEBUG, "No migration metadata available for double read");
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
        serverLog(LL_DEBUG, "Double read found data for key '%.*s' (from destination)", (int)keylen, key);
    } else {
        serverLog(LL_DEBUG, "Double read found no data for key '%.*s'", (int)keylen, key);
    }
    
    /* Mark that double read was performed */
    response->double_read_performed = 1;
    serverLog(LL_DEBUG, "Double read completed for key '%.*s'", (int)keylen, key);
    
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
    serverLog(LL_WARNING, "MIGRATION.RANGE command called with %d arguments", c->argc);
    
    if (c->argc != 3) {
        serverLog(LL_DEBUG, "MIGRATION.RANGE: wrong number of arguments (%d)", c->argc);
        addReplyError(c, "Wrong number of arguments for MIGRATION.RANGE");
        return;
    }
    
    long start_slot, end_slot;
    if (getLongFromObjectOrReply(c, c->argv[1], &start_slot, NULL) != C_OK) return;
    if (getLongFromObjectOrReply(c, c->argv[2], &end_slot, NULL) != C_OK) return;
    
    serverLog(LL_DEBUG, "MIGRATION.RANGE: parsing slots %ld-%ld", start_slot, end_slot);
    
    if (start_slot < 0 || start_slot > 16383 || end_slot < 0 || end_slot > 16383) {
        serverLog(LL_DEBUG, "MIGRATION.RANGE: invalid slot range %ld-%ld", start_slot, end_slot);
        addReplyError(c, "Invalid slot range. Must be between 0 and 16383");
        return;
    }
    
    if (start_slot > end_slot) {
        serverLog(LL_DEBUG, "MIGRATION.RANGE: start slot %ld > end slot %ld", start_slot, end_slot);
        addReplyError(c, "Start slot must be less than or equal to end slot");
        return;
    }
    
    serverLog(LL_DEBUG, "MIGRATION.RANGE: updating migration range %ld-%ld", start_slot, end_slot);
    updateMigrationRange(start_slot, end_slot);
    addReply(c, shared.ok);
}

/* Command to get migration status */
void migrationStatusCommand(client *c) {
    serverLog(LL_DEBUG, "MIGRATION.STATUS command called");
    
    if (migration_ctx == NULL) {
        serverLog(LL_DEBUG, "MIGRATION.STATUS: migration context is NULL");
        addReplyNull(c);
        return;
    }
    
    int range_count = listLength(migration_ctx->migration_ranges);
    serverLog(LL_DEBUG, "MIGRATION.STATUS: active=%d, source=%u, dest=%u, ranges=%d", 
             migration_ctx->migration_active, migration_ctx->source_node_id, 
             migration_ctx->dest_node_id, range_count);
    
    addReplyArrayLen(c, 4);
    addReplyLongLong(c, migration_ctx->migration_active);
    addReplyLongLong(c, migration_ctx->source_node_id);
    addReplyLongLong(c, migration_ctx->dest_node_id);
    addReplyLongLong(c, range_count);
}

/* Command to set migration nodes */
void migrationSetNodesCommand(client *c) {
    serverLog(LL_DEBUG, "MIGRATION.SETNODES command called with %d arguments", c->argc);
    
    if (c->argc != 3) {
        serverLog(LL_DEBUG, "MIGRATION.SETNODES: wrong number of arguments (%d)", c->argc);
        addReplyError(c, "Wrong number of arguments for MIGRATION.SETNODES");
        return;
    }
    
    long source_id, dest_id;
    if (getLongFromObjectOrReply(c, c->argv[1], &source_id, NULL) != C_OK) return;
    if (getLongFromObjectOrReply(c, c->argv[2], &dest_id, NULL) != C_OK) return;
    
    serverLog(LL_DEBUG, "MIGRATION.SETNODES: parsing source_id=%ld, dest_id=%ld", source_id, dest_id);
    
    if (migration_ctx == NULL) {
        serverLog(LL_DEBUG, "MIGRATION.SETNODES: migration context is NULL, initializing");
        initMigrationContext();
    }
    
    migration_ctx->source_node_id = source_id;
    migration_ctx->dest_node_id = dest_id;
    
    serverLog(LL_DEBUG, "MIGRATION.SETNODES: set source_id=%u, dest_id=%u", 
             migration_ctx->source_node_id, migration_ctx->dest_node_id);
    
    addReply(c, shared.ok);
} 