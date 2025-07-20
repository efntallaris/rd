#include "server.h"
#include "migration.h"
#include "cluster.h"
#include <string.h>

/* Get hash slot for a key */
uint32_t getSlotForKey(const char *key, size_t keylen) {
    uint32_t slot = keyHashSlot((char*)key, (int)keylen);
    serverLog(LL_DEBUG, "Key '%.*s' mapped to slot %u", (int)keylen, key, slot);
    return slot;
}

/* Check if key is in migration range */
int isKeyInMigrationRange(const char *key, size_t keylen) {
    if (server.cluster == NULL) {
        serverLog(LL_DEBUG, "Key '%.*s' not in migration range: cluster not enabled", (int)keylen, key);
        return 0;
    }
    
    uint32_t slot = getSlotForKey(key, keylen);
    
    /* Check if this slot is being migrated to another node */
    if (server.cluster->migrating_slots_to[slot] != NULL) {
        serverLog(LL_DEBUG, "Key '%.*s' (slot %u) found in migrating slot to node %.40s", 
                 (int)keylen, key, slot, server.cluster->migrating_slots_to[slot]->name);
        return 1;
    }
    
    /* Check if this slot is being imported from another node */
    if (server.cluster->importing_slots_from[slot] != NULL) {
        serverLog(LL_DEBUG, "Key '%.*s' (slot %u) found in importing slot from node %.40s", 
                 (int)keylen, key, slot, server.cluster->importing_slots_from[slot]->name);
        return 1;
    }
    
    serverLog(LL_DEBUG, "Key '%.*s' (slot %u) not found in any migration range", (int)keylen, key, slot);
    return 0;
}

/* Determine if double read should be performed */
int shouldPerformDoubleRead(const char *key, size_t keylen) {
    if (server.cluster == NULL) {
        serverLog(LL_DEBUG, "Double read not needed for key '%.*s': cluster not enabled", (int)keylen, key);
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
    if (server.cluster == NULL) {
        serverLog(LL_WARNING, "Cannot get metadata for key '%.*s': cluster not enabled", (int)keylen, key);
        return NULL;
    }
    
    uint32_t slot = getSlotForKey(key, keylen);
    
    /* Allocate metadata structure */
    migrationMetadata *metadata = zmalloc(sizeof(migrationMetadata));
    metadata->slot_id = slot;
    metadata->source_id = 0;
    metadata->dest_id = 0;
    
    /* Determine migration status based on cluster state */
    if (server.cluster->migrating_slots_to[slot] != NULL) {
        /* This slot is being migrated to another node */
        metadata->migration_status = MIGRATION_STATUS_IN_PROGRESS;
        metadata->source_id = (uint32_t)server.cluster->myself->configEpoch;
        metadata->dest_id = (uint32_t)server.cluster->migrating_slots_to[slot]->configEpoch;
        serverLog(LL_DEBUG, "Key '%.*s' (slot %u) marked as MIGRATING to node %.40s", 
                 (int)keylen, key, slot, server.cluster->migrating_slots_to[slot]->name);
    } else if (server.cluster->importing_slots_from[slot] != NULL) {
        /* This slot is being imported from another node */
        metadata->migration_status = MIGRATION_STATUS_IN_PROGRESS;
        metadata->source_id = (uint32_t)server.cluster->importing_slots_from[slot]->configEpoch;
        metadata->dest_id = (uint32_t)server.cluster->myself->configEpoch;
        serverLog(LL_DEBUG, "Key '%.*s' (slot %u) marked as IMPORTING from node %.40s", 
                 (int)keylen, key, slot, server.cluster->importing_slots_from[slot]->name);
    } else {
        /* This slot is not being migrated */
        metadata->migration_status = MIGRATION_STATUS_NOT_MIGRATED;
        metadata->source_id = (uint32_t)server.cluster->myself->configEpoch;
        metadata->dest_id = 0;
        serverLog(LL_DEBUG, "Key '%.*s' (slot %u) marked as NOT_MIGRATED", (int)keylen, key, slot);
    }
    
    return metadata;
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

    addReplyAttributeLen(c, 4); /* 4 attributes: slot_id, migration_status */
    addReplyProto(c, "slot_id", 7);
    addReplyLongLong(c, metadata->slot_id);
    addReplyProto(c, "migration_status", 16);
    addReplyLongLong(c, metadata->migration_status);
    addReplyProto(c, "source_id", 9);
    addReplyLongLong(c, metadata->source_id);
    addReplyProto(c, "dest_id", 7);
    addReplyLongLong(c, metadata->dest_id);
    
    /* Free the metadata since we don't need it anymore */
    zfree(metadata);
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
        zfree(metadata); /* Free since we copied it */
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

/* Command to get migration status */
void migrationStatusCommand(client *c) {
    serverLog(LL_WARNING, "MIGRATION.STATUS command called");
    
    if (server.cluster == NULL) {
        serverLog(LL_WARNING, "MIGRATION.STATUS: cluster not enabled");
        addReplyNull(c);
        return;
    }
    
    int migrating_count = 0;
    int importing_count = 0;
    
    /* Count migrating and importing slots */
    for (int i = 0; i < CLUSTER_SLOTS; i++) {
        if (server.cluster->migrating_slots_to[i] != NULL) {
            migrating_count++;
        }
        if (server.cluster->importing_slots_from[i] != NULL) {
            importing_count++;
        }
    }
    
    serverLog(LL_WARNING, "MIGRATION.STATUS: migrating_slots=%d, importing_slots=%d", 
             migrating_count, importing_count);
    
    addReplyArrayLen(c, 3);
    addReplyLongLong(c, migrating_count);
    addReplyLongLong(c, importing_count);
    addReplyLongLong(c, server.cluster->currentEpoch);
}

/* Command to get migration info for a specific slot */
void migrationSlotInfoCommand(client *c) {
    if (c->argc != 2) {
        addReplyError(c, "Wrong number of arguments for MIGRATION.SLOTINFO");
        return;
    }
    
    long slot;
    if (getLongFromObjectOrReply(c, c->argv[1], &slot, NULL) != C_OK) return;
    
    if (slot < 0 || slot >= CLUSTER_SLOTS) {
        addReplyError(c, "Invalid slot number. Must be between 0 and 16383");
        return;
    }
    
    if (server.cluster == NULL) {
        addReplyNull(c);
        return;
    }
    
    addReplyArrayLen(c, 3);
    
    if (server.cluster->migrating_slots_to[slot] != NULL) {
        addReplyBulkCString(c, "migrating");
        addReplyBulkCString(c, server.cluster->migrating_slots_to[slot]->name);
        addReplyLongLong(c, server.cluster->migrating_slots_to[slot]->configEpoch);
    } else if (server.cluster->importing_slots_from[slot] != NULL) {
        addReplyBulkCString(c, "importing");
        addReplyBulkCString(c, server.cluster->importing_slots_from[slot]->name);
        addReplyLongLong(c, server.cluster->importing_slots_from[slot]->configEpoch);
    } else {
        addReplyBulkCString(c, "stable");
        addReplyNull(c);
        addReplyLongLong(c, 0);
    }
} 