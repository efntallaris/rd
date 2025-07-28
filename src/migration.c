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

/* Get migration metadata for a key - original version */
migrationMetadata* getMigrationMetadata(const char *key, size_t keylen) {
    if (server.cluster == NULL) {
        serverLog(LL_WARNING, "Cannot get metadata for key '%.*s': cluster not enabled", (int)keylen, key);
        return NULL;
    }
    
    uint32_t slot = getSlotForKey(key, keylen);
    
    /* Allocate metadata structure */
    migrationMetadata *metadata = zmalloc(sizeof(migrationMetadata));
    metadata->slot_id = slot;
    
    /* Initialize host and port fields */
    metadata->host[0] = '\0';  /* Initialize empty */
    metadata->port = 0;        /* Initialize to 0 */
    
    /* Set host and port from server configuration */
    if (server.cluster != NULL && server.cluster->myself != NULL) {
        strncpy(metadata->host, server.cluster->myself->ip, MAX_HOST_LEN - 1);
        metadata->host[MAX_HOST_LEN - 1] = '\0';  /* Ensure null termination */
    }
    metadata->port = server.port;
    
    /* Determine migration status based on cluster state */
    if (server.cluster->migrating_slots_to[slot] != NULL) {
        /* This slot is being migrated to another node */
        metadata->migration_status = MIGRATION_STATUS_IN_PROGRESS;
        serverLog(LL_DEBUG, "Key '%.*s' (slot %u) marked as MIGRATING to node %.40s", 
                 (int)keylen, key, slot, server.cluster->migrating_slots_to[slot]->name);
    } else if (server.cluster->importing_slots_from[slot] != NULL) {
        /* This slot is being imported from another node */
        metadata->migration_status = MIGRATION_STATUS_IN_PROGRESS;
        serverLog(LL_DEBUG, "Key '%.*s' (slot %u) marked as IMPORTING from node %.40s", 
                 (int)keylen, key, slot, server.cluster->importing_slots_from[slot]->name);
    } else {
        /* This slot is not being migrated */
        metadata->migration_status = MIGRATION_STATUS_NOT_MIGRATED;
        serverLog(LL_DEBUG, "Key '%.*s' (slot %u) marked as NOT_MIGRATED", (int)keylen, key, slot);
    }
    
    return metadata;
}

/* Get migration metadata for a key - optimized version */
migrationMetadata* getMigrationMetadataOptimized(const char *key, size_t keylen) {
    if (server.cluster == NULL) {
        serverLog(LL_WARNING, "Cannot get metadata for key '%.*s': cluster not enabled", (int)keylen, key);
        return NULL;
    }
    
    uint32_t slot = getSlotForKey(key, keylen);
    
    /* Allocate metadata structure */
    migrationMetadata *metadata = zmalloc(sizeof(migrationMetadata));
    metadata->slot_id = (uint16_t)slot;  /* Cast to uint16_t to save space */
    
    /* Initialize host and port fields */
    metadata->host[0] = '\0';  /* Initialize empty */
    metadata->port = 0;        /* Initialize to 0 */
    
    /* Set host and port from server configuration */
    if (server.cluster != NULL && server.cluster->myself != NULL) {
        strncpy(metadata->host, server.cluster->myself->ip, MAX_HOST_LEN - 1);
        metadata->host[MAX_HOST_LEN - 1] = '\0';  /* Ensure null termination */
    }
    metadata->port = server.port;
    
    /* Determine migration status based on cluster state */
    if (server.cluster->migrating_slots_to[slot] != NULL) {
        /* This slot is being migrated to another node */
        metadata->migration_status = (uint16_t)MIGRATION_STATUS_IN_PROGRESS;
        serverLog(LL_DEBUG, "Key '%.*s' (slot %u) marked as MIGRATING to node %.40s", 
                 (int)keylen, key, slot, server.cluster->migrating_slots_to[slot]->name);
    } else if (server.cluster->importing_slots_from[slot] != NULL) {
        /* This slot is being imported from another node */
        metadata->migration_status = (uint16_t)MIGRATION_STATUS_IN_PROGRESS;
        serverLog(LL_DEBUG, "Key '%.*s' (slot %u) marked as IMPORTING from node %.40s", 
                 (int)keylen, key, slot, server.cluster->importing_slots_from[slot]->name);
    } else {
        /* This slot is not being migrated */
        metadata->migration_status = (uint16_t)MIGRATION_STATUS_NOT_MIGRATED;
        serverLog(LL_DEBUG, "Key '%.*s' (slot %u) marked as NOT_MIGRATED", (int)keylen, key, slot);
    }
    
    return metadata;
}

/* Create a single buffer containing metadata and data */
metadataBuffer* createMetadataBuffer(const char *key, size_t keylen, const char *value, size_t valuelen) {
    migrationMetadata *metadata = getMigrationMetadataOptimized(key, keylen);
    if (metadata == NULL) {
        return NULL;
    }
    
    /* Calculate total buffer size */
    size_t metadata_size = sizeof(migrationMetadata);
    size_t total_size = metadata_size + valuelen;
    
    /* Allocate the combined buffer */
    metadataBuffer *buffer = zmalloc(sizeof(metadataBuffer));
    buffer->metadata = *metadata;  /* Copy metadata */
    buffer->data = zmalloc(valuelen);
    buffer->data_len = valuelen;
    buffer->total_size = total_size;
    
    /* Copy the value data */
    memcpy(buffer->data, value, valuelen);
    
    /* Free the temporary metadata */
    zfree(metadata);
    
    serverLog(LL_DEBUG, "Created metadata buffer: slot=%u, status=%u, host=%s, port=%u, size=%zu", 
             buffer->metadata.slot_id, buffer->metadata.migration_status, 
             buffer->metadata.host, buffer->metadata.port, total_size);
    
    return buffer;
}

/* Free metadata buffer */
void freeMetadataBuffer(metadataBuffer *buffer) {
    if (buffer != NULL) {
        if (buffer->data != NULL) {
            zfree(buffer->data);
        }
        zfree(buffer);
    }
}

// Helper functions for little-endian serialization
static void write_le16(char *buf, uint16_t v) {
    buf[0] = v & 0xFF;
    buf[1] = (v >> 8) & 0xFF;
}
static void write_le32(char *buf, uint32_t v) {
    buf[0] = v & 0xFF;
    buf[1] = (v >> 8) & 0xFF;
    buf[2] = (v >> 16) & 0xFF;
    buf[3] = (v >> 24) & 0xFF;
}

char* createDataWithMetadataBuffer(const char *value, size_t valuelen, const char *key, size_t keylen, size_t *total_len) {
    migrationMetadata *metadata = getMigrationMetadataOptimized(key, keylen);
    if (metadata == NULL) {
        /* If no metadata, just return the original data */
        *total_len = valuelen;
        char *buffer = zmalloc(valuelen);
        memcpy(buffer, value, valuelen);
        return buffer;
    }
    
    size_t metadata_size = 50; // 2+2+46+2 (slot_id + migration_status + host + port)
    size_t total_size = valuelen + metadata_size;
    char *buffer = zmalloc(total_size);
    memcpy(buffer, value, valuelen);
    size_t offset = valuelen;
    write_le16(buffer + offset, metadata->slot_id); offset += 2;
    write_le16(buffer + offset, metadata->migration_status); offset += 2;
    memcpy(buffer + offset, metadata->host, MAX_HOST_LEN); offset += MAX_HOST_LEN;
    write_le16(buffer + offset, metadata->port); offset += 2;
    *total_len = total_size;
    zfree(metadata);
    serverLog(LL_DEBUG, "Created data+metadata buffer (portable): data_size=%zu, metadata_size=%zu, total=%zu", 
             valuelen, metadata_size, total_size);
    return buffer;
}

/* Add data with metadata using single addReplyBulkCBuffer call */
void addReplyBulkCBufferWithMetadata(client *c, const char *value, size_t valuelen, const char *key, size_t keylen) {
    size_t total_len;
    char *buffer = createDataWithMetadataBuffer(value, valuelen, key, keylen, &total_len);
    
    if (buffer == NULL) {
        serverLog(LL_WARNING, "Cannot create buffer with metadata for key '%.*s'", (int)keylen, key);
        /* Fallback to regular response without metadata */
        addReplyBulkCBuffer(c, value, valuelen);
        return;
    }
    
    /* Send the combined buffer as a single response */
    addReplyBulkCBuffer(c, buffer, total_len);
    
    /* Free the buffer */
    zfree(buffer);
    
    serverLog(LL_DEBUG, "Sent data+metadata buffer for key '%.*s': total_size=%zu", (int)keylen, key, total_len);
}

// Helper functions for little-endian deserialization
static uint16_t read_le16(const char *buf) {
    return (uint16_t)buf[0] | ((uint16_t)buf[1] << 8);
}
static uint32_t read_le32(const char *buf) {
    return (uint32_t)buf[0] | ((uint32_t)buf[1] << 8) | ((uint32_t)buf[2] << 16) | ((uint32_t)buf[3] << 24);
}

migrationMetadata* extractMetadataFromBuffer(const char *buffer, size_t buffer_len, size_t data_len) {
    size_t metadata_size = 50; // 2+2+46+2 (slot_id + migration_status + host + port)
    if (buffer_len < data_len + metadata_size) {
        return NULL;
    }
    const char *meta = buffer + data_len;
    migrationMetadata *metadata = zmalloc(sizeof(migrationMetadata));
    metadata->slot_id = read_le16(meta);
    metadata->migration_status = read_le16(meta + 2);
    memcpy(metadata->host, meta + 4, MAX_HOST_LEN);
    metadata->port = read_le16(meta + 4 + MAX_HOST_LEN);
    return metadata;
}

/* Get only the data part from a buffer that contains data + metadata */
char* extractDataFromBuffer(const char *buffer, size_t buffer_len, size_t *data_len) {
    size_t metadata_size = sizeof(migrationMetadata);
    
    if (buffer_len <= metadata_size) {
        /* Buffer too small to contain metadata */
        *data_len = buffer_len;
        char *data = zmalloc(buffer_len);
        memcpy(data, buffer, buffer_len);
        return data;
    }
    
    /* Calculate data length (everything except metadata) */
    *data_len = buffer_len - metadata_size;
    
    /* Extract data */
    char *data = zmalloc(*data_len);
    memcpy(data, buffer, *data_len);
    
    return data;
}
