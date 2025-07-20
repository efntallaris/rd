# Redis Cluster Migration Metadata Implementation

## Overview

This implementation provides migration metadata for Redis read operations using Redis's existing cluster migration data structures (`migrating_slots_to` and `importing_slots_from`) instead of creating a separate migration context.

## Key Changes

### 1. **Removed Custom Migration Context**
- Eliminated `migrationContext` structure
- Removed `initMigrationContext()` and `freeMigrationContext()` functions
- No longer maintains separate migration state

### 2. **Uses Redis Cluster Migration State**
- Leverages `server.cluster->migrating_slots_to[slot]` array
- Leverages `server.cluster->importing_slots_from[slot]` array
- Integrates with existing Redis cluster migration system

### 3. **Updated Data Structures**

#### Migration Metadata
```c
typedef struct migrationMetadata {
    uint32_t slot_id;           // Hash slot ID
    uint32_t source_id;         // Source node configEpoch
    uint32_t dest_id;           // Destination node configEpoch  
    uint32_t migration_status;  // Migration status constant
} migrationMetadata;
```

#### Migration Status Constants
```c
#define MIGRATION_STATUS_NOT_MIGRATED 0
#define MIGRATION_STATUS_IN_PROGRESS  1
#define MIGRATION_STATUS_MIGRATED     2
```

## How It Works

### 1. **Slot Detection**
The system checks if a key's hash slot is in migration state:

```c
uint32_t slot = keyHashSlot(key, keylen);

// Check if migrating to another node
if (server.cluster->migrating_slots_to[slot] != NULL) {
    // Slot is being migrated to another node
}

// Check if importing from another node  
if (server.cluster->importing_slots_from[slot] != NULL) {
    // Slot is being imported from another node
}
```

### 2. **Metadata Generation**
Metadata is generated based on cluster state:

- **Migrating slots**: `source_id` = current node's configEpoch, `dest_id` = target node's configEpoch
- **Importing slots**: `source_id` = source node's configEpoch, `dest_id` = current node's configEpoch
- **Stable slots**: `source_id` = current node's configEpoch, `dest_id` = 0

### 3. **Response Enhancement**
Metadata is added to all read responses as RESP3 attributes:

```c
addReplyAttributeLen(c, 2);
addReplyProto(c, "slot_id", 7);
addReplyLongLong(c, metadata->slot_id);
addReplyProto(c, "migration_status", 16);
addReplyLongLong(c, metadata->migration_status);
addReplyProto(c, "source_id", 9);
addReplyLongLong(c, metadata->source_id);
addReplyProto(c, "dest_id", 7);
addReplyLongLong(c, metadata->dest_id);
```

## Commands

### `MIGRATION.STATUS`
Returns migration status information:
```
[number_of_migrating_slots, number_of_importing_slots, current_epoch]
```

### `MIGRATION.SLOTINFO <slot>`
Returns detailed information about a specific slot:
```
[status, node_name, config_epoch]
```
Where `status` can be:
- `"migrating"` - slot is being migrated to another node
- `"importing"` - slot is being imported from another node  
- `"stable"` - slot is not in migration

## Integration with Redis Cluster

### Setting Up Migration Slots
Use Redis cluster commands to set up migration:

```bash
# Mark slot 1000 as migrating to node abc123
CLUSTER SETSLOT 1000 MIGRATING abc123

# Mark slot 1000 as importing from node def456  
CLUSTER SETSLOT 1000 IMPORTING def456
```

### Reading Keys with Metadata
When you read keys in migrating/importing slots, metadata is automatically added:

```bash
# Set a key in slot 1000
SET key1000 value

# Get the key (metadata will be included in RESP3)
GET key1000
```

## Benefits

1. **No Duplicate State**: Uses Redis's existing cluster migration state
2. **Automatic Integration**: Works with existing cluster migration commands
3. **Consistent State**: Migration state is managed by Redis cluster system
4. **Simplified Code**: No need to maintain separate migration context
5. **Real-time Updates**: Metadata reflects current cluster state

## Testing

Run the test script to verify functionality:

```bash
python3 test_cluster_migration.py
```

## Logging

The implementation includes comprehensive debug logging:

- Key-to-slot mapping
- Migration state detection
- Metadata generation
- Response enhancement

To see logs, set Redis log level to warning or debug:

```bash
redis-cli CONFIG SET loglevel warning
```

## Files Modified

- `src/migration.c` - Complete rewrite using cluster state
- `src/migration.h` - Updated structures and function declarations
- `src/server.c` - Updated command registrations
- `test_cluster_migration.py` - Test script for verification 