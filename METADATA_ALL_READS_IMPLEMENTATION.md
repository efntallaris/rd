# Redis Metadata on All Read Requests Implementation

This implementation adds metadata to **all** Redis read command responses, providing comprehensive migration tracking and double read functionality across all data types.

## Overview

The implementation extends the double read mechanism to include metadata on **every read request** in Redis, not just specific commands. This provides:

1. **Global metadata tracking** for all read operations
2. **Automatic double read detection** based on migration ranges
3. **Consistent metadata format** across all data types
4. **Always-on metadata inclusion** (no configuration needed)

## Key Features

### 📊 **Comprehensive Coverage**
- **All Data Types**: String, Hash, List, Set, Sorted Set
- **All Read Commands**: GET, HGET, LINDEX, SISMEMBER, ZSCORE, etc.
- **Consistent Format**: Same metadata structure across all commands

### 🔄 **Migration Integration**
- **Automatic Detection**: Determines if double read is needed
- **Status Tracking**: Shows migration progress per key
- **Slot-based Logic**: Uses hash slots for migration range checking

## Implementation Details

### Core Components

#### 1. **Migration Context**
```c
typedef struct migrationContext {
    list *migration_ranges;     /* List of migration completion ranges */
    migrationMetadata *metadata; /* Current migration metadata */
    uint32_t source_node_id;    /* Source node ID */
    uint32_t dest_node_id;      /* Destination node ID */
    int migration_active;       /* Whether migration is active */
    time_t migration_start;     /* When migration started */
} migrationContext;
```

#### 2. **Global Metadata Function**
```c
void addMetadataToAllReadResponses(client *c, const char *key, size_t keylen, const char *field);
```

This function is called by **all read commands** and:
- Determines if double read should be performed
- Adds migration metadata to responses
- Handles field-specific metadata for hash operations

#### 3. **Migration Commands**
```bash
# Set up migration nodes
MIGRATION.SETNODES <source_id> <dest_id>

# Add migration ranges
MIGRATION.RANGE <start_slot> <end_slot>

# Check migration status
MIGRATION.STATUS
```

### Modified Commands

The following read commands now include metadata:

#### **String Commands**
- `GET` - Basic string retrieval
- `GETEX` - Get with expiration options
- `GETDEL` - Get and delete
- `GETRANGE` - Get substring
- `STRLEN` - Get string length

#### **Hash Commands**
- `HGET` - Get hash field value
- `HMGET` - Get multiple hash fields
- `HGETALL` - Get all hash fields
- `HKEYS` - Get hash field names
- `HVALS` - Get hash field values
- `HEXISTS` - Check if field exists
- `HLEN` - Get hash length
- `HSTRLEN` - Get field string length

#### **List Commands**
- `LINDEX` - Get list element by index
- `LRANGE` - Get list range
- `LLEN` - Get list length
- `LPOS` - Find element position

#### **Set Commands**
- `SISMEMBER` - Check if member exists
- `SMISMEMBER` - Check multiple members
- `SCARD` - Get set cardinality
- `SMEMBERS` - Get all members
- `SRANDMEMBER` - Get random member

#### **Sorted Set Commands**
- `ZSCORE` - Get member score
- `ZMSCORE` - Get multiple member scores
- `ZCARD` - Get sorted set cardinality
- `ZRANK` - Get member rank
- `ZREVRANK` - Get member reverse rank
- `ZRANGE` - Get range of members
- `ZCOUNT` - Count members in score range

## Response Format

### Standard Response (No Migration)
```
GET mykey
"myvalue"
# Additional metadata attributes (RESP3)
>3
slot_id:1234
migration_status:0
source_id:0
dest_id:0
```

### Response with Migration Active
```
GET mykey
"myvalue"
# Additional metadata attributes (RESP3)
>3
slot_id:1234
migration_status:2
source_id:1001
dest_id:1002
```

### Hash Response with Metadata
```
HGET myhash field1
"fieldvalue"
# Additional metadata attributes (RESP3)
>3
slot_id:5678
migration_status:1
source_id:1001
dest_id:1002
```

## Metadata Fields

### Common Fields (All Data Types)
- **slot_id**: Hash slot ID (0-16383)
- **migration_status**: 
  - `0` = Not migrated
  - `1` = Migrated
  - `2` = In progress (triggers double read)
- **source_id**: Source node ID
- **dest_id**: Destination node ID

### Migration Status Logic
1. **Not Migrated (0)**: No migration active
2. **Migrated (1)**: Key's slot is in migration range
3. **In Progress (2)**: Migration active but key's slot not yet migrated

## Double Read Logic

### When Double Read is Performed
- Migration is active
- Key's hash slot is **NOT** in migration completion ranges

### Double Read Process
1. **Check Migration Range**: Determine if key is in migrated slots
2. **Read from Destination**: Try to get value from destination first
3. **Fallback to Source**: If destination returns null, read from source
4. **Return Latest**: Return the most recent value
5. **Add Metadata**: Include migration information in response

### Performance Considerations
- **Smart Detection**: Only performs double reads when necessary
- **Caching**: Migration ranges are cached for fast lookups

## Usage Examples

### Basic Setup
```bash
# Set up migration nodes
MIGRATION.SETNODES 1001 1002

# Add migration ranges
MIGRATION.RANGE 0 1000
```

### Testing Different Data Types
```bash
# String operations
SET string_key "value"
GET string_key  # Includes metadata

# Hash operations  
HSET hash_key field1 "value1"
HGET hash_key field1  # Includes metadata

# List operations
LPUSH list_key "item1"
LINDEX list_key 0  # Includes metadata

# Set operations
SADD set_key "member1"
SISMEMBER set_key member1  # Includes metadata

# Sorted set operations
ZADD zset_key 1.0 "member1"
ZSCORE zset_key member1  # Includes metadata
```

### Monitoring Migration Progress
```bash
# Check migration status
MIGRATION.STATUS

# Response: [active, source_id, dest_id, range_count]
# Example: [1, 1001, 1002, 3]
```

## Configuration

### Default Settings
- **Migration Active**: `false` (no migration by default)
- **Source Node**: `0` (not set)
- **Destination Node**: `0` (not set)

## Performance Impact

### Minimal Overhead
- **Always Active**: Metadata included in all read responses
- **Efficient Lookups**: Hash slot calculation is fast
- **Cached Ranges**: Migration ranges are cached in memory

### Memory Usage
- **Migration Context**: ~100 bytes per instance
- **Migration Ranges**: ~24 bytes per range
- **Metadata**: ~16 bytes per response

### Network Impact
- **RESP3 Attributes**: Minimal additional bytes
- **RESP2 Compatibility**: Metadata not included (maintains compatibility)

## Migration Scenarios

### Scenario 1: No Migration
```
GET mykey
"myvalue"
# Metadata: slot_id:1234, migration_status:0, source_id:0, dest_id:0
```

### Scenario 2: Migration Active, Key Migrated
```
GET mykey
"myvalue"
# Metadata: slot_id:1234, migration_status:1, source_id:1001, dest_id:1002
```

### Scenario 3: Migration Active, Key Not Migrated
```
GET mykey
"myvalue"
# Metadata: slot_id:5678, migration_status:2, source_id:1001, dest_id:1002
# Double read performed automatically
```

## Client Library Considerations

### Python (redis-py)
```python
import redis
r = redis.Redis()

# All read operations now include metadata
result = r.get("mykey")
# Result includes both data and metadata (in RESP3)
```

### Node.js (node_redis)
```javascript
const redis = require('redis');
const client = redis.createClient();

// All read operations now include metadata
const result = await client.get('mykey');
// Result includes both data and metadata (in RESP3)
```

## Testing

### Test Script
Run the comprehensive test script:
```bash
python3 test_metadata_all_reads.py
```

This script tests:
- All data types (string, hash, list, set, zset)
- Migration scenarios
- Double read functionality
- Response format validation

### Manual Testing
```bash
# Start Redis with the implementation
redis-server

# Connect and test
redis-cli

# Test various commands
SET test_key "test_value"
GET test_key

# Check for metadata in response
```

## Future Enhancements

### Planned Features
1. **RESP2 Metadata Support**: Add metadata support for RESP2 clients
2. **Custom Metadata Fields**: Allow custom metadata fields
3. **Performance Metrics**: Add timing information to metadata
4. **Compression**: Compress metadata for large responses

### Extensibility
The implementation is designed to be easily extensible:
- **New Metadata Fields**: Add fields to migrationMetadata structure
- **New Data Types**: Extend addMetadataToAllReadResponses for new types
- **Custom Logic**: Add custom double read logic per data type
- **Client Libraries**: Extend client libraries to parse metadata

## Conclusion

This implementation provides a comprehensive solution for adding metadata to all Redis read requests. It offers powerful migration tracking and double read capabilities with a consistent metadata format across all data types. The metadata is always included, providing complete visibility into migration status and data access patterns. 