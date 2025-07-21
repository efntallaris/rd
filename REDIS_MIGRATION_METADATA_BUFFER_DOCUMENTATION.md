# Redis Migration Metadata Buffer Documentation

## Table of Contents
1. [Overview](#overview)
2. [Buffer Structure](#buffer-structure)
3. [Client-Side Extraction Procedure](#client-side-extraction-procedure)
4. [Lettuce Client Implementation](#lettuce-client-implementation)
5. [Network Protocol Considerations](#network-protocol-considerations)
6. [Migration-Aware Application Logic](#migration-aware-application-logic)
7. [Performance Considerations](#performance-considerations)
8. [Monitoring and Debugging](#monitoring-and-debugging)
9. [Testing Strategy](#testing-strategy)
10. [Backward Compatibility](#backward-compatibility)
11. [Security Considerations](#security-considerations)
12. [Implementation Examples](#implementation-examples)

## Overview

When Redis includes migration metadata in read responses, the data is sent as a single buffer with the original data followed by the metadata structure. This document explains how to extract and parse this information on the client side.

### Key Features
- **Space Efficient**: Uses optimized data types (uint16_t for slot_id and migration_status)
- **Single Buffer**: Data and metadata combined in one allocation
- **Backward Compatible**: Works with existing Redis clients
- **Migration Aware**: Provides real-time migration status information

## Buffer Structure

The buffer follows this layout:
```
[Original Data (N bytes)] [Metadata Structure (12 bytes)]
```

### Metadata Structure Layout (12 bytes total)
```
Offset 0-1:   slot_id (uint16_t) - Hash slot number (0-16383)
Offset 2-3:   migration_status (uint16_t) - Migration status (0-2)
Offset 4-7:   source_id (uint32_t) - Source node configEpoch
Offset 8-11:  dest_id (uint32_t) - Destination node configEpoch
```

### Migration Status Values
- **0**: NOT_MIGRATED - Key is not in migration
- **1**: IN_PROGRESS - Key is currently being migrated
- **2**: MIGRATED - Key has been migrated

### Space Optimization
- **Original structure**: 16 bytes (uint32_t for all fields)
- **Optimized structure**: 12 bytes (uint16_t for slot_id and migration_status)
- **Space savings**: 4 bytes (25% reduction) per metadata structure

## Client-Side Extraction Procedure

### Step 1: Determine Buffer Boundaries
1. **Receive the complete buffer** from Redis
2. **Calculate data length**: `data_length = total_buffer_length - 12`
3. **Validate buffer size**: Ensure buffer is at least 12 bytes (metadata size)

### Step 2: Extract Original Data
1. **Copy first N bytes** where N = `total_buffer_length - 12`
2. **This is your original Redis value** (string, hash field, etc.)
3. **Process this data normally** as you would any Redis response

### Step 3: Extract Metadata
1. **Locate metadata start**: `buffer + data_length`
2. **Read slot_id**: First 2 bytes as little-endian uint16_t
3. **Read migration_status**: Next 2 bytes as little-endian uint16_t
4. **Read source_id**: Next 4 bytes as little-endian uint32_t
5. **Read dest_id**: Last 4 bytes as little-endian uint32_t

### Step 4: Interpret Migration Status
- **0**: NOT_MIGRATED - Key is not in migration
- **1**: IN_PROGRESS - Key is currently being migrated
- **2**: MIGRATED - Key has been migrated

## Lettuce Client Implementation

### Phase 1: Buffer Reception
1. **Intercept RESP3 bulk string responses**
2. **Check if response length > expected data length**
3. **If yes, metadata is present**; if no, process normally

### Phase 2: Data Extraction
1. **Calculate original data size**: `response_length - 12`
2. **Extract data portion**: First `data_length` bytes
3. **Convert to appropriate type** (String, byte array, etc.)

### Phase 3: Metadata Parsing
1. **Create metadata object** with 4 fields
2. **Parse binary data** from last 12 bytes
3. **Handle endianness** (Redis uses little-endian)
4. **Validate slot_id range** (0-16383)

### Phase 4: Response Assembly
1. **Create response wrapper** containing both data and metadata
2. **Expose metadata fields** through getter methods
3. **Maintain backward compatibility** for existing code

## Network Protocol Considerations

### RESP3 Protocol
- **Bulk strings** are prefixed with `$<length>\r\n`
- **Total length** includes both data and metadata
- **No special framing** - metadata is just appended

### Error Handling
1. **Buffer too small**: Return original data without metadata
2. **Invalid slot_id**: Log warning, continue processing
3. **Corrupted metadata**: Fall back to data-only parsing

## Migration-Aware Application Logic

### Decision Points
1. **Check migration_status** after each read operation
2. **If status = 1 (IN_PROGRESS)**: Consider double-read strategy
3. **If status = 2 (MIGRATED)**: Key is fully migrated
4. **If status = 0 (NOT_MIGRATED)**: Normal operation

### Double-Read Implementation
1. **Read from destination node first**
2. **If null/empty, read from source node**
3. **Return most recent value**
4. **Log migration events** for monitoring

### Migration Status Logic
1. **NOT_MIGRATED (0)**: No migration active
2. **MIGRATED (1)**: Key's slot is in migration range
3. **IN_PROGRESS (2)**: Migration active but key's slot not yet migrated

## Performance Considerations

### Memory Management
1. **Avoid double allocation** - extract in place when possible
2. **Reuse metadata objects** for similar operations
3. **Pool buffer objects** for high-frequency operations

### Caching Strategy
1. **Cache migration status** per slot to avoid repeated parsing
2. **Invalidate cache** when migration status changes
3. **Use TTL-based expiration** for migration metadata

### Network Impact
- **RESP3 Attributes**: Minimal additional bytes
- **RESP2 Compatibility**: Metadata not included (maintains compatibility)
- **Single Buffer**: Reduces network overhead compared to separate calls

## Monitoring and Debugging

### Logging Requirements
1. **Log metadata extraction** at DEBUG level
2. **Log migration status changes** at INFO level
3. **Log parsing errors** at WARN level

### Metrics Collection
1. **Track metadata presence** in responses
2. **Monitor migration status distribution**
3. **Measure extraction performance** overhead

### Debug Information
- **Buffer sizes**: Log data and metadata sizes
- **Migration events**: Track status transitions
- **Performance metrics**: Monitor parsing overhead

## Testing Strategy

### Unit Tests
1. **Test buffer parsing** with known data/metadata combinations
2. **Test edge cases**: empty data, maximum slot_id, etc.
3. **Test error conditions**: corrupted buffers, wrong sizes

### Integration Tests
1. **Test with actual Redis cluster** in migration state
2. **Verify metadata accuracy** during live migrations
3. **Test client behavior** during migration transitions

### Test Scenarios
- **No migration**: Verify normal operation
- **Migration in progress**: Test double-read logic
- **Migration completed**: Verify status updates
- **Error conditions**: Test fallback behavior

## Backward Compatibility

### RESP2 Protocol
1. **No metadata included** - normal Redis behavior
2. **No parsing required** - existing code works unchanged
3. **Graceful degradation** when metadata not available

### Feature Detection
1. **Check Redis version** for metadata support
2. **Test buffer size** to detect metadata presence
3. **Fall back gracefully** when metadata unavailable

### Client Compatibility
- **Existing clients**: Continue to work normally
- **New clients**: Can opt-in to metadata features
- **Hybrid deployments**: Support both old and new clients

## Security Considerations

### Input Validation
1. **Validate slot_id range** (0-16383)
2. **Check buffer boundaries** to prevent overflow
3. **Sanitize metadata** before logging

### Access Control
1. **Metadata contains cluster information** - handle appropriately
2. **Log metadata carefully** - may contain sensitive cluster details
3. **Consider encryption** for metadata in transit if needed

### Data Integrity
- **Verify buffer integrity** before parsing
- **Handle corrupted metadata** gracefully
- **Validate migration status** values

## Implementation Examples

### Redis Server Functions
```c
// Create buffer with metadata appended
char* createDataWithMetadataBuffer(const char *value, size_t valuelen, 
                                  const char *key, size_t keylen, size_t *total_len);

// Send buffer with metadata
void addReplyBulkCBufferWithMetadata(client *c, const char *value, size_t valuelen, 
                                    const char *key, size_t keylen);

// Extract metadata from buffer
migrationMetadata* extractMetadataFromBuffer(const char *buffer, size_t buffer_len, 
                                            size_t data_len);

// Extract data from buffer
char* extractDataFromBuffer(const char *buffer, size_t buffer_len, size_t *data_len);
```

### Usage Pattern
```c
// Old approach (separate calls)
addReplyBulkCBuffer(c, value, valuelen);
addMigrationMetadataToResponse(c, key, keylen);

// New approach (single buffer)
addReplyBulkCBufferWithMetadata(c, value, valuelen, key, keylen);
```

### Client-Side Parsing
1. **Receive buffer** from Redis
2. **Calculate data length**: `total_length - 12`
3. **Extract data**: First `data_length` bytes
4. **Extract metadata**: Last 12 bytes
5. **Parse metadata fields** according to structure layout

## Migration Scenarios

### Scenario 1: No Migration
```
GET mykey
"myvalue"
# Buffer: [myvalue][metadata: slot=1234, status=0, source=0, dest=0]
```

### Scenario 2: Migration Active, Key Migrated
```
GET mykey
"myvalue"
# Buffer: [myvalue][metadata: slot=1234, status=1, source=1001, dest=1002]
```

### Scenario 3: Migration Active, Key Not Migrated
```
GET mykey
"myvalue"
# Buffer: [myvalue][metadata: slot=5678, status=2, source=1001, dest=1002]
# Double read performed automatically
```

## Conclusion

This documentation provides a comprehensive guide for implementing Redis migration metadata buffer handling in client applications. The approach offers:

- **Space efficiency** through optimized data types
- **Performance benefits** from single buffer allocation
- **Backward compatibility** with existing Redis deployments
- **Migration awareness** for intelligent client behavior

The implementation enables clients to make informed decisions about data access patterns during Redis cluster migrations, improving consistency and performance in distributed environments. 