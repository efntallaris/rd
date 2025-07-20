# Redis Migration Metadata - Client Implementation Documentation

## RESP3 Response Structure

When Redis includes migration metadata in read responses, the payload follows this RESP3 protocol structure:

```
*2\r\n           # Array with 2 elements: [attributes, value]
%4\r\n           # Map with 4 key-value pairs (attributes)
$7\r\nslot_id\r\n:1000\r\n
$16\r\nmigration_status\r\n:1\r\n
$9\r\nsource_id\r\n:12345\r\n
$7\r\ndest_id\r\n:67890\r\n
$9\r\ntestvalue\r\n  # The actual value
```

## Response Breakdown

| Element | Type | Description |
|---------|------|-------------|
| `*2` | Array | Array with 2 elements |
| `%4` | Map | Map with 4 attribute entries |
| `slot_id` | String | Attribute key for hash slot |
| `:1000` | Integer | Hash slot number (0-16383) |
| `migration_status` | String | Attribute key for migration status |
| `:1` | Integer | Status: 0=NOT_MIGRATED, 1=IN_PROGRESS, 2=MIGRATED |
| `source_id` | String | Attribute key for source node |
| `:12345` | Integer | Source node configEpoch |
| `dest_id` | String | Attribute key for destination node |
| `:67890` | Integer | Destination node configEpoch |
| `testvalue` | String | The actual data value |

## Metadata Fields

### `slot_id`
- **Type**: Integer
- **Range**: 0-16383
- **Description**: Hash slot number for the key

### `migration_status`
- **Type**: Integer
- **Values**:
  - `0`: NOT_MIGRATED - Key is not in migration
  - `1`: IN_PROGRESS - Key is currently being migrated
  - `2`: MIGRATED - Key has been migrated

### `source_id`
- **Type**: Integer
- **Description**: Source node's configEpoch (when migrating from)

### `dest_id`
- **Type**: Integer
- **Description**: Destination node's configEpoch (when migrating to)

## Client Implementation Requirements

### 1. RESP3 Protocol
- Client must use RESP3 protocol (`HELLO 3` command)
- Metadata is only available in RESP3 responses

### 2. Parser Logic
```pseudocode
function parseResponse(buffer):
    if buffer.startsWith("*2"):
        // Array with 2 elements
        if nextElement.startsWith("%4"):
            // Parse attributes map
            metadata = parseAttributesMap(buffer)
            value = parseValue(buffer)
            return {metadata, value}
        else:
            // Normal array response
            return parseArray(buffer)
    else:
        // Normal response
        return parseValue(buffer)

function parseAttributesMap(buffer):
    // Parse 4 key-value pairs
    slot_id = parseInteger(buffer)
    migration_status = parseInteger(buffer)
    source_id = parseInteger(buffer)
    dest_id = parseInteger(buffer)
    return {slot_id, migration_status, source_id, dest_id}
```

### 3. Data Structures
```typescript
interface MigrationMetadata {
    slotId: number;           // 0-16383
    migrationStatus: number;  // 0, 1, or 2
    sourceId: number;         // Source node configEpoch
    destId: number;           // Destination node configEpoch
}

interface ResponseWithMetadata<T> {
    value: T;
    metadata: MigrationMetadata | null;
}
```

## Usage Examples

### Basic GET with Metadata
```typescript
// Client sends: GET testkey
// Redis responds with metadata
const response = await client.get("testkey");
// response = {
//   value: "testvalue",
//   metadata: {
//     slotId: 1000,
//     migrationStatus: 1,
//     sourceId: 12345,
//     destId: 67890
//   }
// }
```

### Migration-Aware Logic
```typescript
function handleRead(key: string, response: ResponseWithMetadata<string>) {
    if (response.metadata?.migrationStatus === 1) {
        // Key is in migration - implement double read logic
        console.log(`Key ${key} is migrating from slot ${response.metadata.slotId}`);
        return performDoubleRead(key);
    }
    return response.value;
}
```

## Commands That Include Metadata

All read commands automatically include metadata when the key is in a migrating/importing slot:

- `GET key`
- `HGET key field`
- `HGETALL key`
- `LINDEX key index`
- `SISMEMBER key member`
- `ZSCORE key member`
- `MGET key1 key2 key3`

## Migration Status Constants

```typescript
enum MigrationStatus {
    NOT_MIGRATED = 0,
    IN_PROGRESS = 1,
    MIGRATED = 2
}
```

## Error Handling

- If cluster is not enabled, metadata will be `null`
- If key is not in migration, metadata will be `null`
- RESP3 protocol must be enabled to receive metadata
- Fall back to normal parsing if attributes are not present

## Testing

To test metadata functionality:

1. Start Redis with cluster mode
2. Set up migration slots: `CLUSTER SETSLOT 1000 MIGRATING node-id`
3. Read keys in migrating slots
4. Verify metadata is included in responses

The metadata allows clients to implement intelligent caching, double reads, and consistency handling during Redis cluster migrations. 