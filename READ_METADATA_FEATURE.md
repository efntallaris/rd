# Redis Read Response Metadata Feature

This feature adds metadata to Redis read command responses, providing additional information about the data being retrieved.

## Overview

When enabled, Redis read commands (like GET, HGET, etc.) will return responses that include both the requested data and additional metadata about the operation.

## Configuration

### Enable/Disable Metadata

```bash
# Enable metadata responses
CONFIG SET read-response-metadata yes

# Disable metadata responses (default)
CONFIG SET read-response-metadata no

# Check current setting
CONFIG GET read-response-metadata
```

### Default Behavior

By default, metadata responses are **disabled** to maintain backward compatibility.

## Response Format

When metadata is enabled, read responses use a Redis map/hash format:

### Standard GET Response (metadata disabled)
```
GET mykey
"myvalue"
```

### GET Response with Metadata (metadata enabled)
```
GET mykey
{
  "data": "myvalue",
  "metadata": {
    "key": "mykey",
    "size": 7,
    "accessed_at": 1640995200,
    "type": "string"
  },
  "stats": {
    "keyspace_hits": 1542,
    "keyspace_misses": 23
  }
}
```

### HGET Response with Metadata
```
HGET myhash field1
{
  "data": "fieldvalue",
  "metadata": {
    "key": "myhash",
    "field": "field1",
    "size": 10,
    "accessed_at": 1640995200,
    "type": "hash"
  },
  "stats": {
    "keyspace_hits": 1543,
    "keyspace_misses": 23
  }
}
```

## Metadata Fields

### Common Fields (all data types)
- **key**: The Redis key that was accessed
- **size**: Size of the data in bytes
- **accessed_at**: Unix timestamp of when the data was accessed
- **type**: Data type (string, hash, list, set, zset, etc.)

### Hash-Specific Fields
- **field**: The hash field that was accessed

### Statistics Fields
- **keyspace_hits**: Total number of successful key lookups on this Redis instance
- **keyspace_misses**: Total number of failed key lookups on this Redis instance

## Supported Commands

The following read commands currently support metadata responses:

### String Commands
- GET
- GETEX (if implemented)
- GETDEL (if implemented)

### Hash Commands  
- HGET
- HMGET (planned)
- HGETALL (planned)

### Future Support
Additional commands can be extended to support metadata by modifying their response handlers.

## Client Library Considerations

### Python (redis-py)
When metadata is enabled, the response will be a dictionary instead of a simple string:

```python
import redis
r = redis.Redis()

# Enable metadata
r.config_set("read-response-metadata", "yes")

# GET will now return a dict
result = r.get("mykey")
print(result["data"])  # The actual value
print(result["metadata"]["size"])  # Size in bytes
```

### Node.js (node_redis)
```javascript
const redis = require('redis');
const client = redis.createClient();

// Enable metadata
await client.configSet('read-response-metadata', 'yes');

// GET will return an object
const result = await client.get('mykey');
console.log(result.data);  // The actual value
console.log(result.metadata.size);  // Size in bytes
```

## Performance Considerations

1. **Overhead**: Metadata responses include additional data, increasing response size
2. **Compatibility**: May break existing clients that expect simple string responses
3. **Network**: Increased bandwidth usage due to larger responses

## Use Cases

### Monitoring and Debugging
- Track data access patterns
- Monitor key sizes
- Debug cache hit/miss ratios

### Application Metrics
- Collect statistics about data usage
- Implement custom caching strategies
- Performance monitoring

### Development and Testing
- Understand data characteristics
- Debug Redis usage patterns
- Validate data integrity

## Migration Guide

### Enabling in Production
1. Test thoroughly in development environment
2. Update client applications to handle new response format
3. Enable feature gradually using feature flags
4. Monitor performance impact

### Client Code Updates
Before enabling metadata, ensure your client code can handle both response formats:

```python
def safe_get(redis_client, key):
    result = redis_client.get(key)
    
    # Handle both formats
    if isinstance(result, dict) and "data" in result:
        return result["data"]  # Metadata enabled
    else:
        return result  # Standard response
```

## Configuration Persistence

The `read-response-metadata` setting can be persisted in redis.conf:

```
# redis.conf
read-response-metadata yes
```

Or set at runtime and persisted with CONFIG REWRITE:

```bash
CONFIG SET read-response-metadata yes
CONFIG REWRITE
``` 