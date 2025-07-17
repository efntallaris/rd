#!/usr/bin/env python3
"""
Redis Metadata Buffer Parser

This library provides functions to parse Redis response buffers that contain
both metadata and data when the read-response-metadata feature is enabled.

Buffer Format:
- Magic header "RDMT" (4 bytes)
- Total buffer size (4 bytes, network byte order)
- Metadata section size (4 bytes, network byte order)
- Data section size (4 bytes, network byte order)
- Key length (4 bytes, network byte order)
- Field length (4 bytes, network byte order) [hash only]
- Data length (4 bytes, network byte order)
- Access timestamp (8 bytes, network byte order)
- Keyspace hits (8 bytes, network byte order)
- Keyspace misses (8 bytes, network byte order)
- Data type (1 byte: 0=string, 1=hash, 2=list, etc.)
- Key string (variable length)
- Field string (variable length) [hash only]
- Actual data (variable length)
"""

import struct
import time
from typing import Dict, Any, Optional, Tuple, Union

class RedisMetadata:
    """Represents metadata from a Redis read operation."""
    
    def __init__(self):
        self.key: str = ""
        self.field: Optional[str] = None
        self.data_type: int = 0
        self.data_size: int = 0
        self.access_timestamp: int = 0
        self.keyspace_hits: int = 0
        self.keyspace_misses: int = 0
        self.total_size: int = 0
        self.metadata_size: int = 0

    @property
    def data_type_name(self) -> str:
        """Get human-readable data type name."""
        types = {
            0: "string",
            1: "hash", 
            2: "list",
            3: "set",
            4: "zset",
            5: "stream"
        }
        return types.get(self.data_type, "unknown")
    
    @property
    def access_time_formatted(self) -> str:
        """Get formatted access time."""
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.access_timestamp))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary."""
        result = {
            "key": self.key,
            "data_type": self.data_type_name,
            "data_size": self.data_size,
            "access_timestamp": self.access_timestamp,
            "access_time": self.access_time_formatted,
            "keyspace_hits": self.keyspace_hits,
            "keyspace_misses": self.keyspace_misses,
            "total_size": self.total_size,
            "metadata_size": self.metadata_size
        }
        if self.field is not None:
            result["field"] = self.field
        return result

class RedisResponse:
    """Represents a complete Redis response with metadata and data."""
    
    def __init__(self, data: bytes, metadata: RedisMetadata):
        self.data = data
        self.metadata = metadata
    
    @property
    def data_str(self) -> str:
        """Get data as UTF-8 string."""
        return self.data.decode('utf-8', errors='replace')
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert entire response to dictionary."""
        return {
            "data": self.data_str,
            "metadata": self.metadata.to_dict()
        }

def parse_redis_metadata_buffer(buffer: bytes) -> Union[RedisResponse, bytes]:
    """
    Parse a Redis response buffer that may contain metadata.
    
    Args:
        buffer: Raw bytes from Redis response
        
    Returns:
        RedisResponse object if metadata is present, otherwise raw bytes
        
    Raises:
        ValueError: If buffer format is invalid
    """
    if len(buffer) < 4:
        return buffer  # Too small to contain metadata
    
    # Check for magic header
    if buffer[:4] != b"RDMT":
        return buffer  # Not a metadata buffer, return raw data
    
    if len(buffer) < 20:  # Minimum size for header
        raise ValueError("Invalid metadata buffer: too small")
    
    try:
        # Parse header
        magic = buffer[0:4]
        total_size = struct.unpack("!I", buffer[4:8])[0]
        metadata_size = struct.unpack("!I", buffer[8:12])[0]
        data_size = struct.unpack("!I", buffer[12:16])[0]
        
        if total_size != len(buffer):
            raise ValueError(f"Buffer size mismatch: expected {total_size}, got {len(buffer)}")
        
        offset = 16
        
        # Parse key length and field length (if hash)
        key_len = struct.unpack("!I", buffer[offset:offset+4])[0]
        offset += 4
        
        # Check data type first to determine if we have field length
        # We need to peek ahead to get the data type
        if offset + 12 > len(buffer):  # Need at least 8 more bytes for other fields + 1 for type
            raise ValueError("Buffer truncated")
        
        # For now, assume we might have field length (hash format)
        # We'll determine this by checking remaining space
        remaining_before_strings = metadata_size - offset
        min_remaining = 4 + 8 + 8 + 8 + 1  # data_len + timestamp + hits + misses + type
        
        field_len = 0
        if remaining_before_strings >= min_remaining + 4:  # Extra 4 bytes suggests field length
            field_len = struct.unpack("!I", buffer[offset:offset+4])[0]
            offset += 4
        
        # Parse data length
        data_len = struct.unpack("!I", buffer[offset:offset+4])[0]
        offset += 4
        
        if data_len != data_size:
            raise ValueError(f"Data size mismatch: header says {data_size}, field says {data_len}")
        
        # Parse timestamp and stats
        access_timestamp = struct.unpack("!Q", buffer[offset:offset+8])[0]
        offset += 8
        
        keyspace_hits = struct.unpack("!Q", buffer[offset:offset+8])[0]
        offset += 8
        
        keyspace_misses = struct.unpack("!Q", buffer[offset:offset+8])[0]
        offset += 8
        
        # Parse data type
        data_type = struct.unpack("!B", buffer[offset:offset+1])[0]
        offset += 1
        
        # Parse key string
        if offset + key_len > len(buffer):
            raise ValueError("Buffer truncated while reading key")
        key = buffer[offset:offset+key_len].decode('utf-8')
        offset += key_len
        
        # Parse field string (if hash)
        field = None
        if field_len > 0:
            if offset + field_len > len(buffer):
                raise ValueError("Buffer truncated while reading field")
            field = buffer[offset:offset+field_len].decode('utf-8')
            offset += field_len
        
        # Parse actual data
        if offset + data_len > len(buffer):
            raise ValueError("Buffer truncated while reading data")
        data = buffer[offset:offset+data_len]
        
        # Create metadata object
        metadata = RedisMetadata()
        metadata.key = key
        metadata.field = field
        metadata.data_type = data_type
        metadata.data_size = data_len
        metadata.access_timestamp = access_timestamp
        metadata.keyspace_hits = keyspace_hits
        metadata.keyspace_misses = keyspace_misses
        metadata.total_size = total_size
        metadata.metadata_size = metadata_size
        
        return RedisResponse(data, metadata)
        
    except struct.error as e:
        raise ValueError(f"Invalid buffer format: {e}")
    except UnicodeDecodeError as e:
        raise ValueError(f"Invalid string encoding: {e}")

def redis_get_with_metadata(redis_client, key: str) -> Union[RedisResponse, str]:
    """
    Helper function to perform GET with automatic metadata parsing.
    
    Args:
        redis_client: Redis client instance
        key: Key to retrieve
        
    Returns:
        RedisResponse if metadata enabled, string value otherwise
    """
    result = redis_client.get(key)
    
    if isinstance(result, bytes):
        parsed = parse_redis_metadata_buffer(result)
        if isinstance(parsed, RedisResponse):
            return parsed
    
    return result

def redis_hget_with_metadata(redis_client, key: str, field: str) -> Union[RedisResponse, str]:
    """
    Helper function to perform HGET with automatic metadata parsing.
    
    Args:
        redis_client: Redis client instance
        key: Hash key
        field: Hash field
        
    Returns:
        RedisResponse if metadata enabled, string value otherwise
    """
    result = redis_client.hget(key, field)
    
    if isinstance(result, bytes):
        parsed = parse_redis_metadata_buffer(result)
        if isinstance(parsed, RedisResponse):
            return parsed
    
    return result

# Example usage
if __name__ == "__main__":
    import redis
    
    # Example buffer parsing
    print("=== Redis Metadata Buffer Parser Example ===\n")
    
    try:
        # Connect to Redis
        r = redis.Redis(host='localhost', port=6379)
        
        print("1. Testing without metadata:")
        r.set("test_key", "test_value")
        result = redis_get_with_metadata(r, "test_key")
        print(f"Result: {result}")
        print(f"Type: {type(result)}\n")
        
        print("2. Enabling metadata:")
        r.config_set("read-response-metadata", "yes")
        
        print("3. Testing with metadata:")
        result = redis_get_with_metadata(r, "test_key")
        print(f"Result type: {type(result)}")
        if isinstance(result, RedisResponse):
            print(f"Data: {result.data_str}")
            print(f"Metadata: {result.metadata.to_dict()}")
        else:
            print(f"Raw result: {result}")
        
        print("\n4. Testing hash with metadata:")
        r.hset("test_hash", "field1", "value1")
        result = redis_hget_with_metadata(r, "test_hash", "field1")
        if isinstance(result, RedisResponse):
            print(f"Data: {result.data_str}")
            print(f"Metadata: {result.metadata.to_dict()}")
        else:
            print(f"Raw result: {result}")
            
    except Exception as e:
        print(f"Error: {e}")
        
    print("\n=== Done ===") 