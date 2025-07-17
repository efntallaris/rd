#!/usr/bin/env python3
"""
Test script to demonstrate Redis read response metadata functionality.
"""

import redis
import json

def test_redis_metadata():
    # Connect to Redis
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    print("=== Redis Read Response Metadata Test ===\n")
    
    # First, test without metadata (default behavior)
    print("1. Testing without metadata (default):")
    r.set("testkey", "testvalue")
    result = r.get("testkey")
    print(f"GET testkey: {result}")
    print(f"Type: {type(result)}\n")
    
    # Enable metadata
    print("2. Enabling read response metadata...")
    r.config_set("read-response-metadata", "yes")
    print("Metadata enabled\n")
    
    # Test with metadata
    print("3. Testing with metadata enabled:")
    result = r.get("testkey")
    print(f"GET testkey: {result}")
    print(f"Type: {type(result)}\n")
    
    # Test hash commands with metadata
    print("4. Testing hash commands with metadata:")
    r.hset("testhash", "field1", "value1")
    result = r.hget("testhash", "field1")
    print(f"HGET testhash field1: {result}")
    print(f"Type: {type(result)}\n")
    
    # Disable metadata
    print("5. Disabling metadata...")
    r.config_set("read-response-metadata", "no")
    result = r.get("testkey")
    print(f"GET testkey (metadata disabled): {result}")
    print(f"Type: {type(result)}\n")

if __name__ == "__main__":
    test_redis_metadata() 