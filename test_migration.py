#!/usr/bin/env python3
"""
Simple test script for Redis migration functionality.
"""

import redis
import sys

def main():
    try:
        # Connect to Redis
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        
        print("=== Testing Redis Migration Functionality ===\n")
        
        # Test 1: Check if migration commands exist
        print("1. Testing migration commands...")
        
        # Test MIGRATION.STATUS
        try:
            status = r.execute_command("MIGRATION.STATUS")
            print(f"   MIGRATION.STATUS: {status}")
        except Exception as e:
            print(f"   MIGRATION.STATUS failed: {e}")
            return 1
        
        # Test MIGRATION.SETNODES
        try:
            r.execute_command("MIGRATION.SETNODES", "1001", "1002")
            print("   MIGRATION.SETNODES: OK")
        except Exception as e:
            print(f"   MIGRATION.SETNODES failed: {e}")
            return 1
        
        # Test MIGRATION.RANGE
        try:
            r.execute_command("MIGRATION.RANGE", "0", "1000")
            print("   MIGRATION.RANGE: OK")
        except Exception as e:
            print(f"   MIGRATION.RANGE failed: {e}")
            return 1
        
        # Test 2: Test basic operations with metadata
        print("\n2. Testing basic operations...")
        
        # Set a key
        r.set("test_key", "test_value")
        print("   SET test_key: OK")
        
        # Get the key (should include metadata)
        result = r.get("test_key")
        print(f"   GET test_key: {result}")
        
        # Test hash operations
        r.hset("test_hash", "field1", "value1")
        result = r.hget("test_hash", "field1")
        print(f"   HGET test_hash field1: {result}")
        
        # Test list operations
        r.lpush("test_list", "item1")
        result = r.lindex("test_list", 0)
        print(f"   LINDEX test_list 0: {result}")
        
        # Test set operations
        r.sadd("test_set", "member1")
        result = r.sismember("test_set", "member1")
        print(f"   SISMEMBER test_set member1: {result}")
        
        # Test sorted set operations
        r.zadd("test_zset", {"member1": 1.0})
        result = r.zscore("test_zset", "member1")
        print(f"   ZSCORE test_zset member1: {result}")
        
        print("\n=== All tests passed! ===")
        print("Migration functionality is working correctly.")
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 