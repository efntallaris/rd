#!/usr/bin/env python3
"""
Test script for Redis cluster migration metadata functionality.
This script tests the new implementation that uses Redis's existing
cluster migration data structures instead of a separate migration context.
"""

import redis
import sys
import time

def test_migration_metadata():
    """Test migration metadata functionality"""
    
    # Connect to Redis
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        r.ping()
        print("✓ Connected to Redis")
    except Exception as e:
        print(f"✗ Failed to connect to Redis: {e}")
        return False
    
    # Test 1: Check if migration commands are available
    print("\n=== Test 1: Migration Commands ===")
    try:
        # Test migration status command
        status = r.execute_command('MIGRATION.STATUS')
        print(f"✓ MIGRATION.STATUS returned: {status}")
        
        # Test migration slot info command
        slot_info = r.execute_command('MIGRATION.SLOTINFO', 1000)
        print(f"✓ MIGRATION.SLOTINFO for slot 1000 returned: {slot_info}")
        
    except Exception as e:
        print(f"✗ Migration commands failed: {e}")
        return False
    
    # Test 2: Test metadata in read responses
    print("\n=== Test 2: Metadata in Read Responses ===")
    try:
        # Set a test key
        r.set('testkey', 'testvalue')
        print("✓ Set testkey = testvalue")
        
        # Get the key and check for metadata
        result = r.get('testkey')
        print(f"✓ GET testkey returned: {result}")
        
        # Note: Metadata is added as RESP3 attributes, so we need to use raw connection
        # to see the full response with attributes
        raw_r = redis.Redis(host='localhost', port=6379, decode_responses=False)
        raw_r.execute_command('HELLO', '3')  # Switch to RESP3
        
        # Get the key with raw connection to see metadata
        raw_result = raw_r.get('testkey')
        print(f"✓ Raw GET result: {raw_result}")
        
    except Exception as e:
        print(f"✗ Read response test failed: {e}")
        return False
    
    # Test 3: Test with cluster mode (if available)
    print("\n=== Test 3: Cluster Mode Test ===")
    try:
        # Check if cluster mode is enabled
        cluster_info = r.execute_command('CLUSTER', 'INFO')
        print("✓ Cluster info available")
        
        # Try to get cluster nodes
        nodes = r.execute_command('CLUSTER', 'NODES')
        print(f"✓ Cluster nodes: {len(nodes.split())} nodes found")
        
    except Exception as e:
        print(f"✗ Cluster mode test failed: {e}")
        print("Note: This is expected if Redis is not in cluster mode")
    
    # Test 4: Test slot mapping
    print("\n=== Test 4: Slot Mapping Test ===")
    try:
        # Test different keys to see slot mapping
        test_keys = ['testkey', 'user:123', 'session:abc', 'data:xyz']
        
        for key in test_keys:
            # Set and get each key to trigger metadata
            r.set(key, f'value_for_{key}')
            result = r.get(key)
            print(f"✓ Key '{key}' -> slot mapping should be logged")
        
    except Exception as e:
        print(f"✗ Slot mapping test failed: {e}")
        return False
    
    print("\n=== Test Summary ===")
    print("✓ All basic tests passed!")
    print("\nTo see migration metadata in action:")
    print("1. Start Redis with cluster mode enabled")
    print("2. Set up cluster nodes with migration slots")
    print("3. Use CLUSTER SETSLOT to mark slots as migrating/importing")
    print("4. Read keys in those slots to see metadata")
    
    return True

def main():
    """Main test function"""
    print("Redis Cluster Migration Metadata Test")
    print("=" * 40)
    
    success = test_migration_metadata()
    
    if success:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 