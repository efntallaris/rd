#!/usr/bin/env python3
"""
Test script to demonstrate RESP3 response structure with migration metadata.
This script shows how Redis responses look when metadata is included.
"""

import redis
import sys

def test_resp3_response_structure():
    """Test and demonstrate RESP3 response structure with metadata"""
    
    print("Redis RESP3 Response Structure with Migration Metadata")
    print("=" * 60)
    
    # Connect to Redis
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=False)
        r.ping()
        print("✓ Connected to Redis")
    except Exception as e:
        print(f"✗ Failed to connect to Redis: {e}")
        return False
    
    # Switch to RESP3 protocol
    try:
        hello_response = r.execute_command('HELLO', '3')
        print(f"✓ Switched to RESP3 protocol: {hello_response}")
    except Exception as e:
        print(f"✗ Failed to switch to RESP3: {e}")
        return False
    
    # Test 1: Set a test key
    print("\n=== Test 1: Setting Test Key ===")
    try:
        r.set('testkey', 'testvalue')
        print("✓ Set testkey = testvalue")
    except Exception as e:
        print(f"✗ Failed to set key: {e}")
        return False
    
    # Test 2: Get the key and examine raw response
    print("\n=== Test 2: Raw RESP3 Response ===")
    try:
        # Get raw response
        raw_response = r.get('testkey')
        print(f"✓ Raw response type: {type(raw_response)}")
        print(f"✓ Raw response: {raw_response}")
        
        # Decode as string for display
        if isinstance(raw_response, bytes):
            decoded = raw_response.decode('utf-8', errors='replace')
            print(f"✓ Decoded response: {repr(decoded)}")
            
            # Show RESP3 structure breakdown
            print("\n--- RESP3 Structure Breakdown ---")
            lines = decoded.split('\\r\\n')
            for i, line in enumerate(lines):
                if line:
                    print(f"Line {i+1}: {repr(line)}")
        
    except Exception as e:
        print(f"✗ Failed to get key: {e}")
        return False
    
    # Test 3: Test with different data types
    print("\n=== Test 3: Different Data Types ===")
    test_cases = [
        ('string_key', 'string_value'),
        ('number_key', 12345),
        ('float_key', 3.14159),
    ]
    
    for key, value in test_cases:
        try:
            r.set(key, value)
            raw_response = r.get(key)
            if isinstance(raw_response, bytes):
                decoded = raw_response.decode('utf-8', errors='replace')
                print(f"✓ {key}: {repr(decoded[:100])}...")
        except Exception as e:
            print(f"✗ Failed to test {key}: {e}")
    
    # Test 4: Hash operations
    print("\n=== Test 4: Hash Operations ===")
    try:
        r.hset('user:123', 'name', 'John Doe')
        r.hset('user:123', 'age', '30')
        
        # Get hash field
        raw_response = r.hget('user:123', 'name')
        if isinstance(raw_response, bytes):
            decoded = raw_response.decode('utf-8', errors='replace')
            print(f"✓ HGET user:123 name: {repr(decoded)}")
        
        # Get all hash fields
        raw_response = r.hgetall('user:123')
        print(f"✓ HGETALL user:123: {raw_response}")
        
    except Exception as e:
        print(f"✗ Failed to test hash operations: {e}")
    
    # Test 5: List operations
    print("\n=== Test 5: List Operations ===")
    try:
        r.lpush('mylist', 'item1', 'item2', 'item3')
        
        # Get list element
        raw_response = r.lindex('mylist', 0)
        if isinstance(raw_response, bytes):
            decoded = raw_response.decode('utf-8', errors='replace')
            print(f"✓ LINDEX mylist 0: {repr(decoded)}")
        
    except Exception as e:
        print(f"✗ Failed to test list operations: {e}")
    
    print("\n=== RESP3 Response Structure Explanation ===")
    print("""
When Redis includes metadata in responses, the structure is:

*2\\r\\n                    # Array with 2 elements: [attributes, value]
%4\\r\\n                    # Map with 4 key-value pairs (attributes)
$7\\r\\nslot_id\\r\\n:1000\\r\\n
$16\\r\\nmigration_status\\r\\n:1\\r\\n
$9\\r\\nsource_id\\r\\n:12345\\r\\n
$7\\r\\ndest_id\\r\\n:67890\\r\\n
$9\\r\\ntestvalue\\r\\n     # The actual value

Where:
- *2 = Array with 2 elements
- %4 = Map with 4 entries
- $7 = String of length 7
- :1000 = Integer value
- slot_id, migration_status, source_id, dest_id = Attribute keys
- testvalue = The actual data value
""")
    
    print("\n=== Client Implementation Notes ===")
    print("""
To implement metadata reading in a client:

1. Parse the RESP3 array structure (*2)
2. Check if first element is a map (%4)
3. Parse the map to extract metadata:
   - slot_id (integer)
   - migration_status (integer)
   - source_id (integer) 
   - dest_id (integer)
4. Parse the second element as the actual value
5. Return both metadata and value to the application

The metadata indicates:
- slot_id: Hash slot number (0-16383)
- migration_status: 0=NOT_MIGRATED, 1=IN_PROGRESS, 2=MIGRATED
- source_id: Source node configEpoch
- dest_id: Destination node configEpoch
""")
    
    return True

def main():
    """Main test function"""
    success = test_resp3_response_structure()
    
    if success:
        print("\n🎉 RESP3 response structure test completed!")
        print("Check the output above to understand the response format.")
        sys.exit(0)
    else:
        print("\n❌ Test failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 