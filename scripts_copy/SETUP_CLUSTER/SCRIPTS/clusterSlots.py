import redis
import time

def get_redis_slots(redis_host='localhost', redis_port=6379):
    try:
        # Connect to Redis
        r = redis.StrictRedis(host=redis_host, port=redis_port)

        # Get slot information
        info = r.execute_command('CLUSTER', 'SLOTS')

        # Extract slot ranges and corresponding nodes
        slot_info = {}
        for slot_range_info in info:
            start_slot = slot_range_info[0]
            end_slot = slot_range_info[1]
            master_node = slot_range_info[2][0]
            slot_info[(start_slot, end_slot)] = master_node

        return slot_info

    except redis.exceptions.RedisError as e:
        print(f"Error: {e}")
        return None

if __name__ == "__main__":
    # Change these values to match your Redis server
    redis_host = '10.10.1.1'
    redis_port = 8000

    while True:
        slots = get_redis_slots(redis_host, redis_port)
        if slots:
            print("Slot Ranges : Master Node")
            print(slots)
            time.sleep(1)
