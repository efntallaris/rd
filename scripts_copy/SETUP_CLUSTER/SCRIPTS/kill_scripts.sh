#!/bin/bash -e
declare -A redis_donors
declare -A redis_recipients



redis_donors["redis0"]="redis0|redis0|9000|apf_redis2|db_redis2"
redis_donors["redis1"]="redis1|redis1|9000|apf_redis0|db_redis0"
redis_donors["redis2"]="redis2|redis2|9000|apf_redis1|db_redis1"
redis_donors["redis3"]="redis3|redis3|9000|apf_redis1|db_redis1"
redis_donors["redis4"]="redis4|redis4|9000|apf_redis1|db_redis1"

#redis_donors["redis4"]="redis4|node05|9000|apf_redis1|db_redis1"


for redisInstance in "${!redis_donors[@]}"; do
    echo "Processing $redisInstance..."
    IFS='|' read -r -a info <<< "${redis_donors[$redisInstance]}"
    node_host=${info[0]}
    echo "Attempting to run script on $node_host"

    ssh "$node_host" sudo bash -c "'
        echo "Running on $node_host"
        pkill -9 redis-server
        pkill -9 redis-cli
        pkill -9 mpstat
        pkill -9 iostat
        pkill -9 ifstat
        pkill -9 ycsb
	pwd

    '"
    echo "Completed running script on $node_host"
done

