#!/bin/bash -e
declare -A instances

LOCAL_SETUP_DIR="/root/rd/redis_bin"
LOCAL_LOG_DIR="/root/systat_logs"

instances["redis-0"]="redis0|10.10.1.1"
instances["redis-1"]="redis1|10.10.1.2"
instances["redis-2"]="redis2|10.10.1.3"
instances["redis-3"]="redis3|10.10.1.4"
instances["ycsb0"]="ycsb0|10.10.1.5"

for redisInstance in "${!instances[@]}"; do
	echo "Processing $redisInstance..."
	IFS='|' read -r -a info <<< "${instances[$redisInstance]}"
	node_host=${info[1]}
	echo "Attempting to run script on $node_host"
	output=$(ssh -o StrictHostKeyChecking=no "$node_host" sudo bash <<EOF
	    echo "Running on $node_host"
	    pkill -9 redis-server
	    pkill -9 redis-cli
	    pkill -9 mpstat
	    pkill -9 iostat
	    pkill -9 ifstat
	    pkill -9 ycsb
	    rm -rf ${LOCAL_SETUP_DIR}/*
	    rm -rf ${LOCAL_LOG_DIR}/*
EOF
	2>&1)

	echo "$output"


done

