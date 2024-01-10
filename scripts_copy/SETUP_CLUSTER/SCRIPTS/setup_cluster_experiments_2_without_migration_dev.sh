#!/bin/bash -e

REDIS_SRC_DIR="/mnt/stratos/redis/src/redis/src"
REDIS_MAIN_SCRIPT_DIR="/mnt/stratos/redis/scripts/SETUP_CLUSTER/SCRIPTS"
LOCAL_SETUP_DIR="/home/entallaris/redis_bin"
YCSB_DIR="/mnt/stratos/modded_redis/run_workload_redis/bin"
YCSB_LOG_FILENAME="/home/entallaris/ycsb_output"

COMMAND_PIPE="/tmp/command-input"
sudo rm -rf /mnt/stratos/redis/logs/*
sudo rm -rf ${COMMAND_PIPE}
mkfifo ${COMMAND_PIPE}
#	SLAVE NODES 	#
#declare -A redis_slave_instances
#redis_slave_instances["redis-4"]="redis3|192.168.20.4|8000|/home/entallaris/node04.conf|node-4.aof|dump-4.rdb"

cd "${REDIS_SRC_DIR}"
sudo make PREFIX="${LOCAL_SETUP_DIR}" install


REDIS_HOST="192.168.20.1"
REDIS_PORT="8000"
REDIS_WORKLOAD_PATH="../../workloads_dev/"
REDIS_WORKLOAD_NAME=$1
REDIS_WORKLOAD=${REDIS_WORKLOAD_PATH}${REDIS_WORKLOAD_NAME}

declare -A redis_migrate_instances
redis_migrate_instances["redis-3"]="redis3|192.168.20.4|8000|/home/entallaris/node04.conf|node-4.aof|dump-4.rdb"

cd $YCSB_DIR
ycsbCommand=$(sudo ./ycsb.sh run redis -p "redis.host=${REDIS_HOST}" -p "redis.port=${REDIS_PORT}" -p "redis.cluster=true" -P ${REDIS_WORKLOAD} -p status.interval=1 -s  -p \measurementtype=timeseries -p redis.timeout=10000 -threads 50 >> ${YCSB_LOG_FILENAME} 2>&1 &)


