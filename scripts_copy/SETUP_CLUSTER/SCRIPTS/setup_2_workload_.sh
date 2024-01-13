#!/bin/bash -e

REDIS_SRC_DIR="/root/rd/src"
REDIS_MAIN_SCRIPT_DIR="/root/rd/scripts_copy/SETUP_CLUSTER/SCRIPTS"
LOCAL_SETUP_DIR="/root/rd/redis_bin"
YCSB_SRC_DIR="/root/rd/ycsb_client"
YCSB_DIR="/root/ycsb_client/bin"
YCSB_LOG_FILENAME="/ycsb_output"

COMMAND_PIPE="/tmp/command-input"
sudo rm -rf /mnt/stratos/redis/logs/*
sudo rm -rf ${COMMAND_PIPE}
mkfifo ${COMMAND_PIPE}
#	SLAVE NODES 	#
#declare -A redis_slave_instances
#redis_slave_instances["redis-4"]="redis3|192.168.20.4|8000|/home/entallaris/node04.conf|node-4.aof|dump-4.rdb"

cd "${REDIS_SRC_DIR}"
sudo make PREFIX="${LOCAL_SETUP_DIR}" install


REDIS_HOST="10.10.1.1"
REDIS_PORT="8000"
MAIN_DIR="/root/rd"
YCSB_DIR="/root/ycsb_client/bin"
REDIS_WORKLOAD_PATH="${MAIN_DIR}/workloads"
REDIS_WORKLOAD_NAME=$1
REDIS_WORKLOAD=${REDIS_WORKLOAD_PATH}${REDIS_WORKLOAD_NAME}
YCSB_LOG_FILENAME="/root/ycsb_output"
declare -A redis_migrate_instances
redis_migrate_instances["redis-3"]="redis3|10.10.1.4|8000|/root/node04.conf"

cd $YCSB_DIR

ycsbCommand=$(sudo ./ycsb.sh run redis -p "redis.host=${REDIS_HOST}" -p "redis.port=${REDIS_PORT}" -p "redis.cluster=true" -P ${REDIS_WORKLOAD} -p status.interval=1 -s  -p \measurementtype=timeseries -p redis.timeout=1000 -threads 100 >> ${YCSB_LOG_FILENAME} 2>&1 &)

sleep 5 
for redis_instance in "${!redis_migrate_instances[@]}"; do
        IFS=',' read -r -a nodeInstance <<< "${redis_migrate_instances[$redis_instance]}"
	IFS="|" read -r -a info <<< "${nodeInstance[0]}"
	cd ${LOCAL_SETUP_DIR}/bin
	migrateNodeID=$(./redis-cli -c -h ${info[1]} -p ${info[2]} CLUSTER MYID)
	tail -f ${COMMAND_PIPE} | ./redis-cli --cluster reshard 10.10.1.4:8000 --cluster-timeout 1200 &
	sleep 4 
	echo "4095" >> ${COMMAND_PIPE}
	sleep 1
	echo ${migrateNodeID} >> ${COMMAND_PIPE}
	sleep 1
	echo "all" >> ${COMMAND_PIPE}
	sleep 1
	echo "yes" >> ${COMMAND_PIPE}
	sleep 1
done
sleep 1

#ycsbCommand=$(sudo ./ycsb run redis -p "redis.host=127.0.0.1" -p "redis.port=8000" -p "redis.cluster=true" -P ../workloads/workloada -p recordcount=60000000 -s -p status.interval=2 -threads 16)
#ycsbCommand=$(sudo ./ycsb run redis -p "redis.host=192.168.20.1" -p "redis.port=8000" -p "redis.cluster=true" -P ../workloads/workloada -p recordcount=10000 -s -p status.interval=2 -threads 16)
#ycsbCommand=$(sudo ./ycsb run redis -p "redis.host=${REDIS_HOST}" -p "redis.port=${REDIS_PORT}" -p "redis.cluster=true" -P ${REDIS_WORKLOAD} -p recordcount=${YCSB_RECORDS} -s -p status.interval=2 -threads 16)

