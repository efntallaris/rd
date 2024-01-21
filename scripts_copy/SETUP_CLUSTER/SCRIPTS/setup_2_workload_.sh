#!/bin/bash -e

REDIS_SRC_DIR="/root/rd/src"
REDIS_MAIN_SCRIPT_DIR="/root/rd/scripts_copy/SETUP_CLUSTER/SCRIPTS"
LOCAL_SETUP_DIR="/root/rd/redis_bin"
YCSB_SRC_DIR="/root/rd/ycsb_client"
YCSB_DIR="/root/ycsb_client/bin"
YCSB_LOG_FILENAME="/tmp/ycsb_output"

COMMAND_PIPE="/tmp/command-input"
sudo rm -rf /mnt/stratos/redis/logs/*
sudo rm -rf ${COMMAND_PIPE}
mkfifo ${COMMAND_PIPE}

cd "${REDIS_SRC_DIR}"
sudo make PREFIX="${LOCAL_SETUP_DIR}" install


REDIS_HOST="10.10.1.1"
REDIS_PORT="8000"
MAIN_DIR="/root/rd"
YCSB_DIR="/root/ycsb_client/bin"
REDIS_WORKLOAD_PATH="${MAIN_DIR}/workloads/"
REDIS_WORKLOAD_NAME=$1
REDIS_WORKLOAD=${REDIS_WORKLOAD_PATH}${REDIS_WORKLOAD_NAME}
YCSB_LOG_FILENAME="/tmp/ycsb_output"
declare -A redis_migrate_instances
redis_migrate_instances["redis-4"]="redis3|10.10.1.5|8000|/root/node03.conf"

declare -A redis_ycsb_instances
redis_ycsb_instances["ycsb-0"]="ycsb0|10.10.1.6"
redis_ycsb_instances["ycsb-1"]="ycsb1|10.10.1.7"
redis_ycsb_instances["ycsb-2"]="ycsb2|10.10.1.8"


# RUN YCSB BEFORE MIGRATION IN YCSB_INSTANCES
for redis_instance in "${!redis_ycsb_instances[@]}"; do
    echo "$redis_instance - ${redis_ycsb_instances[$redis_instance]}"
    IFS=',' read -r -a nodeInstance <<< "${redis_ycsb_instances[$redis_instance]}"
    for i in "${!nodeInstance[@]}"; do
        IFS="|" read -r -a info <<< "${nodeInstance[i]}"
        echo "running script on $redis_instance, ${info[1]} port ${info[2]}"
        tko=$(sudo ssh -o StrictHostKeyChecking=no ${info[1]} bash <<EOF
		cd $YCSB_DIR

		sudo ./ycsb.sh run redis -p "redis.host=${REDIS_HOST}" -p "redis.port=${REDIS_PORT}" -p "redis.cluster=true" -P ${REDIS_WORKLOAD} -p status.interval=1 -s  -p \measurementtype=timeseries -p redis.timeout=1000 -threads 100 >> ${YCSB_LOG_FILENAME} 2>&1 &
EOF
2>&1)
    echo "$tko"
    done
done


sleep 10 
#for redis_instance in "${!redis_migrate_instances[@]}"; do
#        IFS=',' read -r -a nodeInstance <<< "${redis_migrate_instances[$redis_instance]}"
#	IFS="|" read -r -a info <<< "${nodeInstance[0]}"
#	cd ${LOCAL_SETUP_DIR}/bin
#	migrateNodeID=$(./redis-cli -c -h ${info[1]} -p ${info[2]} CLUSTER MYID)
#	tail -f ${COMMAND_PIPE} | ./redis-cli --cluster reshard 10.10.1.4:8000 --cluster-timeout 1200 &
#	sleep 4 
#	echo "4095" >> ${COMMAND_PIPE}
#	sleep 1
#	echo ${migrateNodeID} >> ${COMMAND_PIPE}
#	sleep 1
#	echo "all" >> ${COMMAND_PIPE}
#	sleep 1
#	echo "yes" >> ${COMMAND_PIPE}
#	sleep 1
#done
sleep 1

