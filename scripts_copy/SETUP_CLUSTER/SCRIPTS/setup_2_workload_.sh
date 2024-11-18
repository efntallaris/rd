#!/bin/bash -e

source ./config.sh

COMMAND_PIPE="/tmp/command-input"
sudo rm -rf /mnt/stratos/redis/logs/*
sudo rm -rf ${COMMAND_PIPE}
mkfifo ${COMMAND_PIPE}

cd "${REDIS_SRC_DIR}"
sudo make PREFIX="${LOCAL_SETUP_DIR}" install


REDIS_WORKLOAD_NAME=$1
REDIS_WORKLOAD=${REDIS_WORKLOAD_PATH}${REDIS_WORKLOAD_NAME}


# RUN YCSB BEFORE MIGRATION IN YCSB_INSTANCES
for redis_instance in "${!redis_ycsb_instances[@]}"; do
    echo "$redis_instance - ${redis_ycsb_instances[$redis_instance]}"
    IFS=',' read -r -a nodeInstance <<< "${redis_ycsb_instances[$redis_instance]}"
    for i in "${!nodeInstance[@]}"; do
        IFS="|" read -r -a info <<< "${nodeInstance[i]}"
        echo "running script on $redis_instance, ${info[1]} port ${info[2]}"
        tko=$(sudo ssh -o StrictHostKeyChecking=no ${info[1]} bash <<EOF
		cd $YCSB_DIR_BIN

		sudo ./ycsb.sh run redis -p "redis.host=${MASTER_HOST}" -p "redis.port=${MASTER_PORT}" -p "redis.cluster=true" -P ${REDIS_WORKLOAD} -p status.interval=1 -s  -p \measurementtype=timeseries -p measurementtype=hdrhistogram -p redis.timeout=10000 -threads 50 >> ${YCSB_LOG_FILENAME}_${info[0]} 2>&1 &
EOF
2>&1)
    echo "$tko"
    done
done


sleep 20

#SINGLE DONOR SINGLE RECIPIENT START
cd ${LOCAL_SETUP_DIR}/bin
first_migration_instance_details=${redis_migrate_instances["redis-4"]}
IFS='|' read -ra ADDR <<< "$first_migration_instance_details"
ip=${ADDR[1]}
port=${ADDR[2]}
migrateNodeID=$(./redis-cli -c -h ${ip} -p ${port} CLUSTER MYID)	
first_instance_details=${redis_master_instances["redis-0"]}
# Extract IP and Port
IFS='|' read -ra ADDR <<< "$first_instance_details"
ip=${ADDR[1]}
port=${ADDR[2]}
sourceNodeID=$(./redis-cli -c -h ${ip} -p ${port} CLUSTER MYID)
tail -f ${COMMAND_PIPE} | ./redis-cli --cluster reshard ${MASTER_HOST}:${MASTER_PORT} --cluster-timeout 1200 &
sleep 4 
echo "1366" >> ${COMMAND_PIPE}
sleep 1
echo ${migrateNodeID} >> ${COMMAND_PIPE}
sleep 1
echo ${sourceNodeID} >> ${COMMAND_PIPE}
sleep 1
echo "done" >> ${COMMAND_PIPE}
sleep 1
echo "yes" >> ${COMMAND_PIPE}
sleep 1
