#!/bin/bash -e
LOCAL_IP=$(ifconfig eno1 | grep inet | awk -F"inet " '{print $2}' | awk -F' ' '{print $1}')


REDIS_SRC_DIR="/mnt/stratos/redis/src/redis/src"
REDIS_MAIN_SCRIPT_DIR="/mnt/stratos/redis/scripts/SETUP_CLUSTER/SCRIPTS"
LOCAL_SETUP_DIR="/home/entallaris/redis_bin"
YCSB_DIR="/mnt/stratos/modded_redis/bin"

declare -A redis_migrate_instances
redis_migrate_instances["redis-3"]="redis3|192.168.20.4|8000|/home/entallaris/node04.conf|node-4.aof|dump-4.rdb"


UNLOCK_START=$1
UNLOCK_END=$2

IP=$3
PORT=$4
echo "${UNLOCK_START} - ${UNLOCK_END}"
cd ${LOCAL_SETUP_DIR}

for redis_instance in "${!redis_migrate_instances[@]}"; do
        IFS=',' read -r -a nodeInstance <<< "${redis_migrate_instances[$redis_instance]}"
        IFS="|" read -r -a info <<< "${nodeInstance[0]}"
        cd ${LOCAL_SETUP_DIR}/bin
        migrateNodeID=$(./redis-cli -c -h ${IP} -p ${PORT} CLUSTER MYID)
	break;
done

echo ${migrateNodeID}
for i in $(seq ${UNLOCK_START} 1 ${UNLOCK_END} )
do
	   cmd=$(sudo ./redis-cli -c -p 8000 CLUSTER SETSLOT $i NODE ${migrateNodeID})
	   echo ${cmd}
done
