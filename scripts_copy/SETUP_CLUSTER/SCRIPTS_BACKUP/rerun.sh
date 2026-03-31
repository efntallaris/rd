#!/bin/bash -e
LOCAL_IP=$(ifconfig eno1 | grep inet | awk -F"inet " '{print $2}' | awk -F' ' '{print $1}')


REDIS_MAIN_SCRIPT_DIR="/mnt/stratos/redis/scripts/SETUP_CLUSTER/SCRIPTS"
LOCAL_SETUP_DIR="/home/entallaris/redis_bin"

COMMAND_PIPE="/tmp/command-input"
sudo rm -rf /mnt/stratos/redis/logs/*
sudo rm -rf ${COMMAND_PIPE}
mkfifo ${COMMAND_PIPE}
#	SLAVE NODES 	#
#declare -A redis_slave_instances
#redis_slave_instances["redis-4"]="redis3|192.168.20.4|8000|/home/entallaris/node04.conf|node-4.aof|dump-4.rdb"


declare -A redis_migrate_instances
redis_migrate_instances["redis-3"]="redis3|192.168.20.4|8000|/home/entallaris/node04.conf|node-4.aof|dump-4.rdb"

./kill_scripts.sh
./clean_cluster.sh
./setup_cluster.sh
./setup_scripts.sh
sleep 2
for redis_instance in "${!redis_migrate_instances[@]}"; do
        IFS=',' read -r -a nodeInstance <<< "${redis_migrate_instances[$redis_instance]}"
	IFS="|" read -r -a info <<< "${nodeInstance[0]}"
	cd ${LOCAL_SETUP_DIR}/bin
	migrateNodeID=$(./redis-cli -c -h ${info[1]} -p ${info[2]} CLUSTER MYID)
	tail -f ${COMMAND_PIPE} | ./redis-cli --cluster reshard 192.168.20.1:8000 --cluster-timeout 1200 &
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

