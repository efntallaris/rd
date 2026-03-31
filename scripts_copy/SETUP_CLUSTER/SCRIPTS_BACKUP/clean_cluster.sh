#!/bin/bash -e
LOCAL_IP=$(ifconfig eno1 | grep inet | awk -F"inet " '{print $2}' | awk -F' ' '{print $1}')


REDIS_SRC_DIR="/mnt/stratos/redis/src/redis/src"
REDIS_MAIN_SCRIPT_DIR="/mnt/stratos/redis/scripts/SETUP_CLUSTER/SCRIPTS"
LOCAL_SETUP_DIR="/home/entallaris/redis_bin"

#	MASTER NODES 	#
declare -A redis_master_instances 
redis_master_instances["redis-0"]="redis0|node01|8000|/home/entallaris/node01.conf|node-1.aof|dump-1.rdb"
redis_master_instances["redis-1"]="redis1|node02|8000|/home/entallaris/node02.conf|node-2.aof|dump-2.rdb"
redis_master_instances["redis-2"]="redis2|node03|8000|/home/entallaris/node03.conf|node-3.aof|dump-3.rdb"
redis_master_instances["redis-3"]="redis3|node04|8000|/home/entallaris/node04.conf|node-4.aof|dump-4.rdb"



#	SLAVE NODES 	#
declare -A redis_slave_instances
#redis_slave_instances["redis-1"]="redis1|node03|7001|/home/entallaris/node02.conf|node-2.aof|dump-2.rdb"

echo "CLEANING MASTERS"

for redis_instance in "${!redis_master_instances[@]}"; do
	echo    "$redis_instance - ${redis_master_instances[$redis_instance]}"
	IFS=',' read -r -a nodeInstance <<< "${redis_master_instances[$redis_instance]}"
	for i in ${!nodeInstance[@]};
	do
		IFS="|" read -r -a info <<< "${nodeInstance[i]}"
		echo "running script on $redis_instance , ${info[1]} port ${info[2]}"
tko=$(sshpass -p 'Leros2012!!' ssh -T -oStrictHostKeyChecking=no -p 2222 entallaris@${info[1]} <<-EOF
	cd "${REDIS_MAIN_SCRIPT_DIR}/HELP_SCRIPTS"
	./clean_node.sh ${LOCAL_SETUP_DIR} ${info[3]}
EOF
)
#	echo $tko
	done
done

echo "CLEANING SLAVES"
for redis_instance in "${!redis_slave_instances[@]}"; do
	echo    "$redis_instance - ${redis_slave_instances[$redis_instance]}"
	IFS=',' read -r -a nodeInstance <<< "${redis_slave_instances[$redis_instance]}"
	for i in ${!nodeInstance[@]};
	do
		IFS="|" read -r -a info <<< "${nodeInstance[i]}"
		echo "running script on $redis_instance , ${info[1]} port ${info[2]}"
		tko=$(sshpass -p 'Leros2012!!' ssh -T -oStrictHostKeyChecking=no -p 2222 entallaris@${info[1]} <<-EOF
			cd "${REDIS_MAIN_SCRIPT_DIR}/HELP_SCRIPTS"
			./clean_node.sh ${LOCAL_SETUP_DIR} ${info[3]}
			EOF
		)
        done
done

