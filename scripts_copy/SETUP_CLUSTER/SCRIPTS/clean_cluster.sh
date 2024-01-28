#!/bin/bash -e
LOCAL_IP=$(ifconfig eno1 | grep inet | awk -F"inet " '{print $2}' | awk -F' ' '{print $1}')


MAIN_DIR="/root/rd"
REDIS_SRC_DIR="/root/rd/src"
YCSB_SRC_DIR="/root/rd/ycsb_client"
REDIS_MAIN_SCRIPT_DIR="/root/rd/scripts_copy/SETUP_CLUSTER/SCRIPTS"
LOCAL_SETUP_DIR="/root/rd/redis_bin"
YCSB_DIR="/root/ycsb_client"

source ../config.sh

echo "CLEANING MASTERS"

for redis_instance in "${!redis_master_instances[@]}"; do
	echo    "$redis_instance - ${redis_master_instances[$redis_instance]}"
	IFS=',' read -r -a nodeInstance <<< "${redis_master_instances[$redis_instance]}"
	for i in ${!nodeInstance[@]};
	do
		IFS="|" read -r -a info <<< "${nodeInstance[i]}"
		echo "running script on $redis_instance , ${info[1]} port ${info[2]}"

		tko=$(sudo ssh -o StrictHostKeyChecking=no ${info[1]} bash <<EOF
		cd "${REDIS_MAIN_SCRIPT_DIR}/HELP_SCRIPTS"
		chmod +x clean_node.sh
		./clean_node.sh ${LOCAL_SETUP_DIR} ${info[3]}
EOF
2>&1)
		echo "$tko"
	done
done

