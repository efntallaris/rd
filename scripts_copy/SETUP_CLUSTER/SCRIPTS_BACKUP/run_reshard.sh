#!/bin/bash -e
LOCAL_IP=$(ifconfig eno1 | grep inet | awk -F"inet " '{print $2}' | awk -F' ' '{print $1}')

REDIS_SRC_DIR="/mnt/stratos/redis/src/redis/src"
REDIS_MAIN_SCRIPT_DIR="/mnt/stratos/redis/scripts/SETUP_CLUSTER/SCRIPTS"
LOCAL_SETUP_DIR="/home/entallaris/redis_bin"
YCSB_DIR="/mnt/stratos/ycsb-0.5.0/new_ycsb/ycsb-0.17.0/bin"

# 	COMPILE THE SOURCE CODE		#
cd "${REDIS_SRC_DIR}"
sudo make 


#	MASTER NODES 	#
declare -A redis_master_instances 
redis_master_instances["redis-0"]="redis0|192.168.20.2|7000|/home/entallaris/node01.conf|node-1.aof|dump-1.rdb"
redis_master_instances["redis-1"]="redis1|192.168.20.3|7000|/home/entallaris/node02.conf|node-2.aof|dump-2.rdb"
redis_master_instances["redis-2"]="redis2|192.168.20.1|7000|/home/entallaris/node03.conf|node-3.aof|dump-3.rdb"

#	SLAVE NODES 	#
declare -A redis_slave_instances
#redis_slave_instances["redis-1"]="redis1|node03|7001|/home/entallaris/node02.conf|node-2.aof|dump-2.rdb"

declare -A redis_migrate_instances
redis_migrate_instances["redis-3"]="redis0|192.168.20.4|7000|/home/entallaris/node04.conf|node-4.aof|dump-4.rdb"

sudo rm -rf /mnt/stratos/redis/logs/*

cd "${LOCAL_SETUP_DIR}/bin"
sudo ./redis-cli --cluster reshard 192.168.20.1:8000 --cluster-timeout 12000 |sudo tee /mnt/stratos/redis/logs/redis-cli.log 
