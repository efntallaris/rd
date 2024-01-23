#!/bin/bash

# DONT ADD / at the end of the filepath or dir
MAIN_DIR="/root/rd"
REDIS_SRC_DIR="/root/rd/src"
YCSB_SRC_DIR="/root/rd/ycsb_client"
REDIS_MAIN_SCRIPT_DIR="/root/rd/scripts_copy/SETUP_CLUSTER/SCRIPTS"
LOCAL_SETUP_DIR="/root/rd/redis_bin"
YCSB_DIR="/root/ycsb_client"
YCSB_LOG_FILENAME="/tmp/ycsb_output"
MASTER_HOST="10.10.1.1"
MASTER_PORT="8000"
REDIS_WORKLOAD="workloadfulva5050"
YCSB_LOADER_INSTANCE="10.10.1.6"
REDIS_LOG_DIR="/proj/streamstore-PG0/experiment_outputs"
YCSB_DIR_BIN="/root/ycsb_client/bin"
REDIS_WORKLOAD_PATH="${MAIN_DIR}/workloads/"
YCSB_LOG_FILENAME="/tmp/ycsb_output"
LOCAL_LOG_DIR="/root/systat_logs"


#	MASTER NODES 	#
declare -A redis_master_instances 
redis_master_instances["redis-0"]="redis0|10.10.1.1|8000|/root/node01.conf"
redis_master_instances["redis-1"]="redis1|10.10.1.2|8000|/root/node02.conf"
redis_master_instances["redis-2"]="redis2|10.10.1.3|8000|/root/node03.conf"
# redis_master_instances["redis-3"]="redis3|10.10.1.4|8000|/root/node04.conf"

declare -A redis_migrate_instances
redis_migrate_instances["redis-3"]="redis4|10.10.1.4|8000|/root/node05.conf"

#       THE NODE WHERE YCSB RUNS        #
declare -A redis_ycsb_instances
redis_ycsb_instances["ycsb-0"]="ycsb0|10.10.1.6"
# redis_ycsb_instances["ycsb-1"]="ycsb1|10.10.1.7"
# redis_ycsb_instances["ycsb-2"]="ycsb2|10.10.1.8"
# redis_ycsb_instances["ycsb-3"]="ycsb1|10.10.1.9"
# redis_ycsb_instances["ycsb-4"]="ycsb2|10.10.1.10"
# redis_ycsb_instances["ycsb-5"]="ycsb1|10.10.1.11"
# redis_ycsb_instances["ycsb-6"]="ycsb2|10.10.1.12"
# redis_ycsb_instances["ycsb-7"]="ycsb2|10.10.1.13"


declare -A instances
instances["redis-0"]="redis0|10.10.1.1"
instances["redis-1"]="redis1|10.10.1.2"
instances["redis-2"]="redis2|10.10.1.3"
instances["redis-3"]="redis3|10.10.1.4"
instances["ycsb-0"]="ycsb0|10.10.1.6"
# instances["ycsb1"]="ycsb1|10.10.1.7"
# instances["ycsb2"]="ycsb1|10.10.1.8"
# instances["ycsb3"]="ycsb1|10.10.1.9"
# instances["ycsb4"]="ycsb1|10.10.1.10"
# instances["ycsb5"]="ycsb1|10.10.1.11"
# instances["ycsb6"]="ycsb1|10.10.1.12"
# instances["ycsb7"]="ycsb1|10.10.1.13"


LOCAL_SETUP_DIR="/root/rd/redis_bin"
SCRIPT_DIR="/mnt/stratos/redis/scripts"

IF_STAT="_ifstat.txt"
MP_STAT="_mpstat.txt"
IO_STAT="_iostat.txt"
