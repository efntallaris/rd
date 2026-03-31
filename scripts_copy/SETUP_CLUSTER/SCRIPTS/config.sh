#!/bin/bash

# ─── Directories ────────────────────────────────────────────────────────────
MAIN_DIR="/root/rd"
REDIS_SRC_DIR="/root/rd/redis/src"
REDIS_MAIN_SCRIPT_DIR="/root/rd/scripts_copy/SETUP_CLUSTER/SCRIPTS"
LOCAL_SETUP_DIR="/root/rd/redis_bin"
YCSB_SRC_DIR="/root/rd/ycsb_client"
YCSB_DIR="/root/ycsb_client"
YCSB_DIR_BIN="${YCSB_DIR}/bin"
REDIS_WORKLOAD_PATH="${MAIN_DIR}/workloads/"
REDIS_LOG_DIR="/tmp/redis_logs"
LOCAL_LOG_DIR="/root/systat_logs"
YCSB_LOG_FILENAME="/tmp/ycsb_output"
EXPERIMENTAL_OUTPUT_DIR="/tmp/experiments"

# ─── Cluster settings ──────────────────────────────────────────────────────
MASTER_PORT="8000"

# ─── Experiment ─────────────────────────────────────────────────────────────
EXPERIMENT_NAME="default_experiment"
REDIS_WORKLOAD="workloadfulva5050"

# ─── Node roles ─────────────────────────────────────────────────────────────
# Format: "hostname|host|port|config_file"
# Edit these to change the cluster topology.

#   MASTER NODES — form the initial cluster
declare -A redis_master_instances
redis_master_instances["redis-0"]="redis0|redis0|${MASTER_PORT}|/root/node00.conf"
redis_master_instances["redis-1"]="redis1|redis1|${MASTER_PORT}|/root/node01.conf"
redis_master_instances["redis-2"]="redis2|redis2|${MASTER_PORT}|/root/node02.conf"
# redis_master_instances["redis-3"]="redis3|redis3|${MASTER_PORT}|/root/node03.conf"
# redis_master_instances["redis-4"]="redis4|redis4|${MASTER_PORT}|/root/node04.conf"

#   MIGRATION NODES — added to cluster, receive resharded slots
declare -A redis_migrate_instances
redis_migrate_instances["redis-3"]="redis3|redis3|${MASTER_PORT}|/root/node03.conf"
# redis_migrate_instances["redis-4"]="redis4|redis4|${MASTER_PORT}|/root/node04.conf"

#   YCSB NODES — run the benchmark client
declare -A redis_ycsb_instances
redis_ycsb_instances["ycsb-0"]="ycsb0|ycsb0"

# Master host used by YCSB to connect to the cluster
MASTER_HOST="redis0"
YCSB_LOADER_INSTANCE="ycsb0"

#   ALL INSTANCES — used by kill/clean scripts
declare -A instances
instances["redis-0"]="redis0|redis0"
instances["redis-1"]="redis1|redis1"
instances["redis-2"]="redis2|redis2"
instances["redis-3"]="redis3|redis3"
instances["redis-4"]="redis4|redis4"
instances["ycsb-0"]="ycsb0|ycsb0"

# ─── Monitoring suffixes ────────────────────────────────────────────────────
IF_STAT="_ifstat.txt"
MP_STAT="_mpstat.txt"
IO_STAT="_iostat.txt"
