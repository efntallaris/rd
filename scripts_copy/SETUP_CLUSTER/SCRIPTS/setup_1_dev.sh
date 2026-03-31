#!/bin/bash -e
LOCAL_IP=$(ifconfig eno1 | grep inet | awk -F"inet " '{print $2}' | awk -F' ' '{print $1}')


REDIS_MAIN_SCRIPT_DIR="/mnt/stratos/redis/scripts/SETUP_CLUSTER/SCRIPTS"
LOCAL_SETUP_DIR="/home/entallaris/redis_bin"

COMMAND_PIPE="/tmp/command-input"
sudo rm -rf /mnt/stratos/redis/logs/*
sudo rm -rf ${COMMAND_PIPE}
mkfifo ${COMMAND_PIPE}

./kill_scripts.sh
./clean_cluster.sh
./setup_cluster_dev.sh
./setup_scripts.sh
