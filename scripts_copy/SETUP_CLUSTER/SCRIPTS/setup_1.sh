#!/bin/bash -e

source ./config.sh

REDIS_WORKLOAD_NAME=$1


COMMAND_PIPE="/tmp/command-input"
sudo rm -rf ${REDIS_LOG_DIR}/*
sudo rm -rf ${COMMAND_PIPE}
mkfifo ${COMMAND_PIPE}

./kill_scripts.sh
./clean_cluster.sh
./setup_cluster.sh ${REDIS_WORKLOAD_NAME}
./setup_scripts.sh
