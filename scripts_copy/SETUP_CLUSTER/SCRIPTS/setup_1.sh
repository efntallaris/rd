#!/bin/bash -e

source ./config.sh
COMMAND_PIPE="/tmp/command-input"
sudo rm -rf ${REDIS_LOG_DIR}/*
sudo rm -rf ${COMMAND_PIPE}
mkfifo ${COMMAND_PIPE}

./kill_scripts.sh
./clean_cluster.sh
./setup_cluster.sh
./setup_scripts.sh
