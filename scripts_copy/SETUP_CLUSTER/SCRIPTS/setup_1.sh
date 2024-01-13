#!/bin/bash -e

COMMAND_PIPE="/tmp/command-input"
LOG_DIR="/proj/streamstore-PG0/experiment_outputs"
sudo rm -rf ${LOG_DIR}/*
sudo rm -rf ${COMMAND_PIPE}
mkfifo ${COMMAND_PIPE}

./kill_scripts.sh
./clean_cluster.sh
./setup_cluster.sh
./setup_scripts.sh
