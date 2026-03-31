#!/bin/bash -e

CONFIG_FILE_DIR=$1
PORT=$2
LOG_FILENAME=$3
echo "CREATING CONFIG FILE - ${CONFIG_FILE_DIR}"
cat <<EOT >> "${CONFIG_FILE_DIR}"
port ${PORT}
cluster-enabled yes
daemonize yes
maxclients 100000
cluster-node-timeout 9000
cluster-require-full-coverage no
bind 0.0.0.0
appendonly no
logfile ${LOG_FILENAME}

EOT
