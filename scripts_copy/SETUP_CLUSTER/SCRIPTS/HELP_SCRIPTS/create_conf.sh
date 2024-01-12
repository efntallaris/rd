#!/bin/bash -e

CONFIG_FILE_DIR=$1
PORT=$2
APPEND_FILENAME=$3
DB_FILENAME=$4
LOG_FILENAME=$5
echo "CREATING CONFIG FILE - ${CONFIG_FILE_DIR}"
cat <<EOT >> "${CONFIG_FILE_DIR}"
port ${PORT}
cluster-enabled yes
daemonize yes
cluster-node-timeout 9000
cluster-require-full-coverage no
bind 0.0.0.0
appendonly no
logfile ${LOG_FILENAME}
EOT
