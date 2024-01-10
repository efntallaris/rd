#!/bin/bash -e


SCRIPT_PATH=$1
PORT=$2
APPEND_FILENAME=$3
DB_FILENAME=$4


cat > $SCRIPT_PATH <<EOF

port ${PORT}
cluster-enabled yes
daemonize yes
cluster-node-timeout 5000
bind 0.0.0.0
appendonly yes
appendfilename ${APPEND_FILENAME}.aof
dbfilename ${DB_FILENAME}.rdb


EOF




