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
#LINE BELOW THROWS AN ERROR
#cluster-config-file ${CONFIG_FILE_DIR}
cluster-node-timeout 9000000 
cluster-require-full-coverage no
bind 0.0.0.0
appendonly no
save ""
#appendonly yes
#appendfilename ${APPEND_FILENAME}
dbfilename ${DB_FILENAME}
#logfile ${LOG_FILENAME} 
logfile ${LOG_FILENAME}
EOT
