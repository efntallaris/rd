#!/bin/bash -e

#YCSB_DIR="/mnt/stratos/ycsb-0.5.0/new_modded_ycsb/newer/YCSB"
YCSB_DIR=$1
YCSB_INTERMEDIATE_FOLDER="/tmp/ycsb_temp"
YCSB_BIN="/root/ycsb_client"
YCSB_BIN=$2

sudo rm -rf ${YCSB_INTERMEDIATE_FOLDER}
sudo mkdir -p ${YCSB_INTERMEDIATE_FOLDER}

cd ${YCSB_DIR}
#sudo mvn -pl redis -am clean install -T 9 -DskipTests -Dcheckstyle.skip
sudo mvn -pl site.ycsb:redis-binding -am clean install -T 9 -DskipTests -Dcheckstyle.skip

cd ${YCSB_DIR}/redis/target
sudo tar -xvf ycsb-redis-binding-0.18.0-SNAPSHOT.tar.gz --directory ${YCSB_INTERMEDIATE_FOLDER}

# Check if the directory exists
if [ -d "$YCSB_BIN" ]; then
  # Directory exists, delete everything inside it
  rm -rf "$YCSB_BIN"/*
else
  # Directory does not exist, so create it
  mkdir -p "$YCSB_BIN"
fi

sudo cp -rf ${YCSB_INTERMEDIATE_FOLDER}/ycsb-redis-binding-0.18.0/* ${YCSB_BIN}
