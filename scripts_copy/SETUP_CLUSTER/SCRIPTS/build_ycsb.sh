#!/bin/bash -e

#YCSB_DIR="/mnt/stratos/ycsb-0.5.0/new_modded_ycsb/newer/YCSB"
YCSB_DIR=$1
YCSB_INTERMEDIATE_FOLDER="/tmp/ycsb_temp"
YCSB_BIN="/root/ycsb_client"
YCSB_BIN=$2


sudo rm -rf ${YCSB_INTERMEDIATE_FOLDER}
sudo mkdir -p ${YCSB_INTERMEDIATE_FOLDER}

cd ${YCSB_DIR}
sudo mvn clean install -T 8 -DskipTests -Dcheckstyle.skip
# sudo mvn clean install -T 8 -DskipTests -Dcheckstyle.skip -pl site.ycsb:redis-binding -am clean package


cd ${YCSB_DIR}/distribution/target
TAR_GZ_FILE=$(ls *.tar.gz | head -n 1)
if [ -z "$TAR_GZ_FILE" ]; then
	  echo "No tar.gz file found in ${YCSB_DIR}/distribution/target"
	    exit 1
fi
sudo tar -xvf "$TAR_GZ_FILE" --directory ${YCSB_INTERMEDIATE_FOLDER}

# Check if the directory exists
if [ -d "$YCSB_BIN" ]; then
    # Directory exists, delete everything inside it
    rm -rf "$YCSB_BIN"/*
else
    # Directory does not exist, so create it
    mkdir -p "$YCSB_BIN"
fi

sudo cp -rf ${YCSB_INTERMEDIATE_FOLDER}/ycsb-0.18.0-SNAPSHOT/*  ${YCSB_BIN}
