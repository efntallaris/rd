#!/bin/bash -e

YCSB_DIR="/mnt/stratos/ycsb-0.5.0/new_modded_ycsb/newer/YCSB"
YCSB_INTERMEDIATE_FOLDER="/tmp/ycsb_temp"
YCSB_BIN="/mnt/stratos/modded_redis/run_workload_redis"
sudo rm -rf ${YCSB_INTERMEDIATE_FOLDER}
sudo mkdir -p ${YCSB_INTERMEDIATE_FOLDER}

cd ${YCSB_DIR}
#sudo mvn clean install -T 8 -Dcheckstyle.skip
#sudo mvn clean install -T 8 -Dcheckstyle.skip
#sudo mvn clean install -T 8 -Dcheckstyle.skip
sudo mvn clean install -T 8 -DskipTests -Dcheckstyle.skip
#sudo mvn -pl site.ycsb:redis-binding -am clean install -X 


cd ${YCSB_DIR}/distribution/target
sudo tar -xvf ycsb-0.18.0-SNAPSHOT.tar.gz --directory ${YCSB_INTERMEDIATE_FOLDER}
sudo cp -rf ${YCSB_INTERMEDIATE_FOLDER}/ycsb-0.18.0-SNAPSHOT/*  ${YCSB_BIN}
