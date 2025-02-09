#!/bin/bash -e

source ./config.sh
rm -rf ${YCSB_LOG_FILENAME}
rm -rf ${LOCAL_LOG_DIR}
for redis_instance in "${!instances[@]}"; do
    echo "$redis_instance - ${instances[$redis_instance]}"
    IFS=',' read -r -a nodeInstance <<< "${instances[$redis_instance]}"
    for i in "${!nodeInstance[@]}"; do
        IFS="|" read -r -a info <<< "${nodeInstance[i]}"
        tko=$(sudo ssh -o StrictHostKeyChecking=no ${info[1]} bash <<EOF
cd /root/rd
git stash 
git pull origin debug_version
git checkout -b debug_version 
cd /root/rd/scripts_copy
#./install_preqs.sh
EOF
2>&1)
    done
done
