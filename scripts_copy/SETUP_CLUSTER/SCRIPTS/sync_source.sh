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
sudo rm -rf /root/rd/src
sudo rm -rf /root/rd/scripts_copy
sudo rm -rf /root/rd/workloads
cd /tmp
rm -rf rd
git clone https://github.com/efntallaris/rd
cp -rf rd/scripts_copy /root/rd/scripts_copy
cp -rf rd/src /root/rd/src
cp -rf rd/workloads /root/rd/workloads
EOF
2>&1)
    done
done
