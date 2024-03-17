#!/bin/bash -e

source ./config.sh
rm -rf ${YCSB_LOG_FILENAME}
rm -rf ${LOCAL_LOG_DIR}

for redis_instance in "${!instances[@]}"; do
    echo "$redis_instance - ${instances[$redis_instance]}"
    IFS=',' read -r -a nodeInstance <<< "${instances[$redis_instance]}"
    for i in "${!nodeInstance[@]}"; do
        IFS="|" read -r -a info <<< "${nodeInstance[i]}"
        (
            tko=$(sudo ssh -o StrictHostKeyChecking=no ${info[1]} bash <<'EOF'
sudo rm -rf /root/rd/src
EOF
2>&1)
            echo "$tko"
        ) &
    done
    wait # Wait for all parallel tasks to finish
done


for redis_instance in "${!instances[@]}"; do
    echo "$redis_instance - ${instances[$redis_instance]}"
    IFS=',' read -r -a nodeInstance <<< "${instances[$redis_instance]}"
    for i in "${!nodeInstance[@]}"; do
        IFS="|" read -r -a info <<< "${nodeInstance[i]}"
        (
            tko=$(sudo ssh -o StrictHostKeyChecking=no ${info[1]} bash <<'EOF'
cd /tmp
rm -rf rd
git clone -b redis_rdma_infiniband_layer https://github.com/efntallaris/rd
cp -rf rd/src /root/rd/src

EOF
2>&1)
            echo "$tko"
        ) &
    done
    wait # Wait for all parallel tasks to finish
done
