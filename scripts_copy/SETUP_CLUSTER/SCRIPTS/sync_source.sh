#!/bin/bash -e


MAIN_DIR="/root/rd"
#	MASTER NODES 	#
declare -A redis_master_instances 
redis_master_instances["redis-1"]="redis1|10.10.1.2|8000|/root/node02.conf"
redis_master_instances["redis-2"]="redis2|10.10.1.3|8000|/root/node03.conf"
redis_master_instances["redis-3"]="redis2|10.10.1.4|8000|/root/node03.conf"
redis_master_instances["redis-4"]="redis0|10.10.1.5|8000|/root/node01.conf"
redis_master_instances["ycsb-0"]="ycsb0|10.10.1.6|8000|/root/node02.conf"
redis_master_instances["ycsb-1"]="ycsb1|10.10.1.7|8000|/root/node03.conf"
redis_master_instances["ycsb-2"]="ycsb2|10.10.1.8|8000|/root/node03.conf"
for redis_instance in "${!redis_master_instances[@]}"; do
    echo "$redis_instance - ${redis_master_instances[$redis_instance]}"
    IFS=',' read -r -a nodeInstance <<< "${redis_master_instances[$redis_instance]}"
    for i in "${!nodeInstance[@]}"; do
        IFS="|" read -r -a info <<< "${nodeInstance[i]}"
        echo "running script on $redis_instance, ${info[1]} port ${info[2]}"
        tko=$(sudo ssh -o StrictHostKeyChecking=no ${info[1]} bash <<EOF
            sudo rm -rf ${MAIN_DIR}
EOF
2>&1)
    echo "$tko"
    done
done

for redis_instance in "${!redis_master_instances[@]}"; do
    echo "$redis_instance - ${redis_master_instances[$redis_instance]}"
    IFS=',' read -r -a nodeInstance <<< "${redis_master_instances[$redis_instance]}"
    for i in "${!nodeInstance[@]}"; do
        IFS="|" read -r -a info <<< "${nodeInstance[i]}"
	tko=$(sudo scp -o StrictHostKeyChecking=no -r ${MAIN_DIR} ${info[1]}:/root/rd)
    	echo "$tko"
    done
done
