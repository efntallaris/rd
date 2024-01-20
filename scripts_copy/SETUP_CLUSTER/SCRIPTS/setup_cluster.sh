#!/bin/bash -e
LOCAL_IP=$(ifconfig eno1 | grep inet | awk -F"inet " '{print $2}' | awk -F' ' '{print $1}')


# DONT ADD / at the end of the filepath or dir
MAIN_DIR="/root/rd"
REDIS_SRC_DIR="/root/rd/src"
YCSB_SRC_DIR="/root/rd/ycsb_client"
REDIS_MAIN_SCRIPT_DIR="/root/rd/scripts_copy/SETUP_CLUSTER/SCRIPTS"
LOCAL_SETUP_DIR="/root/rd/redis_bin"
YCSB_DIR="/root/ycsb_client"

MASTER_HOST="130.127.134.83"
MASTER_PORT="8000"
REDIS_WORKLOAD="workloadfulva5050"

YCSB_LOADER_INSTANCE="130.127.134.81"
REDIS_LOG_DIR="/proj/streamstore-PG0/experiment_outputs"


# 	COMPILE THE SOURCE CODE		#
cd "${REDIS_SRC_DIR}"
sudo make

#	MASTER NODES 	#
declare -A redis_master_instances 
redis_master_instances["redis-0"]="redis0|130.127.134.83|8000|/root/node01.conf"
redis_master_instances["redis-1"]="redis1|130.127.134.73|8000|/root/node02.conf"
redis_master_instances["redis-2"]="redis2|130.127.134.96|8000|/root/node03.conf"

declare -A redis_migrate_instances
redis_migrate_instances["redis-3"]="redis3|130.127.134.75|8000|/root/node03.conf"

#       THE NODE WHERE YCSB RUNS        #
declare -A redis_ycsb_instance
redis_ycsb_instance["ycsb-0"]="redis4|130.127.134.81"

#TO run redis with detailed logs
#sudo ./redis-server ${info[3]} --loglevel debug

#TO run redis 
#sudo ./redis-server ${info[3]} 

for redis_instance in "${!redis_master_instances[@]}"; do
    echo "$redis_instance - ${redis_master_instances[$redis_instance]}"
    IFS=',' read -r -a nodeInstance <<< "${redis_master_instances[$redis_instance]}"
    for i in "${!nodeInstance[@]}"; do
        IFS="|" read -r -a info <<< "${nodeInstance[i]}"
        echo "running script on $redis_instance, ${info[1]} port ${info[2]}"
        tko=$(sudo ssh -o StrictHostKeyChecking=no ${info[1]} bash <<EOF
            sudo rm -rf ${info[3]}
            cd "${REDIS_SRC_DIR}"
            sudo make PREFIX="${LOCAL_SETUP_DIR}" install
            cd "${REDIS_MAIN_SCRIPT_DIR}/HELP_SCRIPTS"
            /bin/sh create_conf.sh ${info[3]} ${info[2]} "${REDIS_LOG_DIR}/${info[0]}__"
            cd "${LOCAL_SETUP_DIR}/bin"
            sudo ./redis-server ${info[3]}

	    ps aux | grep redis-server
EOF
2>&1)
    echo "$tko"
    done
done


clusterCreateCommand="sudo ./redis-cli --cluster create"
for redis_instance in "${!redis_master_instances[@]}"; do
        echo    "$redis_instance - ${redis_master_instances[$redis_instance]}"
        IFS=',' read -r -a nodeInstance <<< "${redis_master_instances[$redis_instance]}"
        for i in ${!nodeInstance[@]};
        do
                IFS="|" read -r -a info <<< "${nodeInstance[i]}"
		clusterCreateCommand="$clusterCreateCommand ${info[1]}:${info[2]}"
        done
done

sleep 10
echo "SETTING UP CLUSTER"
clusterCreateCommand="$clusterCreateCommand --cluster-yes"
echo "cluster create command is ${clusterCreateCommand}"
eval "$clusterCreateCommand"
#
#
sleep 5
tko=$(
    sudo ssh -o StrictHostKeyChecking=no ${YCSB_LOADER_INSTANCE} <<-EOF
	cd "${REDIS_MAIN_SCRIPT_DIR}/"
	chmod +x build_ycsb.sh
	sudo ./build_ycsb.sh ${YCSB_SRC_DIR} ${YCSB_DIR}
	cd ${YCSB_DIR}/bin
	echo "running loading phase for workload ${REDIS_WORKLOAD}"
	sudo ./ycsb.sh load redis -p "redis.host=${MASTER_HOST}" -p "redis.port=${MASTER_PORT}" -p "redis.cluster=true" -P ${MAIN_DIR}/workloads/${REDIS_WORKLOAD} -threads 100
EOF
2>&1)
echo ${tko}



for redis_instance in "${!redis_migrate_instances[@]}"; do
    echo "$redis_instance - ${redis_migrate_instances[$redis_instance]}"
    IFS=',' read -r -a nodeInstance <<< "${redis_migrate_instances[$redis_instance]}"
    for i in "${!nodeInstance[@]}"; do
        IFS="|" read -r -a info <<< "${nodeInstance[i]}"
        echo "running script on $redis_instance, ${info[1]} port ${info[2]}"
        tko=$(sudo ssh -o StrictHostKeyChecking=no ${info[1]} bash <<EOF
            sudo rm -rf ${info[3]}
            cd "${REDIS_SRC_DIR}"
            sudo make PREFIX="${LOCAL_SETUP_DIR}" install
            cd "${REDIS_MAIN_SCRIPT_DIR}/HELP_SCRIPTS"
            /bin/sh create_conf.sh ${info[3]} ${info[2]} "/proj/streamstore-PG0/experiment_outputs/${info[0]}__"
            cd "${LOCAL_SETUP_DIR}/bin"
            sudo ./redis-server ${info[3]}

	    ps aux | grep redis-server
            sleep 3
	    sudo ./redis-cli -p 8000 --cluster add-node ${info[1]}:${info[2]} ${MASTER_HOST}:${MASTER_PORT}
EOF
2>&1)
    echo "$tko"
    done
done
#
