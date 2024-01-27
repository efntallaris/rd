#!/bin/bash -e
LOCAL_IP=$(ifconfig eno1 | grep inet | awk -F"inet " '{print $2}' | awk -F' ' '{print $1}')


source ./config.sh

for redis_instance in "${!redis_master_instances[@]}"; do
    echo "$redis_instance - ${redis_master_instances[$redis_instance]}"
    IFS=',' read -r -a nodeInstance <<< "${redis_master_instances[$redis_instance]}"
    for i in "${!nodeInstance[@]}"; do
        IFS="|" read -r -a info <<< "${nodeInstance[i]}"
        echo "running script on $redis_instance, ${info[1]} port ${info[2]}"
        tko=$(sudo ssh -o StrictHostKeyChecking=no ${info[1]} bash <<EOF
	    ulimit -n 65535
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


cd ${LOCAL_SETUP_DIR}/bin
pwd
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


# INSTALL YCSB IN YCSB_INSTANCES
for redis_instance in "${!redis_ycsb_instances[@]}"; do
    echo "$redis_instance - ${redis_ycsb_instances[$redis_instance]}"
    IFS=',' read -r -a nodeInstance <<< "${redis_ycsb_instances[$redis_instance]}"
    for i in "${!nodeInstance[@]}"; do
        IFS="|" read -r -a info <<< "${nodeInstance[i]}"
        echo "running script on $redis_instance, ${info[1]} port ${info[2]}"
        tko=$(sudo ssh -o StrictHostKeyChecking=no ${info[1]} bash <<EOF
		cd "${REDIS_MAIN_SCRIPT_DIR}/"
		chmod +x build_ycsb.sh
		sudo ./build_ycsb.sh ${YCSB_SRC_DIR} ${YCSB_DIR}
EOF
2>&1)
    echo "$tko"
    done
done



for redis_instance in "${!redis_migrate_instances[@]}"; do
    echo "$redis_instance - ${redis_migrate_instances[$redis_instance]}"
    IFS=',' read -r -a nodeInstance <<< "${redis_migrate_instances[$redis_instance]}"
    for i in "${!nodeInstance[@]}"; do
        IFS="|" read -r -a info <<< "${nodeInstance[i]}"
        echo "running script on $redis_instance, ${info[1]} port ${info[2]}"
        tko=$(sudo ssh -o StrictHostKeyChecking=no ${info[1]} bash <<EOF
	    ulimit -n 65535
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
