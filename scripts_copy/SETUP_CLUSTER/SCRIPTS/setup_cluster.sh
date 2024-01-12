#!/bin/bash -e
LOCAL_IP=$(ifconfig eno1 | grep inet | awk -F"inet " '{print $2}' | awk -F' ' '{print $1}')


MAIN_DIR="/root/rd/"
REDIS_SRC_DIR="/root/rd/src"
YCSB_SRC_DIR="/root/rd/ycsb_client"
REDIS_MAIN_SCRIPT_DIR="/root/rd/scripts_copy/SETUP_CLUSTER/SCRIPTS"
LOCAL_SETUP_DIR="/root/rd/redis_bin"
YCSB_DIR="/root/ycsb_client"

#YCSB_RECORDS="5000000"
YCSB_RECORDS="500"
REDIS_HOST="127.0.0.1"
REDIS_PORT="8000"
REDIS_WORKLOAD="../workloads/workloadreadonly"

YCSB_LOADER_INSTANCE="ycsb0"


# 	COMPILE THE SOURCE CODE		#
cd "${REDIS_SRC_DIR}"
sudo make

#	MASTER NODES 	#
declare -A redis_master_instances 
redis_master_instances["redis-0"]="redis0|10.10.1.1|8000|/root/node01.conf|node-1.aof|dump-1.rdb"
redis_master_instances["redis-1"]="redis1|10.10.1.2|8000|/root/node02.conf|node-2.aof|dump-2.rdb"
redis_master_instances["redis-2"]="redis2|10.10.1.3|8000|/root/node03.conf|node-3.aof|dump-3.rdb"

declare -A redis_migrate_instances
redis_migrate_instances["redis-3"]="redis3|192.168.20.4|8000|/home/entallaris/node04.conf|node-4.aof|dump-4.rdb"
#redis_migrate_instances["redis-4"]="redis4|192.168.20.5|8000|/home/entallaris/node05.conf|node-5.aof|dump-5.rdb"


#       THE NODE WHERE YCSB RUNS        #
declare -A redis_ycsb_instance
redis_ycsb_instance["redis-4"]="redis4|192.168.20.4|8000|/home/entallaris/node05.conf|node-5.aof|dump-5.rdb"

#TO run redis with detailed logs
#sudo ./redis-server ${info[3]} --loglevel debug

#TO run redis 
#sudo ./redis-server ${info[3]} 

for redis_instance in "${!redis_master_instances[@]}"; do
        echo    "$redis_instance - ${redis_master_instances[$redis_instance]}"
        IFS=',' read -r -a nodeInstance <<< "${redis_master_instances[$redis_instance]}"
        for i in ${!nodeInstance[@]};
        do
                IFS="|" read -r -a info <<< "${nodeInstance[i]}"
                echo "running script on $redis_instance , ${info[1]} port ${info[2]}"
		tko=$(
		    sudo ssh -o StrictHostKeyChecking=no ${info[1]} <<-EOF
			sudo su
			sudo rm -rf ${info[3]}
			sudo rm -rf /home/entallaris/${info[4]}
			sudo rm -rf /home/entallaris/${info[5]}
			cd "${REDIS_SRC_DIR}"
			sudo make PREFIX="${LOCAL_SETUP_DIR}" install
			cd "${REDIS_MAIN_SCRIPT_DIR}/HELP_SCRIPTS"
			/bin/sh create_conf.sh ${info[3]} ${info[2]} ${info[4]} ${info[5]} "\"/proj/streamstore-PG0/experiment_outputs/${info[0]}__\""
			cd "${LOCAL_SETUP_DIR}/bin"
			sudo ./redis-server ${info[3]}
		EOF
		)

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

#echo "SETTING UP CLUSTER"
#clusterCreateCommand="$clusterCreateCommand --cluster-yes"
#echo "cluster create command is ${clusterCreateCommand}"
#eval "$clusterCreateCommand"


sleep 5
tko=$(
    sudo ssh -o StrictHostKeyChecking=no ${YCSB_LOADER_INSTANCE} <<-EOF
	cd "${REDIS_MAIN_SCRIPT_DIR}/"
	sudo /bin/sh build_ycsb.sh ${YCSB_SRC_DIR} ${YCSB_DIR}
	cd ${YCSB_DIR}/bin
	sudo ./ycsb.sh load redis -p "redis.host=${REDIS_HOST}" -p "redis.port=${REDIS_PORT}" -p "redis.cluster=true" -P ${REDIS_WORKLOAD} -threads 100
EOF
)
#	ycsbCommand=$(sudo ./ycsb load redis -p "redis.host=${REDIS_HOST}" -p "redis.port=${REDIS_PORT}" -p "redis.cluster=true" -P ${REDIS_WORKLOAD} -threads 100)
#	sleep 10

