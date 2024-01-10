#!/bin/bash -e
LOCAL_IP=$(ifconfig eno1 | grep inet | awk -F"inet " '{print $2}' | awk -F' ' '{print $1}')


REDIS_SRC_DIR="/mnt/stratos/redis/src/redis/src"
REDIS_MAIN_SCRIPT_DIR="/mnt/stratos/redis/scripts/SETUP_CLUSTER/SCRIPTS"
LOCAL_SETUP_DIR="/home/entallaris/redis_bin"
YCSB_DIR="/mnt/stratos/modded_redis/run_workload_redis/bin"

#YCSB_RECORDS="5000000"
REDIS_HOST="127.0.0.1"
REDIS_PORT="8000"
REDIS_WORKLOAD_PATH="../../workloads_dev/"
REDIS_WORKLOAD_NAME=$1
REDIS_WORKLOAD=${REDIS_WORKLOAD_PATH}${REDIS_WORKLOAD_NAME}

# 	COMPILE THE SOURCE CODE		#
cd "${REDIS_SRC_DIR}"
sudo make 


#	MASTER NODES 	#
declare -A redis_master_instances 
redis_master_instances["redis-0"]="redis0|192.168.20.1|8000|/home/entallaris/node01.conf|node-1.aof|dump-1.rdb"
redis_master_instances["redis-1"]="redis1|192.168.20.2|8000|/home/entallaris/node02.conf|node-2.aof|dump-2.rdb"
redis_master_instances["redis-2"]="redis2|192.168.20.3|8000|/home/entallaris/node03.conf|node-3.aof|dump-3.rdb"


#	SLAVE NODES 	#
#declare -A redis_slave_instances
#redis_slave_instances["redis-4"]="redis3|192.168.20.4|8000|/home/entallaris/node04.conf|node-4.aof|dump-4.rdb"


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
		tko=$(sshpass -p 'Leros2012!!' ssh -T -oStrictHostKeyChecking=no -p 2222 entallaris@${info[1]} <<-EOF
			sudo rm -rf ${info[3]}
			sudo rm -rf /home/entallaris/${info[4]}
			sudo rm -rf /home/entallaris/${info[5]}
			cd "${REDIS_SRC_DIR}"
			sudo make PREFIX="${LOCAL_SETUP_DIR}" install
			cd "${REDIS_MAIN_SCRIPT_DIR}/HELP_SCRIPTS"
			sudo ./create_conf.sh ${info[3]} ${info[2]} ${info[4]} ${info[5]} "\"/mnt/stratos/redis/logs/${info[0]}__\"" 
			cd "${LOCAL_SETUP_DIR}/bin"
			echo "RUNNING REDIS SERVER WITH CONFID ${info[3]}"
			sudo ./redis-server ${info[3]}
EOF
		)
        done
done


for redis_instance in "${!redis_slave_instances[@]}"; do
        echo    "$redis_instance - ${redis_slave_instances[$redis_instance]}"
        IFS=',' read -r -a nodeInstance <<< "${redis_slave_instances[$redis_instance]}"
        for i in ${!nodeInstance[@]};
        do
                IFS="|" read -r -a info <<< "${nodeInstance[i]}"
                echo "running script on $redis_instance , ${info[1]} port ${info[2]}"
		tko=$(sshpass -p 'Leros2012!!' ssh -T -oStrictHostKeyChecking=no -p 2222 entallaris@${info[1]} <<-EOF
			cd "${REDIS_SRC_DIR}"
			sudo make PREFIX=${LOCAL_SETUP_DIR} install -j8
			cd "${REDIS_MAIN_SCRIPT_DIR}/HELP_SCRIPTS"
			sudo ./create_conf.sh ${info[3]} ${info[2]} ${info[4]} ${info[5]} "\"/mnt/stratos/redis/logs/${info[0]}\"" 
			cd "${LOCAL_SETUP_DIR}/bin"
			sudo ./redis-server ${info[3]}
EOF
) 
	done
done



for redis_instance in "${!redis_ycsb_instance[@]}"; do
        echo    "$redis_instance - ${redis_ycsb_instance[$redis_instance]}"
        IFS=',' read -r -a nodeInstance <<< "${redis_ycsb_instance[$redis_instance]}"
        for i in ${!nodeInstance[@]};
        do
                IFS="|" read -r -a info <<< "${nodeInstance[i]}"
                echo "running script on $redis_instance , ${info[1]} port ${info[2]}"
		tko=$(sshpass -p 'Leros2012!!' ssh -T -oStrictHostKeyChecking=no -p 2222 entallaris@${info[1]} <<-EOF
			cd "${REDIS_SRC_DIR}"
			sudo make PREFIX=${LOCAL_SETUP_DIR} install -j8
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


sleep 3 
echo "SETTING UP CLUSTER"
clusterCreateCommand="$clusterCreateCommand --cluster-yes"
eval "$clusterCreateCommand"


echo "RUNNING WORKLOAD ${REDIS_WORKLOAD}"
sleep 5
cd $YCSB_DIR
ycsbCommand=$(sudo ./ycsb load redis -p "redis.host=${REDIS_HOST}" -p "redis.port=${REDIS_PORT}" -p "redis.cluster=true" -P ${REDIS_WORKLOAD} -threads 25)
echo ${ycsbCommand}
sleep 10

#sleep 1 h
echo " SETTING UP RECIPIENTS"
for redis_instance in "${!redis_migrate_instances[@]}"; do
	echo    "$redis_instance - ${redis_migrate_instances[$redis_instance]}"
	IFS=',' read -r -a nodeInstance <<< "${redis_migrate_instances[$redis_instance]}"
	for i in ${!nodeInstance[@]};
        do
                IFS="|" read -r -a info <<< "${nodeInstance[i]}"
                echo "running script on $redis_instance , ${info[1]} port ${info[2]}"
		tko=$(sshpass -p 'Leros2012!!' ssh -T -oStrictHostKeyChecking=no -p 2222 entallaris@${info[1]} <<-EOF
			cd "${REDIS_SRC_DIR}"
			sudo make PREFIX="${LOCAL_SETUP_DIR}" install
			cd "${REDIS_MAIN_SCRIPT_DIR}/HELP_SCRIPTS"
			sudo ./create_conf.sh ${info[3]} ${info[2]} ${info[4]} ${info[5]} "\"/mnt/stratos/redis/logs/${info[0]}\"" 
			cd "${LOCAL_SETUP_DIR}/bin"
			echo "RUNNING REDIS SERVER WITH CONFID ${info[3]}"
			sudo ./redis-server ${info[3]}
EOF
)

			sleep 3
			/home/entallaris/redis_bin/bin/redis-cli -p 8000  --cluster add-node ${info[1]}:${info[2]} 192.168.20.1:${info[2]}
        done
done

