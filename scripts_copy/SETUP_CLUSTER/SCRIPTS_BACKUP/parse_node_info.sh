#!/bin/bash -e
LOCAL_IP=$(ifconfig eno1 | grep inet | awk -F"inet " '{print $2}' | awk -F' ' '{print $1}')



#	MASTER NODES 	#
declare -A redis_master_instances 
redis_master_instances["redis-0"]="redis0|192.168.20.2|8000|"
redis_master_instances["redis-1"]="redis1|192.168.20.3|8000|"
redis_master_instances["redis-2"]="redis2|192.168.20.1|8000|"
redis_master_instances["redis-3"]="redis3|192.168.20.4|8000|"



#	SLAVE NODES 	#
declare -A redis_slave_instances
#redis_slave_instances["redis-1"]="redis1|node03|7001|/home/entallaris/node02.conf|node-2.aof|dump-2.rdb"

final_string=""
for redis_instance in "${!redis_master_instances[@]}"; do
        echo    "$redis_instance - ${redis_master_instances[$redis_instance]}"
        IFS=',' read -r -a nodeInstance <<< "${redis_master_instances[$redis_instance]}"
        for i in ${!nodeInstance[@]};
        do
                IFS="|" read -r -a info <<< "${nodeInstance[i]}"
		redis_node_info=$(redis-cli -c -h ${info[1]} -p ${info[2]} info)
		redis_node_info=$(echo -e "NODENAME:${info[1]}\n${redis_node_info}")
		echo "${redis_node_info}"
        done
done
