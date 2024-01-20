#!/bin/bash -e


MAIN_DIR="/root/rd"
#	MASTER NODES 	#
declare -A redis_master_instances 
redis_master_instances["redis-1"]="redis1|130.127.134.73|8000|/root/node02.conf"
redis_master_instances["redis-2"]="redis2|130.127.134.96|8000|/root/node03.conf"
redis_master_instances["redis-3"]="redis3|130.127.134.75|8000|/root/node03.conf"
redis_master_instances["ycsb-0"]="redis4|130.127.134.81"

for redis_instance in "${!redis_master_instances[@]}"; do
    echo "$redis_instance - ${redis_master_instances[$redis_instance]}"
    IFS=',' read -r -a nodeInstance <<< "${redis_master_instances[$redis_instance]}"
    for i in "${!nodeInstance[@]}"; do
        IFS="|" read -r -a info <<< "${nodeInstance[i]}"
        tko=$(sudo ssh -o StrictHostKeyChecking=no ${info[1]} bash <<EOF
        sudo rm -rf ${MAIN_DIR}
	cd /root
	git clone https://github.com/efntallaris/rd
	cd rd/scripts_copy/
	chmod +x install_preqs.sh
	./install_preqs.sh
	
EOF
2>&1)

    	echo "$tko"
    done
done
