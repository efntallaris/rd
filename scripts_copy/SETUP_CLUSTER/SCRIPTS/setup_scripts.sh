#!/bin/bash -e
declare -A redis_donors
declare -A redis_recipients



LOCAL_SETUP_DIR="/root/rd/redis_bin"
SCRIPT_DIR="/mnt/stratos/redis/scripts"
HOME_DIR="/home/entallaris"

declare -A redis_master_instances 
redis_master_instances["redis-0"]="redis0|10.10.1.1|8000|/root/node01.conf"
redis_master_instances["redis-1"]="redis1|10.10.1.2|8000|/root/node02.conf"
redis_master_instances["redis-2"]="redis2|10.10.1.3|8000|/root/node03.conf"
redis_master_instances["redis-3"]="redis3|10.10.1.4|8000|/root/node04.conf"

IF_STAT="_ifstat.txt"
MP_STAT="_mpstat.txt"
IO_STAT="_iostat.txt"
LOCAL_LOG_DIR="/root/systat_logs"
# Check if the directory exists
if [ ! -d "$LOCAL_LOG_DIR" ]; then
    # If it doesn't exist, create it
    mkdir -p "$LOCAL_LOG_DIR"
    echo "Directory created: $LOCAL_LOG_DIR"
else
    echo "Directory already exists: $LOCAL_LOG_DIR"
fi


for redisInstance in "${!redis_master_instances[@]}"; do
        echo    "$redisInstance - ${redis_master_instances[$redisInstance]}"
        IFS=',' read -r -a nodeInstance <<< "${redis_master_instances[$redisInstance]}"
        for i in ${!nodeInstance[@]};
        do
                IFS="|" read -r -a info <<< "${nodeInstance[i]}"
                echo "running script on $redisInstance , ${info[0]}"

		tko=$(sudo ssh -o StrictHostKeyChecking=no ${info[1]} bash <<EOF
			# Check if the directory exists
			if [ ! -d "$LOCAL_LOG_DIR" ]; then
			    # If it doesn't exist, create it
			    mkdir -p "$LOCAL_LOG_DIR"
			    echo "Directory created: $LOCAL_LOG_DIR"
			else
			    echo "Directory already exists: $LOCAL_LOG_DIR"
			fi
			echo "${LOCAL_LOG_DIR}/${info[1]}${MP_STAT}"
			(nohup mpstat 1 >> "${LOCAL_LOG_DIR}/${info[1]}${MP_STAT}" 2>&1 </dev/null &)

			(nohup iostat -t 1 >> "${LOCAL_LOG_DIR}/${info[1]}${IO_STAT}" 2>&1 </dev/null &)
			(nohup ifstat -t 1 >> "${LOCAL_LOG_DIR}/${info[1]}${IF_STAT}" 2>&1 </dev/null &)
EOF
2>&1)
        echo ${tko}
        done
done


