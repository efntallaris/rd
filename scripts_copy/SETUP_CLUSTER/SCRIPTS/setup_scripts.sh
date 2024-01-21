#!/bin/bash -e
source ./config.sh

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


