#!/bin/bash -e
declare -A redis_donors
declare -A redis_recipients



BIN_DIR="/home/entallaris/redis_bin"
SCRIPT_DIR="/mnt/stratos/redis/scripts"
HOME_DIR="/home/entallaris"


redis_donors["redis0"]="redis0|node01|9000|apf_redis0|db_redis0"
redis_donors["redis1"]="redis1|node02|9000|apf_redis1|db_redis1"
redis_donors["redis2"]="redis2|node03|9000|apf_redis2|db_redis2"

redis_recipients["redis3"]="redis3|node04|9000|"





for redisInstance in "${!redis_donors[@]}"; do
        echo    "$redisInstance - ${redis_donors[$redisInstance]}"
        IFS=',' read -r -a nodeInstance <<< "${mongoD_instances[$redisInstance]}"
        for i in ${!nodeInstance[@]};
        do
                IFS="|" read -r -a info <<< "${nodeInstance[i]}"
                echo "running script on $redisInstance , ${info[0]}"
                tko=$(sshpass -p 'Leros2012!!' ssh -T -oStrictHostKeyChecking=no -p 2222 entallaris@${info[1]} <<-EOF
cd "${SCRIPT_DIR}"
sudo ./generate_config_file.sh ${HOME_DIR}/${info[0]} ${info[2]} ${info[3]} ${info[4]}
cd "${BIN_DIR}"
sudo ./redis-server ${HOME_DIR}/${info[0]}
EOF
)
        echo ${tko}
        done
done

