#!/bin/bash -e
declare -A redis_donors
declare -A redis_recipients



BIN_DIR="/home/entallaris/redis_bin"
SCRIPT_DIR="/mnt/stratos/redis/scripts"
HOME_DIR="/home/entallaris"


redis_donors["redis2"]="redis2|node03|/home/entallaris/192.168.20.3_mpstat.txt|/home/entallaris/192.168.20.3_iostat.txt|/home/entallaris/192.168.20.3_ifstat.txt"
redis_donors["redis0"]="redis0|node01|/home/entallaris/192.168.20.1_mpstat.txt|/home/entallaris/192.168.20.1_iostat.txt|/home/entallaris/192.168.20.1_ifstat.txt"
redis_donors["redis1"]="redis1|node02|/home/entallaris/192.168.20.2_mpstat.txt|/home/entallaris/192.168.20.2_iostat.txt|/home/entallaris/192.168.20.2_ifstat.txt"
redis_donors["redis4"]="redis3|node04|/home/entallaris/192.168.20.4_mpstat.txt|/home/entallaris/192.168.20.4_iostat.txt|/home/entallaris/192.168.20.4_ifstat.txt"

redis_recipients["redis3"]="redis3|node04|9000|"





for redisInstance in "${!redis_donors[@]}"; do
        echo    "$redisInstance - ${redis_donors[$redisInstance]}"
        IFS=',' read -r -a nodeInstance <<< "${redis_donors[$redisInstance]}"
        for i in ${!nodeInstance[@]};
        do
                IFS="|" read -r -a info <<< "${nodeInstance[i]}"
                echo "running script on $redisInstance , ${info[0]}"
                tko=$(sshpass -p 'Leros2012!!' ssh -T -oStrictHostKeyChecking=no -p 2222 entallaris@${info[1]} <<-EOF 
(nohup mpstat 1 >>  ${info[2]})&
(nohup iostat -t 1 >> ${info[3]})&
(nohup ifstat -t 1 >> ${info[4]})&
EOF
)
        echo ${tko}
        done
done

