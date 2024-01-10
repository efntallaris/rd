#!/bin/bash -e
REDIS_MAIN_SCRIPT_DIR="/mnt/stratos/redis/scripts/SETUP_CLUSTER/SCRIPTS"
OUTPUT_EXPERIMENT_DIR_AT_YCSB_NODE="/home/entallaris/experimental_dir"
REDIS_LOG_DIR="/mnt/stratos/redis/logs"
SETUP_NODE="192.168.20.1"
YCSB_NODE="192.168.20.5"

#running workload
declare -A redis_experiments
#redis_experiments["experiment_write_heavy"]="workloadwriteheavy|workload_write_heavy"
#redis_experiments["experiment_read_heavy"]="workloadreadheavy|workload_read_heavy"
#redis_experiments["experiment_read_only"]="workloadreadonly|workload_read_only"
redis_experiments["experiment_write_only"]="workloadwriteonly|workload_write_only"
redis_experiments["experiment_update_only"]="workloadupdateonly|workload_update_only"
#TO run redis with detailed logs
#sudo ./redis-server ${info[3]} --loglevel debug

#TO run redis 
#sudo ./redis-server ${info[3]} 



tko=$(sshpass -p 'Leros2012!!' ssh -T -oStrictHostKeyChecking=no -p 2222 entallaris@${YCSB_NODE} <<-EOF
	cd /home/entallaris/experimental_dir
	sudo rm -rf *
EOF
)
echo $tko

for redis_experiment in "${!redis_experiments[@]}"; do
	IFS=',' read -r -a expInstance <<< "${redis_experiments[$redis_experiment]}"
	for i in ${!expInstance[@]};
	do
		IFS="|" read -r -a info <<< "${expInstance[i]}"
		echo ${info[0]}

		tko=$(sshpass -p 'Leros2012!!' ssh -T -oStrictHostKeyChecking=no -p 2222 entallaris@${SETUP_NODE} <<-EOF
			cd ${REDIS_MAIN_SCRIPT_DIR}
			./setup_1_dev.sh
		EOF
		)
		tko=$(sshpass -p 'Leros2012!!' ssh -T -oStrictHostKeyChecking=no -p 2222 entallaris@${YCSB_NODE} <<-EOF
			cd ${REDIS_MAIN_SCRIPT_DIR}
			echo "Running script setup_2_workload"
			./setup_2_workload_dev.sh ${info[0]}
		EOF
		)
		echo ${tko}
		echo "SLEEPING FOR 2 MINUTES"
		sleep 2m 
		tko=$(sshpass -p 'Leros2012!!' ssh -T -oStrictHostKeyChecking=no -p 2222 entallaris@${YCSB_NODE} <<-EOF
			cd ${REDIS_MAIN_SCRIPT_DIR}
			sudo ./kill_scripts.sh
			cd ${OUTPUT_EXPERIMENT_DIR_AT_YCSB_NODE}
			mkdir ${info[1]}
			cd ${info[1]}
			#todo change to variable
			cp -rf /home/entallaris/ycsb_output .
			sudo cp -rf ${REDIS_LOG_DIR} .
			sudo rm -rf /home/entallaris/ycsb_output
		EOF
		)
		echo $tko

	done
done


