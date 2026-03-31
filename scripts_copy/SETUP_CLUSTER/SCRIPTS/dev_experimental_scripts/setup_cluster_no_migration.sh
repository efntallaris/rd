#!/bin/bash -e
REDIS_MAIN_SCRIPT_DIR="/mnt/stratos/redis/scripts/SETUP_CLUSTER/SCRIPTS"
OUTPUT_EXPERIMENT_DIR_AT_YCSB_NODE="/home/entallaris/experimental_dir"
REDIS_LOG_DIR="/mnt/stratos/redis/logs"
SETUP_NODE="192.168.20.1"
YCSB_NODE="192.168.20.5"

#running workload
declare -A redis_experiments
#redis_experiments["experiment_read_heavy"]="workloadreadheavy|workload_read_heavy"
#redis_experiments["experiment_read_only"]="workloadreadonly|workload_read_only"
#redis_experiments["experiment_write_heavy"]="workloadwriteheavy|workload_write_heavy"
#redis_experiments["experiment_write_only"]="workloadwriteonly|workload_write_only"
redis_experiments["experiment_update_only"]="workloadupdateonly|workload_update_only"
#TO run redis with detailed logs
#sudo ./redis-server ${info[3]} --loglevel debug

#TO run redis 
#sudo ./redis-server ${info[3]} 


cd ${REDIS_MAIN_SCRIPT_DIR}
./kill_scripts.sh
./clean_cluster.sh

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
		echo "RUNNING ${info[0]}"

		tko=$(sshpass -p 'Leros2012!!' ssh -T -oStrictHostKeyChecking=no -p 2222 entallaris@${SETUP_NODE} <<-EOF
			cd ${REDIS_MAIN_SCRIPT_DIR}
			./setup_cluster_experiments.sh ${info[0]}
		EOF
		)
		echo ${tko}
		tko=$(sshpass -p 'Leros2012!!' ssh -T -oStrictHostKeyChecking=no -p 2222 entallaris@${YCSB_NODE} <<-EOF
			cd ${REDIS_MAIN_SCRIPT_DIR}
			./setup_scripts.sh
			./setup_cluster_experiments_2_without_migration.sh ${info[0]}
		EOF
		)
		echo "SLEEPING FOR 3 MINUTES"
		sleep 3m 
		tko=$(sshpass -p 'Leros2012!!' ssh -T -oStrictHostKeyChecking=no -p 2222 entallaris@${YCSB_NODE} <<-EOF
			cd ${REDIS_MAIN_SCRIPT_DIR}
			sudo ./kill_scripts.sh
			sudo ./clean_cluster.sh
			cd ${OUTPUT_EXPERIMENT_DIR_AT_YCSB_NODE}
			sudo mkdir ${info[1]}
			cd ${info[1]}
			#todo change to variable
			sudo cp -rf /home/entallaris/ycsb_output .
			sudo cp -rf ${REDIS_LOG_DIR} .
			sudo rm -rf /home/entallaris/ycsb_output
			
		EOF
		)
		#echo $tko

	done
done


