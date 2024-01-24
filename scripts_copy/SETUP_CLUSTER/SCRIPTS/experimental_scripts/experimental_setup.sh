#!/bin/bash -e

MAIN_DIR="/root/rd"
REDIS_SRC_DIR="/root/rd/src"
YCSB_SRC_DIR="/root/rd/ycsb_client"
REDIS_MAIN_SCRIPT_DIR="/root/rd/scripts_copy/SETUP_CLUSTER/SCRIPTS"
LOCAL_SETUP_DIR="/root/rd/redis_bin"
YCSB_DIR="/root/ycsb_client"
YCSB_LOG_FILENAME="/tmp/ycsb_output"
MASTER_HOST="10.10.1.1"
MASTER_PORT="8000"
REDIS_WORKLOAD="workloadfulva5050"
YCSB_LOADER_INSTANCE="10.10.1.6"
REDIS_LOG_DIR="/proj/streamstore-PG0/experiment_outputs"
YCSB_DIR_BIN="/root/ycsb_client/bin"
REDIS_WORKLOAD_PATH="${MAIN_DIR}/workloads/"
YCSB_LOG_FILENAME="/tmp/ycsb_output"
LOCAL_LOG_DIR="/root/systat_logs"
declare -A redis_experiments
#redis_experiments["experiment_write_heavy"]="workloadwriteheavy|workload_write_heavy"
#redis_experiments["experiment_read_heavy"]="workloadreadheavy|workload_read_heavy"
#redis_experiments["experiment_read_only"]="workloadreadonly|workload_read_only"
#redis_experiments["experiment_write_only"]="workloadwriteonly|workload_write_only"
#redis_experiments["experiment_update_only"]="workloadupdateonly|workload_update_only"
redis_experiments["experiment_fulva_95_5"]="workloadfulva955|workload_fulva_95_5"
redis_experiments["experiment_fulva_50_50"]="workloadfulva5050|workload_fulva_50_50"

tko=$(sudo ssh -o StrictHostKeyChecking=no ${YCSB_LOADER_INSTANCE} bash <<EOF
	if [ ! -d "$REDIS_LOG_DIR" ]; then
	    # If it doesn't exist, create it
	    mkdir -p "$REDIS_LOG_DIR"
	    echo "Directory created: $REDIS_LOG_DIR"
	else
	    echo "Directory already exists: $REDIS_LOG_DIR"
	fi
	#cd ${REDIS_LOG_DIR}
	#sudo rm -rf *
EOF
2>&1)
echo "$tko"

for redis_experiment in "${!redis_experiments[@]}"; do
	IFS=',' read -r -a expInstance <<< "${redis_experiments[$redis_experiment]}"
	for i in ${!expInstance[@]};
	do
		IFS="|" read -r -a info <<< "${expInstance[i]}"
		tko=$(sudo ssh -o StrictHostKeyChecking=no ${MASTER_NODE} bash <<EOF
					cd ${REDIS_MAIN_SCRIPT_DIR}
					chmod +x setup_1.sh
					sudo ./setup_1.sh
EOF
2>&1)
		echo "$tko"

		tko=$(sudo ssh -o StrictHostKeyChecking=no ${YCSB_LOADER_INSTANCE} bash <<EOF
					cd ${REDIS_MAIN_SCRIPT_DIR}
					echo "Running script setup_2_workload"
						
					chmod +x setup_2_workload_.sh
					./setup_2_workload_.sh ${info[0]}
EOF
2>&1)
		echo "$tko"
		echo "SLEEPING FOR 5 MINUTES"
		sleep 5m 

		timestamp=$(date '+%Y_%m_%d_%H_%M_%S')
		exp_dir=${info[1]}_${timestamp}
		cd ${REDIS_LOG_DIR}
		mkdir -p ${exp_dir}
		mkdir -p ${exp_dir}/logs

		for redis_instance in "${!redis_ycsb_instances[@]}"; do
		    echo "$redis_instance - ${redis_ycsb_instances[$redis_instance]}"
		    IFS=',' read -r -a nodeInstance <<< "${redis_ycsb_instances[$redis_instance]}"
		    for i in "${!nodeInstance[@]}"; do
			IFS="|" read -r -a info <<< "${nodeInstance[i]}"
			echo "running script on $redis_instance, ${info[1]} port ${info[2]}"
			tko=$(sudo ssh -o StrictHostKeyChecking=no ${info[1]} bash <<EOF
				cd ${REDIS_LOG_DIR}
				cd ${exp_dir}
				cp -rf /tmp/ycsb_output_* .
				sudo rm -rf /tmp/ycsb_output_*
EOF
2>&1)
		    echo "$tko"
		    done
		done

		for redis_instance in "${!redis_master_instances[@]}"; do
		    echo "$redis_instance - ${redis_master_instances[$redis_instance]}"
		    IFS=',' read -r -a nodeInstance <<< "${redis_master_instances[$redis_instance]}"
		    for i in "${!nodeInstance[@]}"; do
			IFS="|" read -r -a info <<< "${nodeInstance[i]}"
			echo "running script on $redis_instance, ${info[1]} port ${info[2]}"
			tko=$(sudo ssh -o StrictHostKeyChecking=no ${info[1]} bash <<EOF
			    cd ${REDIS_LOG_DIR}
			    cd ${exp_dir}
			    cd logs
			    cp -rf ${LOCAL_LOG_DIR}/* .
			    rm -rf ${LOCAL_LOG_DIR}
			    cp -rf ${REDIS_LOG_DIR}/* .
		            rm -rf ${REDIS_LOG_DIR}/*
EOF
2>&1)
    echo "$tko"
    done
done

		tko=$(sudo ssh -o StrictHostKeyChecking=no ${YCSB_LOADER_INSTANCE} bash <<EOF
            		echo "KILLING SCRIPTS"
			cd ${REDIS_MAIN_SCRIPT_DIR}
			sudo ./kill_scripts.sh
EOF
2>&1)
	done
done


