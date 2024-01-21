#!/bin/bash -e
REDIS_MAIN_SCRIPT_DIR="/root/rd/scripts_copy/SETUP_CLUSTER/SCRIPTS"
OUTPUT_EXPERIMENT_DIR_AT_YCSB_NODE="/root/experimental_dir"
REDIS_LOG_DIR="/proj/streamstore-PG0/experiment_outputs"
SETUP_NODE="redis0"
YCSB_NODE="ycsb0"

#running workload
declare -A redis_experiments
#redis_experiments["experiment_write_heavy"]="workloadwriteheavy|workload_write_heavy"
#redis_experiments["experiment_read_heavy"]="workloadreadheavy|workload_read_heavy"
#redis_experiments["experiment_read_only"]="workloadreadonly|workload_read_only"
#redis_experiments["experiment_write_only"]="workloadwriteonly|workload_write_only"
#redis_experiments["experiment_update_only"]="workloadupdateonly|workload_update_only"
redis_experiments["experiment_fulva_95_5"]="workloadfulva955|workload_fulva_95_5"
redis_experiments["experiment_fulva_50_50"]="workloadfulva5050|workload_fulva_50_50"

#SETUP_NODE=$(nslookup ${SETUP_NODE} | grep 'Address:' | grep -v '#' | awk '{ print $2 }')
#YCSB_NODE=$(nslookup ${YCSB_NODE} | grep 'Address:' | grep -v '#' | awk '{ print $2 }')
SETUP_NODE="10.10.1.1"
YCSB_NODE="10.10.1.5"



tko=$(sudo ssh -o StrictHostKeyChecking=no ${YCSB_NODE} bash <<EOF
	if [ ! -d "$OUTPUT_EXPERIMENT_DIR_AT_YCSB_NODE" ]; then
	    # If it doesn't exist, create it
	    mkdir -p "$OUTPUT_EXPERIMENT_DIR_AT_YCSB_NODE"
	    echo "Directory created: $OUTPUT_EXPERIMENT_DIR_AT_YCSB_NODE"
	else
	    echo "Directory already exists: $OUTPUT_EXPERIMENT_DIR_AT_YCSB_NODE"
	fi
	cd ${OUTPUT_EXPERIMENT_DIR_AT_YCSB_NODE}
	sudo rm -rf *
EOF
2>&1)
echo "$tko"

for redis_experiment in "${!redis_experiments[@]}"; do
	IFS=',' read -r -a expInstance <<< "${redis_experiments[$redis_experiment]}"
	for i in ${!expInstance[@]};
	do
		IFS="|" read -r -a info <<< "${expInstance[i]}"
		tko=$(sudo ssh -o StrictHostKeyChecking=no ${SETUP_NODE} bash <<EOF
					cd ${REDIS_MAIN_SCRIPT_DIR}
					chmod +x setup_1.sh
					sudo ./setup_1.sh
EOF
2>&1)
		echo "$tko"

		tko=$(sudo ssh -o StrictHostKeyChecking=no ${YCSB_NODE} bash <<EOF
					cd ${REDIS_MAIN_SCRIPT_DIR}
					echo "Running script setup_2_workload"
						
					chmod +x setup_2_workload_.sh
					./setup_2_workload_.sh ${info[0]}
EOF
2>&1)
		echo "$tko"
		echo "SLEEPING FOR 3 MINUTES"
		sleep 3m 

		tko=$(sudo ssh -o StrictHostKeyChecking=no ${YCSB_NODE} bash <<EOF
            		echo "KILLING SCRIPTS"
			cd ${REDIS_MAIN_SCRIPT_DIR}
			sudo ./kill_scripts.sh
			cd ${OUTPUT_EXPERIMENT_DIR_AT_YCSB_NODE}
			mkdir ${info[1]}
			cd ${info[1]}
			#todo change to variable
			cp -rf /root/ycsb_output .
			sudo cp -rf ${REDIS_LOG_DIR} .
			sudo rm -rf /root/ycsb_output
EOF
2>&1)
	done
done


