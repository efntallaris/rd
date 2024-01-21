#!/bin/bash -e
REDIS_MAIN_SCRIPT_DIR="/root/rd/scripts_copy/SETUP_CLUSTER/SCRIPTS"
OUTPUT_EXPERIMENT_DIR="/proj/streamstore-PG0/experiments"
REDIS_LOG_DIR="/proj/streamstore-PG0/experiment_outputs"
SETUP_NODE="redis0"
YCSB_NODE="ycsb0"
LOCAL_LOG_DIR="/root/systat_logs"

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
YCSB_NODE="10.10.1.6"


LOCAL_LOG_DIR="/root/systat_logs"

#	MASTER NODES 	#
declare -A redis_master_instances 
redis_master_instances["redis-0"]="redis0|10.10.1.1|8000|/root/node01.conf"
redis_master_instances["redis-1"]="redis1|10.10.1.2|8000|/root/node02.conf"
redis_master_instances["redis-2"]="redis2|10.10.1.3|8000|/root/node03.conf"
redis_master_instances["redis-3"]="redis2|10.10.1.4|8000|/root/node04.conf"

declare -A redis_migrate_instances
redis_migrate_instances["redis-4"]="redis3|10.10.1.5|8000|/root/node05.conf"

declare -A redis_ycsb_instances
redis_ycsb_instances["ycsb0"]="ycsb0|10.10.1.6"
redis_ycsb_instances["ycsb1"]="ycsb1|10.10.1.7"
redis_ycsb_instances["ycsb2"]="ycsb2|10.10.1.8"

tko=$(sudo ssh -o StrictHostKeyChecking=no ${YCSB_NODE} bash <<EOF
	if [ ! -d "$OUTPUT_EXPERIMENT_DIR" ]; then
	    # If it doesn't exist, create it
	    mkdir -p "$OUTPUT_EXPERIMENT_DIR"
	    echo "Directory created: $OUTPUT_EXPERIMENT_DIR"
	else
	    echo "Directory already exists: $OUTPUT_EXPERIMENT_DIR"
	fi
	#cd ${OUTPUT_EXPERIMENT_DIR}
	#sudo rm -rf *
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

		timestamp=$(date '+%Y_%m_%d_%H_%M_%S')
		exp_dir=${info[1]}_${timestamp}
		cd ${OUTPUT_EXPERIMENT_DIR}
		mkdir -p ${exp_dir}
		mkdir -p ${exp_dir}/logs

		for redis_instance in "${!redis_ycsb_instances[@]}"; do
		    echo "$redis_instance - ${redis_ycsb_instances[$redis_instance]}"
		    IFS=',' read -r -a nodeInstance <<< "${redis_ycsb_instances[$redis_instance]}"
		    for i in "${!nodeInstance[@]}"; do
			IFS="|" read -r -a info <<< "${nodeInstance[i]}"
			echo "running script on $redis_instance, ${info[1]} port ${info[2]}"
			tko=$(sudo ssh -o StrictHostKeyChecking=no ${info[1]} bash <<EOF
				cd ${OUTPUT_EXPERIMENT_DIR}
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
			    cd ${OUTPUT_EXPERIMENT_DIR}
			    cd ${exp_dir}
			    cd logs
			    cp -rf ${LOCAL_LOG_DIR}/* .
			    rm -rf ${LOCAL_LOG_DIR}
EOF
2>&1)
    echo "$tko"
    done
done

		tko=$(sudo ssh -o StrictHostKeyChecking=no ${YCSB_NODE} bash <<EOF
            		echo "KILLING SCRIPTS"
			cd ${REDIS_MAIN_SCRIPT_DIR}
			sudo ./kill_scripts.sh
EOF
2>&1)
	done
done


