#!/bin/bash -e
REDIS_MAIN_SCRIPT_DIR="/mnt/stratos/redis/scripts/SETUP_CLUSTER/SCRIPTS/experimental_scripts"
OUTPUT_EXPERIMENT_DIR_AT_YCSB_NODE="/home/entallaris/experimental_dir"
REDIS_LOG_DIR="/mnt/stratos/redis/logs"
FINAL_EXPERIMENTAL_DIR="/home/entallaris/test_dir"

# RUN THIS SCRIPT FROM NODE05 (YCSB CLIENT) only

if [ ! -d "$FINAL_EXPERIMENTAL_DIR" ]; then
    mkdir -p "$FINAL_EXPERIMENTAL_DIR"
    echo "Folder created: $FINAL_EXPERIMENTAL_DIR"
    else
    echo "Folder already exists: $FINAL_EXPERIMENTAL_DIR"
fi

declare -A source_lines
#source_lines["no_sleep"]="6467| |EXPERIMENTAL LINE TO BE CHANGED FROM SCRIPT"
#source_lines["4_ms_sleep"]="6496|usleep(4000);|EXPERIMENTAL LINE TO BE CHANGED FROM SCRIPT"
source_lines["8_ms_sleep"]="6469|usleep(8000);|EXPERIMENTAL LINE TO BE CHANGED FROM SCRIPT"
#source_lines["16_ms_sleep"]="6496|usleep(16000);|EXPERIMENTAL LINE TO BE CHANGED FROM SCRIPT"
#source_lines["30_7_ms_sleep"]="6461|usleep(37000);|EXPERIMENTAL LINE TO BE CHANGED FROM SCRIPT"

file_name="/mnt/stratos/redis/src/redis/src/cluster.c"

for source_line in "${!source_lines[@]}"; do
IFS=',' read -r -a expInstance <<< "${source_lines[$source_line]}"
for i in ${!expInstance[@]};
do
IFS="|" read -r -a info <<< "${expInstance[i]}"
line_number=${info[0]};
previous_line_number=$(($line_number-1))
    new_content=${info[1]};
experimental_line=${info[2]};

previous_line_content=$(sed -n "${previous_line_number}p" "$file_name");
line_content=$(sed -n "${line_number}p" "$file_name")
#echo "Line $line_number contains ${line_content}"
#echo "Line $previous_line_number contains ${previous_line_content}"

if [[ ( -z "$line_content" || "$line_content" =~ ^[[:space:]]*$ || "$line_content" == *"usleep"* ) && "$previous_line_content" == *"${experimental_line}"* ]]; then
# Replace the line
sed -i "${line_number}s/.*/${new_content}/" "$file_name"

echo "Line $line_number replaced with: $new_content"
    echo "Running experiment ${source_line}"
    current_minute=$(date +%M | sed 's/^0//')  # Remove leading zero
    if ((current_minute >= 50)); then
    echo "Sleeping for 10 minutes..."
    sleep 10m 
    echo "Awake!"
    fi

    cd ${REDIS_MAIN_SCRIPT_DIR}
    experimental_output=$(sudo ./experimental_setup_processes_workloads.sh > /dev/null 2>&1)
    sudo cp -rf ${OUTPUT_EXPERIMENT_DIR_AT_YCSB_NODE} ${FINAL_EXPERIMENTAL_DIR}/${source_line}

    else
    echo "Line $line_number is not empty and does not contain 'usleep' or previous line on line ${previous_line_number} is not ${experimental_line}"
    fi	
    done
    done


