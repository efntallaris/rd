#!/bin/bash -e

source ../config.sh
file_name="${REDIS_SRC_DIR}/cluster.c"
echo ${file_name}

experimental_dir="${EXPERIMENTAL_OUTPUT_DIR}/DELAYS_${EXPERIMENT_NAME}"
if [ ! -d "$experimental_dir" ]; then
    sudo mkdir -p "$experimental_dir"
fi

declare -A source_lines
source_lines["no_sleep"]=""
source_lines["2_ms_sleep"]="usleep(2000);"
source_lines["4_ms_sleep"]="usleep(4000);"
source_lines["8_ms_sleep"]="usleep(8000);"

for pattern in "${!source_lines[@]}"; do
    for redisInstance in "${!instances[@]}"; do
        echo "Processing $redisInstance..."
        IFS='|' read -r -a info <<< "${instances[$redisInstance]}"
        node_host=${info[1]}
        echo "Attempting to run script on $node_host"
        output=$(ssh -o StrictHostKeyChecking=no "$node_host" sudo bash <<EOF
            echo "Running on $node_host"
            cp "$file_name" "$file_name.bak"

            # Define append_text function within the SSH session
            append_text() {
                local pattern="\$1"
                local text="\$2"
                local files=("\${@:3}")

                for file in "\${files[@]}"; do
                    sed -i.bak "/\$pattern/ {
                        N
                        /\n$/ {
                            s/\(.*\)\n\(.*\)/\1\n\$text\n\2/
                        }
                        /\n[^[:space:]]/! {
                            s/\(.*\)/\1\n\$text/
                        }
                    }" "\$file"
                done
            }

            append_text "EXPERIMENTAL LINE TO BE CHANGED FROM SCRIPT" "${source_lines[$pattern]}" "$file_name"
EOF
        2>&1)

        echo "$output"
    done

    # Rest of your code within the pattern loop
    cd "${REDIS_MAIN_SCRIPT_DIR}/experimental_scripts"
    sudo ./experimental_setup.sh

    sudo cp -rf "${EXPERIMENTAL_OUTPUT_DIR}/exp_${EXPERIMENT_NAME}" "${experimental_dir}/${pattern}"

    for redisInstance in "${!instances[@]}"; do
        echo "Processing $redisInstance..."
        IFS='|' read -r -a info <<< "${instances[$redisInstance]}"
        node_host=${info[1]}
        echo "Attempting to run script on $node_host"
        output=$(ssh -o StrictHostKeyChecking=no "$node_host" sudo bash <<EOF
            echo "Running on $node_host"
sudo rm -rf ${file_name}
sudo cp -rf ${file_name}.bak ${file_name}
EOF
        2>&1)

        echo "$output"
    done
done
