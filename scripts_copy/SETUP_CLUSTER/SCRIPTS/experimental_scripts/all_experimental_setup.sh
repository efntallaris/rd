#!/bin/bash -e

source ../config.sh
file_name="${REDIS_SRC_DIR}/cluster.c"
echo ${file_name}

experimental_dir="${EXPERIMENTAL_OUTPUT_DIR}/DELAYS_${EXPERIMENT_NAME}"
if [ ! -d "$experimental_dir" ]; then
    sudo mkdir -p "$experimental_dir"
fi


append_text() {
    local pattern="$1"
    local text="$2"
    local files=("${@:3}")

    for file in "${files[@]}"; do
        # Use sed to find the pattern and append the text
        sed -i.bak "/$pattern/ {
            N
            /\n$/ {
                s/\(.*\)\n\(.*\)/\1\n$text\n\2/
            }
            /\n[^[:space:]]/! {
                s/\(.*\)/\1\n$text/
            }
        }" "$file"
    done
}


restore_file() {
    local file="$1"
    mv "$file.bak" "$file"
}

declare -A source_lines
source_lines["no_sleep"]=""
source_lines["2_ms_sleep"]="usleep(2000);"
source_lines["4_ms_sleep"]="usleep(4000);"
source_lines["8_ms_sleep"]="usleep(8000);"


for pattern in "${!source_lines[@]}"; do

    cp "$file_name" "$file_name.bak"
    append_text "EXPERIMENTAL LINE TO BE CHANGED FROM SCRIPT" "${source_lines[$pattern]}" "$file_name"

    cd "${REDIS_MAIN_SCRIPT_DIR}/experimental_scripts"
    sudo ./experimental_setup.sh

    sudo cp -rf ${EXPERIMENTAL_OUTPUT_DIR}/exp_${EXPERIMENT_NAME} ${experimental_dir}/${pattern}

    restore_file "$file_name"
done
