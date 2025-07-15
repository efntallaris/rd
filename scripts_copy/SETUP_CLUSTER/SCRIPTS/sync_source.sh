#!/bin/bash -e

# Default branch if no argument is passed
DEFAULT_BRANCH="redis_fulva_protocol"
BRANCH_NAME=${1:-$DEFAULT_BRANCH}

source ./config.sh

rm -rf "${YCSB_LOG_FILENAME}"
rm -rf "${LOCAL_LOG_DIR}"

for redis_instance in "${!instances[@]}"; do
  echo "$redis_instance - ${instances[$redis_instance]}"
  IFS=',' read -r -a nodeInstance <<<"${instances[$redis_instance]}"

  for i in "${!nodeInstance[@]}"; do
    IFS="|" read -r -a info <<<"${nodeInstance[i]}"

    sudo ssh -o StrictHostKeyChecking=no "${info[1]}" bash <<EOF
set -e

BRANCH="${BRANCH_NAME}"

# Clone repo if missing
if [ ! -d /root/rd/.git ]; then
    rm -rf /root/rd
    git clone https://github.com/efntallaris/rd /root/rd
fi

cd /root/rd

# Sync to specified branch
git fetch origin
git reset --hard
git checkout -B "\$BRANCH" "origin/\$BRANCH"
git pull origin "\$BRANCH"
EOF

  done
done
