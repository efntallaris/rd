#!/bin/bash -e
# vanilla_raft_baseline: upstream Redis with RedisRaft module loaded.
# 3 ShardGroups (each a 3-node Raft cluster) co-located on redis0/1/2
# as 3 processes per host. Replication-only — no resharding.
#
# Usage:
#   sudo ./run_sweep.sh                          # workloada b c f
#   sudo ./run_sweep.sh workloada workloadc      # subset
#   sudo VANILLA_REDIS_TAG=8.6.1 ./run_sweep.sh  # override upstream version

set -o pipefail
VARIANT="vanilla_raft_baseline"
cd "$(dirname "$0")/../.."   # cd into ansible/

WORKLOADS=("$@")
[ ${#WORKLOADS[@]} -eq 0 ] && WORKLOADS=(workloada workloadb workloadc workloadf)

VANILLA_REDIS_REPO="${VANILLA_REDIS_REPO:-https://github.com/redis/redis.git}"
VANILLA_REDIS_TAG="${VANILLA_REDIS_TAG:-8.6.2}"
VANILLA_REDIS_DIR="${VANILLA_REDIS_DIR:-/users/entall/rd/redis-vanilla}"
VANILLA_BIN_DIR="${VANILLA_BIN_DIR:-/users/entall/rd/redis_bin_vanilla}"

EXTRA_VARS=(
  -e "redis_variant=vanilla_raft"
  -e "redis_dir=${VANILLA_REDIS_DIR}"
  -e "local_setup_dir=${VANILLA_BIN_DIR}"
  -e "vanilla_redis_repo=${VANILLA_REDIS_REPO}"
  -e "vanilla_redis_tag=${VANILLA_REDIS_TAG}"
)

TS="$(date +%Y%m%d_%H%M%S)"
LOG_DIR="/tmp/${VARIANT}_sweep_${TS}"
mkdir -p "${LOG_DIR}"

echo ">>> ${VARIANT} sweep starting at ${TS}"
echo ">>> Upstream Redis: ${VANILLA_REDIS_REPO} @ ${VANILLA_REDIS_TAG}"
echo ">>> Workloads:      ${WORKLOADS[*]}"
echo ">>> Driver logs:    ${LOG_DIR}/"

ansible-playbook -i inventory.ini experiments/${VARIANT}/setup.yml "${EXTRA_VARS[@]}" \
  2>&1 | tee "${LOG_DIR}/setup.log"

for w in "${WORKLOADS[@]}"; do
  echo ">>> [run] ${w}"
  ansible-playbook -i inventory.ini experiments/${VARIANT}/workload.yml "${EXTRA_VARS[@]}" \
    -e "experiment_name=${VARIANT}_${w}" \
    -e "redis_workload=${w}" \
    2>&1 | tee "${LOG_DIR}/${w}.log"
done

ansible-playbook -i inventory.ini experiments/${VARIANT}/teardown.yml "${EXTRA_VARS[@]}" \
  2>&1 | tee "${LOG_DIR}/teardown.log"

echo ">>> Sweep complete. Results: /tmp/experiments/${VARIANT}_<workload>/"
