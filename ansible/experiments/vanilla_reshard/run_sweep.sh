#!/bin/bash -e
# vanilla_reshard: upstream Redis + concurrent reshard during YCSB.
# Same paths/tag as vanilla_baseline; differs in the workload.yml flow.

set -o pipefail
VARIANT="vanilla_reshard"
cd "$(dirname "$0")/../.."

WORKLOADS=("$@")
[ ${#WORKLOADS[@]} -eq 0 ] && WORKLOADS=(workloada workloadb workloadc workloadf)

VANILLA_REDIS_REPO="${VANILLA_REDIS_REPO:-https://github.com/redis/redis.git}"
VANILLA_REDIS_TAG="${VANILLA_REDIS_TAG:-8.6.2}"
VANILLA_REDIS_DIR="${VANILLA_REDIS_DIR:-/users/entall/rd/redis-vanilla}"
VANILLA_BIN_DIR="${VANILLA_BIN_DIR:-/users/entall/rd/redis_bin_vanilla}"

EXTRA_VARS=(
  -e "redis_variant=vanilla"
  -e "redis_dir=${VANILLA_REDIS_DIR}"
  -e "local_setup_dir=${VANILLA_BIN_DIR}"
  -e "vanilla_redis_repo=${VANILLA_REDIS_REPO}"
  -e "vanilla_redis_tag=${VANILLA_REDIS_TAG}"
)

TS="$(date +%Y%m%d_%H%M%S)"
LOG_DIR="/tmp/${VARIANT}_sweep_${TS}"
mkdir -p "${LOG_DIR}"

echo ">>> ${VARIANT} sweep starting at ${TS}"
echo ">>> Workloads:   ${WORKLOADS[*]}"
echo ">>> Driver logs: ${LOG_DIR}/"

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
