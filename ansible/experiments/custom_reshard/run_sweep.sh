#!/bin/bash -e
# custom_reshard: aqueduct fork Redis with concurrent reshard during YCSB.
# This is the primary aqueduct experiment (legacy experiment.yml flow).

set -o pipefail
VARIANT="custom_reshard"
cd "$(dirname "$0")/../.."

WORKLOADS=("$@")
[ ${#WORKLOADS[@]} -eq 0 ] && WORKLOADS=(workloada workloadb workloadc workloadf)

EXTRA_VARS=(
  -e "redis_variant=custom"
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
