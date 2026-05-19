#!/bin/bash -e
# custom_reshard_v2: aqueduct fork Redis with concurrent reshard during YCSB,
# using the early-ownership-flip protocol (RDMA RESHARD-FLIP between RESHARD
# and RESHARD-EXEC).

set -o pipefail
VARIANT="custom_reshard_v2"
cd "$(dirname "$0")/../.."

WORKLOADS=("$@")
[ ${#WORKLOADS[@]} -eq 0 ] && WORKLOADS=(workloada workloadb workloadc workloadf)

REPO_ROOT="/users/entall/rd"
YCSB_WORKLOADS_SRC="${REPO_ROOT}/ycsb_client/workloads"
SHARED_WORKLOADS_DIR="/rd/workloads"
RECORDCOUNT="${RECORDCOUNT:-10000000}"
# 18M ops: at the ~300K-400K ops/s sustained throughput, this gives a
# ~50s YCSB run — enough for the 18s pre-migration baseline (so the first
# migration starts at YCSB second ~20) + ~20s migration band (three
# sequential sources at ~7s spacing each via serial:1) + ~10s post-
# migration recovery in the timeseries plots. Bumped from 10M when
# pre_reshard_pause shifted from 10 to 18.
OPERATIONCOUNT="${OPERATIONCOUNT:-18000000}"

echo ">>> ${VARIANT}: staging scaled YCSB workloads (records=${RECORDCOUNT}, ops=${OPERATIONCOUNT})"
for w in "${WORKLOADS[@]}"; do
    src="${YCSB_WORKLOADS_SRC}/${w}"
    if [ ! -f "${src}" ]; then
        echo "!!! workload source not found: ${src}" >&2
        exit 1
    fi
    sudo cp "${src}" "${SHARED_WORKLOADS_DIR}/${w}"
    sudo sed -i \
        -e "s/^recordcount=.*/recordcount=${RECORDCOUNT}/" \
        -e "s/^operationcount=.*/operationcount=${OPERATIONCOUNT}/" \
        "${SHARED_WORKLOADS_DIR}/${w}"
done

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
