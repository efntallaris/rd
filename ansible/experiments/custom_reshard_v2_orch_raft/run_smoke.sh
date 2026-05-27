#!/bin/bash -e
# Fast iteration variant of run_sweep.sh.
#
# Skips setup.yml entirely (which does the full remote rebuild — slow).
# Assumes the binary is already built + deployed on remotes. Run:
#   sudo ansible-playbook -i inventory.ini tasks/build/build_redis_custom.yml
# once after you sync source changes, then this script for each smoke.
#
# Also uses workloada_smoke (100K records, 3M ops cap, maxexecutiontime=45)
# and pre_reshard_pause=8 so total wall-clock is ~75 s instead of ~5 min.
# Skips teardown.yml — leave the cluster running; workload.yml's
# kill_processes + clean_runtime at the start of the next run handles cleanup.

set -o pipefail
VARIANT="custom_reshard_v2_orch_raft"
WORKLOAD="workloada_smoke"
cd "$(dirname "$0")/../.."

EXTRA_VARS=(
  -e "redis_variant=custom"
  -e "pre_reshard_pause=8"
)

TS="$(date +%Y%m%d_%H%M%S)"
LOG_DIR="/tmp/${VARIANT}_smoke_${TS}"
mkdir -p "${LOG_DIR}"

echo ">>> ${VARIANT} SMOKE starting at ${TS} (workload=${WORKLOAD}, pause=8s, NO build/teardown)"

ansible-playbook -i inventory.ini experiments/${VARIANT}/workload.yml "${EXTRA_VARS[@]}" \
  -e "experiment_name=${VARIANT}_${WORKLOAD}" \
  -e "redis_workload=${WORKLOAD}" \
  2>&1 | tee "${LOG_DIR}/${WORKLOAD}.log"

echo ">>> Smoke complete. Results: /tmp/experiments/${VARIANT}_${WORKLOAD}/"
echo ">>> To rebuild after src changes:"
echo "    sudo ansible-playbook -i inventory.ini tasks/build/build_redis_custom.yml"
