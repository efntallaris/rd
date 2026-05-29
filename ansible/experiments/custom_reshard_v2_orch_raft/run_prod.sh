#!/bin/bash -e
# Production-scale single run (clone of run_smoke.sh) using workloada_prod
# (500K records, maxexecutiontime=300) so the run continues ~200s past the
# migration window and the post-migration steady state ("after effect") is
# clearly visible. Skips setup/teardown; assumes binaries already built +
# deployed. workload.yml's kill_processes + clean_runtime handle cleanup.

set -o pipefail
VARIANT="custom_reshard_v2_orch_raft"
WORKLOAD="workloada_prod"
cd "$(dirname "$0")/../.."

EXTRA_VARS=(
  -e "redis_variant=custom"
  -e "pre_reshard_pause=10"
)

TS="$(date +%Y%m%d_%H%M%S)"
LOG_DIR="/tmp/${VARIANT}_prod_${TS}"
mkdir -p "${LOG_DIR}"

echo ">>> ${VARIANT} PROD starting at ${TS} (workload=${WORKLOAD}, pause=10s, NO build/teardown)"

ansible-playbook -i inventory.ini experiments/${VARIANT}/workload.yml "${EXTRA_VARS[@]}" \
  -e "experiment_name=${VARIANT}_${WORKLOAD}" \
  -e "redis_workload=${WORKLOAD}" \
  2>&1 | tee "${LOG_DIR}/${WORKLOAD}.log"

echo ">>> Prod run complete. Results: /tmp/experiments/${VARIANT}_${WORKLOAD}/"
