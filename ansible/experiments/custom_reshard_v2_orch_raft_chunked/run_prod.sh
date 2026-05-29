#!/bin/bash -e
# 2-round (chunked) production run of the orchestrated raft reshard. Same
# workloada_prod (500K records, maxexecutiontime=300) as
# custom_reshard_v2_orch_raft/run_prod.sh, but the migration happens in two
# rounds so the post-migration steady state shows two distinct events.
# Skips setup/teardown; assumes binaries already built + deployed.

set -o pipefail
VARIANT="custom_reshard_v2_orch_raft_chunked"
WORKLOAD="workloada_prod"
cd "$(dirname "$0")/../.."

EXTRA_VARS=(
  -e "redis_variant=custom"
  -e "pre_reshard_pause=10"
)

TS="$(date +%Y%m%d_%H%M%S)"
LOG_DIR="/tmp/${VARIANT}_prod_${TS}"
mkdir -p "${LOG_DIR}"

echo ">>> ${VARIANT} PROD starting at ${TS} (workload=${WORKLOAD}, 2 rounds, NO build/teardown)"

ansible-playbook -i inventory.ini experiments/${VARIANT}/workload.yml "${EXTRA_VARS[@]}" \
  -e "experiment_name=${VARIANT}_${WORKLOAD}" \
  -e "redis_workload=${WORKLOAD}" \
  2>&1 | tee "${LOG_DIR}/${WORKLOAD}.log"

echo ">>> Prod run complete. Results: /tmp/experiments/${VARIANT}_${WORKLOAD}/"
