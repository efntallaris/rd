#!/bin/bash -e
# custom_procs_scaling: aqueduct fork Redis as a plain cluster (no Raft, no
# migrate, no reshard) on redis0/1/2, once with 1 redis-server per host
# (3 masters) and once with 2 per host (6 masters). Measures what the extra
# processes buy on fixed hardware.
#
#   sudo ./experiments/custom_procs_scaling/run_sweep.sh                # default workload
#   sudo SKIP_BUILD=1 THREADS=400 ./experiments/custom_procs_scaling/run_sweep.sh
#
# SKIP_BUILD=1 skips setup.yml/teardown.yml (which wipe and rebuild the
# binaries) and runs against whatever is already deployed.

set -o pipefail
VARIANT="custom_procs_scaling"
cd "$(dirname "$0")/../.."

WORKLOADS=("$@")
[ ${#WORKLOADS[@]} -eq 0 ] && WORKLOADS=(workloada_procs_scaling)
PROCS_PER_HOST=(1 2)
THREADS="${THREADS:-200}"   # per YCSB node

# Every playbook here starts by killing all redis-servers on all hosts.
if pgrep -f 'ansible-playbook' >/dev/null; then
  echo "!!! another ansible-playbook is running on this controller; not starting" >&2
  exit 1
fi

# Uses default paths from group_vars/all.yml — redis_dir=/users/entall/rd/redis
EXTRA_VARS=(
  -e "redis_variant=custom"
  -e "ycsb_threads_run=${THREADS}"
)

TS="$(date +%Y%m%d_%H%M%S)"
LOG_DIR="/tmp/${VARIANT}_sweep_${TS}"
mkdir -p "${LOG_DIR}"

echo ">>> ${VARIANT} sweep starting at ${TS}"
echo ">>> Workloads:   ${WORKLOADS[*]}"
echo ">>> Driver logs: ${LOG_DIR}/"

if [ -z "${SKIP_BUILD:-}" ]; then
  ansible-playbook -i inventory.ini experiments/${VARIANT}/setup.yml "${EXTRA_VARS[@]}" \
    2>&1 | tee "${LOG_DIR}/setup.log"
fi

for w in "${WORKLOADS[@]}"; do
  for n in "${PROCS_PER_HOST[@]}"; do
    run="${VARIANT}_$((3 * n))procs_${w}"
    echo ">>> [run] ${run}"
    rm -rf "/tmp/experiments/${run}"
    ansible-playbook -i inventory.ini experiments/${VARIANT}/workload.yml "${EXTRA_VARS[@]}" \
      -e "procs_per_host=${n}" \
      -e "experiment_name=${run}" \
      -e "redis_workload=${w}" \
      2>&1 | tee "${LOG_DIR}/$((3 * n))procs_${w}.log"
  done
done

if [ -z "${SKIP_BUILD:-}" ]; then
  ansible-playbook -i inventory.ini experiments/${VARIANT}/teardown.yml "${EXTRA_VARS[@]}" \
    2>&1 | tee "${LOG_DIR}/teardown.log"
else
  ansible-playbook -i inventory.ini tasks/teardown/kill_processes.yml \
    2>&1 | tee "${LOG_DIR}/teardown.log"
fi

echo ">>> Sweep complete. Results: /tmp/experiments/${VARIANT}_{3,6}procs_<workload>/"
