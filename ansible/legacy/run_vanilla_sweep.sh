#!/bin/bash -e
# ── Vanilla-Redis baseline sweep ──────────────────────────────────────────
# Clones UPSTREAM Redis (https://github.com/redis/redis) at the tag matching
# the local aqueduct fork's REDIS_VERSION, builds it, then runs YCSB
# workloads A, B, C, F against a 3-master vanilla cluster — no migrate/
# reshard. Use to baseline aqueduct against unmodified upstream Redis.
#
# Paths used (kept separate from the fork's so both can coexist):
#   /users/entall/rd/redis-vanilla        — vanilla source checkout
#   /users/entall/rd/redis_bin_vanilla    — vanilla install prefix
#   /tmp/experiments/vanilla_<workload>/  — per-workload results on this host
#
# Usage:
#   sudo ./run_vanilla_sweep.sh                       # all four workloads
#   sudo ./run_vanilla_sweep.sh workloada workloadc   # subset
#   sudo VANILLA_REDIS_TAG=8.6.1 ./run_vanilla_sweep.sh   # override version
# ──────────────────────────────────────────────────────────────────────────

set -o pipefail
cd "$(dirname "$0")"

WORKLOADS=("$@")
if [ ${#WORKLOADS[@]} -eq 0 ]; then
  WORKLOADS=(workloada workloadb workloadc workloadf)
fi

VANILLA_REDIS_REPO="${VANILLA_REDIS_REPO:-https://github.com/redis/redis.git}"
VANILLA_REDIS_TAG="${VANILLA_REDIS_TAG:-8.6.2}"
VANILLA_REDIS_DIR="${VANILLA_REDIS_DIR:-/users/entall/rd/redis-vanilla}"
VANILLA_BIN_DIR="${VANILLA_BIN_DIR:-/users/entall/rd/redis_bin_vanilla}"

EXTRA_VARS=(
  -e "redis_dir=${VANILLA_REDIS_DIR}"
  -e "local_setup_dir=${VANILLA_BIN_DIR}"
  -e "vanilla_redis_repo=${VANILLA_REDIS_REPO}"
  -e "vanilla_redis_tag=${VANILLA_REDIS_TAG}"
)

TS="$(date +%Y%m%d_%H%M%S)"
LOG_DIR="/tmp/vanilla_sweep_${TS}"
mkdir -p "${LOG_DIR}"

echo ">>> Sweep starting at ${TS}"
echo ">>> Upstream Redis: ${VANILLA_REDIS_REPO} @ ${VANILLA_REDIS_TAG}"
echo ">>> Source dir:     ${VANILLA_REDIS_DIR}"
echo ">>> Install prefix: ${VANILLA_BIN_DIR}"
echo ">>> Workloads:      ${WORKLOADS[*]}"
echo ">>> Driver logs:    ${LOG_DIR}/"

echo ">>> [setup] clone vanilla redis + build redis + build ycsb"
ansible-playbook -i inventory.ini vanilla_setup.yml "${EXTRA_VARS[@]}" \
  2>&1 | tee "${LOG_DIR}/setup.log"

for w in "${WORKLOADS[@]}"; do
  echo ">>> [run] ${w}"
  ansible-playbook -i inventory.ini vanilla_workload.yml "${EXTRA_VARS[@]}" \
    -e "experiment_name=vanilla_${w}" \
    -e "redis_workload=${w}" \
    2>&1 | tee "${LOG_DIR}/${w}.log"
done

echo ">>> [teardown]"
ansible-playbook -i inventory.ini vanilla_teardown.yml "${EXTRA_VARS[@]}" \
  2>&1 | tee "${LOG_DIR}/teardown.log"

echo ">>> Sweep complete."
echo ">>> Per-workload results in /tmp/experiments/vanilla_<workload>/"
echo ">>> Per-stage logs in ${LOG_DIR}/"
