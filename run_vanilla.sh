#!/bin/bash
# run_vanilla.sh — drive the two vanilla-redis sweeps end-to-end.
#
#   * scenario A: vanilla_baseline (no reshard)
#   * scenario B: vanilla_reshard  (concurrent reshard during YCSB)
#
# Uses the LOCAL `vanilla_redis` branch (checked out into
# /users/entall/rd/redis-vanilla via `git worktree`) — explicitly NOT
# re-cloning from upstream. The clone import is already commented out in
# vanilla_{baseline,reshard}/setup.yml.
#
# Usage:
#   bash run_vanilla.sh                        # default workloads: workloada workloadb
#   bash run_vanilla.sh workloada              # single workload
#   bash run_vanilla.sh workloada workloadc    # explicit list
set -euo pipefail

WORKLOADS=("$@")
[ ${#WORKLOADS[@]} -eq 0 ] && WORKLOADS=(workloada workloadb)

REPO_ROOT="/users/entall/rd"
REDIS_DIR="${REPO_ROOT}/redis"
VANILLA_DIR="${REPO_ROOT}/redis-vanilla"
YCSB_WORKLOADS_SRC="${REPO_ROOT}/ycsb_client/workloads"
SHARED_WORKLOADS_DIR="/rd/workloads"
RECORDCOUNT="${RECORDCOUNT:-100000}"
OPERATIONCOUNT="${OPERATIONCOUNT:-1000000}"

# --- prep step 1: vanilla worktree (idempotent) ---------------------------
# Use `git worktree` so the aqueduct fork's `vanilla_redis` branch is checked
# out at /users/entall/rd/redis-vanilla. build_redis_vanilla.yml builds
# whatever sits there. Skip if the worktree already exists.
if [ ! -e "${VANILLA_DIR}/.git" ]; then
    echo ">>> vanilla: creating worktree at ${VANILLA_DIR} from branch vanilla_redis"
    sudo git -C "${REDIS_DIR}" worktree add "${VANILLA_DIR}" vanilla_redis
else
    echo ">>> vanilla: worktree already present at ${VANILLA_DIR}"
fi

# --- prep step 2: scaled workload files at /rd/workloads/ -----------------
echo ">>> vanilla: staging scaled YCSB workloads (records=${RECORDCOUNT}, ops=${OPERATIONCOUNT})"
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

cd "${REPO_ROOT}/ansible"

echo ">>> vanilla: baseline sweep — workloads: ${WORKLOADS[*]}"
bash experiments/vanilla_baseline/run_sweep.sh "${WORKLOADS[@]}"

echo ">>> vanilla: reshard sweep — workloads: ${WORKLOADS[*]}"
bash experiments/vanilla_reshard/run_sweep.sh "${WORKLOADS[@]}"

echo ">>> vanilla: done"
echo "    baseline results: /tmp/experiments/vanilla_baseline_<workload>/"
echo "    reshard  results: /tmp/experiments/vanilla_reshard_<workload>/"
echo "    driver logs:      /tmp/vanilla_{baseline,reshard}_sweep_<ts>/"
