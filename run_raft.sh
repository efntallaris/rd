#!/bin/bash
# run_raft.sh — drive the two RedisRaft sweeps end-to-end.
#
#   * vanilla_raft_baseline: upstream Redis + redisraft.so module
#   * custom_raft_baseline:  aqueduct fork  + redisraft.so module
#
# Both build redisraft.so from /users/entall/rd/redisraft (once), then run
# YCSB against a 3-ShardGroup x 3-Raft-node topology co-located on
# redis0/redis1/redis2 (3 redis-server procs per host).
#
# Usage:
#   bash run_raft.sh                        # default workloads: workloada workloadb
#   bash run_raft.sh workloada              # single workload
#   OPERATIONCOUNT=100000 RECORDCOUNT=10000 bash run_raft.sh workloada   # smoke
set -euo pipefail

WORKLOADS=("$@")
[ ${#WORKLOADS[@]} -eq 0 ] && WORKLOADS=(workloada workloadb)

REPO_ROOT="/users/entall/rd"
YCSB_WORKLOADS_SRC="${REPO_ROOT}/ycsb_client/workloads"
SHARED_WORKLOADS_DIR="/rd/workloads"
RECORDCOUNT="${RECORDCOUNT:-500000}"
OPERATIONCOUNT="${OPERATIONCOUNT:-1000000}"

# --- prep step 1: scaled workload files at /rd/workloads/ -----------------
# Note: vanilla_redis worktree creation is handled by the ansible task
# create_vanilla_worktree.yml — it must run on each redis host, since
# /users/entall is per-host (no shared FS).
echo ">>> raft: staging scaled YCSB workloads (records=${RECORDCOUNT}, ops=${OPERATIONCOUNT})"
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

echo ">>> raft: vanilla sweep — workloads: ${WORKLOADS[*]}"
bash experiments/vanilla_raft_baseline/run_sweep.sh "${WORKLOADS[@]}"

echo ">>> raft: custom sweep — workloads: ${WORKLOADS[*]}"
bash experiments/custom_raft_baseline/run_sweep.sh "${WORKLOADS[@]}"

echo ">>> raft: done"
echo "    vanilla results: /tmp/experiments/vanilla_raft_baseline_<workload>/"
echo "    custom  results: /tmp/experiments/custom_raft_baseline_<workload>/"
echo "    driver logs:     /tmp/{vanilla,custom}_raft_baseline_sweep_<ts>/"
