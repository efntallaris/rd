#!/bin/bash
# run_custom.sh — drive the two custom-fork sweeps end-to-end.
#
#   * scenario A: custom_baseline (no reshard)
#   * scenario B: custom_reshard  (concurrent reshard during YCSB)
#
# Both sweeps build /users/entall/rd/redis from the aqueduct working tree
# (Phase 2.5 r_allocator path, cluster-rdma-allocator-shadow=yes) and drive
# YCSB through redis0:8000 from the ycsb0 inventory host.
#
# Usage:
#   bash run_custom.sh                        # default workloads: workloada workloadb
#   bash run_custom.sh workloada              # single workload
#   bash run_custom.sh workloada workloadc    # explicit list
set -euo pipefail

WORKLOADS=("$@")
[ ${#WORKLOADS[@]} -eq 0 ] && WORKLOADS=(workloada workloadb)

REPO_ROOT="/users/entall/rd"
YCSB_WORKLOADS_SRC="${REPO_ROOT}/ycsb_client/workloads"
SHARED_WORKLOADS_DIR="/rd/workloads"
RECORDCOUNT="${RECORDCOUNT:-500000}"
OPERATIONCOUNT="${OPERATIONCOUNT:-1000000}"

# --- one-time prep: scaled workload files at /rd/workloads/ ----------------
# build_ycsb.yml copies /rd/workloads/* to each YCSB node, so the scaled
# files must live there. Idempotent: overwrite each run so user can adjust
# RECORDCOUNT / OPERATIONCOUNT via env without manual cleanup.
echo ">>> custom: staging scaled YCSB workloads (records=${RECORDCOUNT}, ops=${OPERATIONCOUNT})"
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

echo ">>> custom: baseline sweep — workloads: ${WORKLOADS[*]}"
bash experiments/custom_baseline/run_sweep.sh "${WORKLOADS[@]}"

echo ">>> custom: reshard sweep — workloads: ${WORKLOADS[*]}"
bash experiments/custom_reshard/run_sweep.sh "${WORKLOADS[@]}"

echo ">>> custom: done"
echo "    baseline results: /tmp/experiments/custom_baseline_<workload>/"
echo "    reshard  results: /tmp/experiments/custom_reshard_<workload>/"
echo "    driver logs:      /tmp/custom_{baseline,reshard}_sweep_<ts>/"
