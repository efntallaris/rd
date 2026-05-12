#!/bin/bash
# run_all_experiments.sh — drive the full 2x2 matrix:
#
#       |              | baseline (no reshard) | reshard |
#       |--------------|-----------------------|---------|
#       | custom       |   custom_baseline     | custom_reshard  |
#       | vanilla      |  vanilla_baseline     | vanilla_reshard |
#
# Custom first (aqueduct binary + Phase 2.5), vanilla second (local
# `vanilla_redis` branch via worktree). Per-sweep wall-clock at the
# default 100K records / 1M ops with two workloads is ~10-20 min; total
# expected ~40-80 min for the full four-sweep matrix.
#
# Usage:
#   bash run_all_experiments.sh                 # default workloads: workloada workloadb
#   bash run_all_experiments.sh workloada       # single workload
set -euo pipefail

WORKLOADS=("$@")
[ ${#WORKLOADS[@]} -eq 0 ] && WORKLOADS=(workloada workloadb)

REPO_ROOT="/users/entall/rd"

echo "================================================================="
echo ">>> run_all_experiments: starting at $(date '+%Y-%m-%d %H:%M:%S')"
echo ">>> workloads: ${WORKLOADS[*]}"
echo "================================================================="

bash "${REPO_ROOT}/run_custom.sh"  "${WORKLOADS[@]}"
bash "${REPO_ROOT}/run_vanilla.sh" "${WORKLOADS[@]}"

echo "================================================================="
echo ">>> all four sweeps complete at $(date '+%Y-%m-%d %H:%M:%S')"
echo ">>> results: /tmp/experiments/{custom,vanilla}_{baseline,reshard}_<workload>/"
echo "================================================================="
