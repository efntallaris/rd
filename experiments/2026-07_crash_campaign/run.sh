#!/usr/bin/env bash
# Run the full crash campaign (HEALTHY + S1..S5). Run ON THE CONTROLLER (redis0) as root,
# from the repo root. ~35 min per 30M scenario. Output: /tmp/crash_inject/campaign and
# /tmp/experiments/crash_s<N>; fetch them here with experiments/tools/fetch_logs.sh.
#   ORDER="S1 S2" WORKLOAD=workloada_debug_500k experiments/2026-07_crash_campaign/run.sh
set -euo pipefail
cd "$(dirname "$0")/../../ansible/crash"
exec ./run_all_scenarios.sh "$@"
