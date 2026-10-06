#!/usr/bin/env bash
# Run ON THE CONTROLLER (redis0) as root, from the repo root.
# SKIP_BUILD=1: uses the binaries already deployed; drop it to wipe + rebuild first.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
W=workloada_procs_scaling
SKIP_BUILD=1 "$here/../../ansible/experiments/custom_procs_scaling/run_sweep.sh" "$W"
for n in 3 6; do
  rm -rf "$here/logs/${n}procs"
  cp -r "/tmp/experiments/custom_procs_scaling_${n}procs_${W}" "$here/logs/${n}procs"
done
python3 "$here/../tools/compare_runs.py" "$here/logs/3procs" "$here/logs/6procs"
