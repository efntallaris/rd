#!/usr/bin/env bash
# Rebuild figures/ from every run under logs/, then print the 3-vs-6 comparison.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
for run in "$here"/logs/*/; do
  [ -d "$run" ] && "$here/../tools/make_figures.sh" "$run" "$here/figures"
done
python3 "$here/../tools/compare_runs.py" "$here/logs/3procs" "$here/logs/6procs"
