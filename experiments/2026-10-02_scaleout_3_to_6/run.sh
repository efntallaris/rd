#!/usr/bin/env bash
# Run ON THE CONTROLLER (redis0), from anywhere.   ./run.sh [scenario ...]   (default: all)
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
export OUT_ROOT="${OUT_ROOT:-/tmp/lincheck/scaleout3to6}"
[ $# -eq 0 ] && set -- HEALTHY S1 S2 S3 S4 S8 S9
"$here/../../ansible/experiments/custom_scaleout_3to6/run_all.sh" "$@"
for s in "$@"; do
  exp=scaleout3to6_crash_${s,,}; [ "$s" = HEALTHY ] && exp=scaleout3to6_lin_healthy
  lin=$(ls -1dt "$OUT_ROOT"/${s}_q*_* 2>/dev/null | head -1)
  sudo rm -rf "$here/logs/$s"; sudo mkdir -p "$here/logs"
  sudo cp -r "/tmp/experiments/$exp" "$here/logs/$s"
  [ -n "$lin" ] && sudo cp -r "$lin" "$here/logs/$s/lincheck"
done
tail -n $# "$OUT_ROOT/summary.txt"
