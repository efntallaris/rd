#!/usr/bin/env bash
# Rebuild the crash-campaign figures from logs/ into figures/.
#   campaign_<S>.png   per-scenario throughput timeline with kill/recovery marks (analyze_crash.py)
#   metrics.json       per-scenario metrics (rise, recovery time, errors, DBSIZE)
#   <run>_ycsb.png / <run>_gantt.png / <run>_full_run.png / <run>_window.txt  (make_figures.sh)
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd); tools="$here/../tools"
if [ -d "$here/logs/campaign" ]; then
  python3 "$tools/analyze_crash.py" "$here/logs/campaign" "$@"
  python3 "$tools/build_artifact.py" "$here/logs/campaign" "$here/figures/crash_artifact.html"
  cp "$here/logs/campaign/metrics.json" "$here/figures/" 2>/dev/null || true
  for f in "$here"/logs/campaign/*/fig.png; do
    [ -e "$f" ] && cp "$f" "$here/figures/campaign_$(basename "$(dirname "$f")").png"
  done
fi
for run in "$here"/logs/campaign_heal "$here"/logs/crash_s*; do
  [ -d "$run" ] && "$tools/make_figures.sh" "$run" "$here/figures"
done
