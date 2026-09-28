#!/usr/bin/env bash
# Rebuild the crash-campaign figures from logs/ into figures/.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd); tools="$here/../tools"
if [ -d "$here/logs/campaign" ]; then
  python3 "$tools/analyze_crash.py" "$here/logs/campaign" "$@"          # metrics.json + <S>/fig.png
  python3 "$tools/build_artifact.py" "$here/logs/campaign" "$here/figures/crash_artifact.html"
  cp "$here/logs/campaign/metrics.json" "$here/figures/" 2>/dev/null || true
  for f in "$here"/logs/campaign/*/fig.png; do
    [ -e "$f" ] && cp "$f" "$here/figures/campaign_$(basename "$(dirname "$f")").png"
  done
fi
for run in "$here"/logs/crash_s* "$here"/logs/campaign_heal; do
  [ -d "$run" ] && "$tools/make_figures.sh" "$run" "$here/figures"
done
