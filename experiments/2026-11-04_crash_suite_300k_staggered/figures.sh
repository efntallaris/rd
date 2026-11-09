#!/usr/bin/env bash
# Figures + suite table from data/ (no cluster needed).   ./figures.sh [scenario ...]
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
T="$here/../tools"
mkdir -p "$here/figures" 2>/dev/null || sudo mkdir -p "$here/figures"
sudo chmod 777 "$here/figures" 2>/dev/null
[ $# -eq 0 ] && set -- $(ls "$here/data" 2>/dev/null)

for s in "$@"; do
  d="$here/data/$s"; [ -d "$d" ] || { echo "skip $s (no data)"; continue; }
  if [ -d "$d/exp" ]; then
    python3 "$T/migration_window.py" "$d/exp" > /tmp/mw_$s.txt 2>/dev/null \
      && sudo cp /tmp/mw_$s.txt "$d/exp/migration_window.txt"
    # throughput + latency over time, summed over both YCSB clients
    python3 "$here/../2026-10-01_protocol_fixes_quick/tools/plot_total_ycsb.py" \
      "$d/exp" "$here/figures/${s}_tput_lat.png" "3->6 scale-out, 300k: $s" || true
  fi
  if [ -d "$d/lincheck" ] && [ -d "$d/exp" ]; then
    # recovery breakdown: detailed timeline + phase table + JSON for the paper figure
    python3 "$T/scaleout/recovery_breakdown.py" "$s" "$d/lincheck" "$d/exp" \
      "$here/figures/${s}_recovery.png" > "$here/figures/${s}_table.txt" 2>&1 \
      && tail -2 "$here/figures/${s}_table.txt"
    if [ -f "$here/figures/${s}_recovery.json" ]; then
      python3 "$T/scaleout/paper_recovery.py" "$here/figures/${s}_recovery.json" \
        "$here/figures/paper_${s}_recovery.pdf" 3.33 || true
      python3 "$T/scaleout/paper_recovery.py" "$here/figures/${s}_recovery.json" \
        "$here/figures/paper_${s}_recovery_wide.pdf" 7 || true
    fi
  fi
done
python3 "$T/scaleout/suite_table.py" "$here" || true
