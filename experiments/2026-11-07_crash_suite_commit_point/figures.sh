#!/usr/bin/env bash
# Figures + suite table from data/ (no cluster needed).   ./figures.sh [scenario ...]
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
T="$here/../tools"
# House style for every figure (2026-11-07): Computer Modern, grayscale with red for faults.
export CMU_DIR="$T/fonts/cmu" GRAY=1
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
# Throughput + latency per scenario and as a grid (plot_scaleout_ycsb.py). Mean read latency
# includes failed reads and p99 every attempt, so an outage shows in the latency panels. The
# plot finds each run by name: stage data/<S>/exp and the lincheck dir under its original name
# (the last field of the scenario's row in data/summary.txt).
ST="$here/figures/.tputlat_stage"; rm -rf "$ST"; mkdir -p "$ST/ycsb" "$ST/lin"
for s in $(ls "$here/data" 2>/dev/null); do
  [ -d "$here/data/$s/exp" ] && [ -d "$here/data/$s/lincheck" ] || continue
  orig=$(grep -E " $s q=" "$here/data/summary.txt" 2>/dev/null | tail -1 | grep -oE '/tmp/lincheck/[^ ]+' | tail -1)
  [ -n "$orig" ] || continue
  ln -sfn "$here/data/$s/exp" "$ST/ycsb/$s"
  ln -sfn "$here/data/$s/lincheck" "$ST/lin/$(basename "$orig")"
done
PLOT="$here/../2026-10-02_scaleout_3_to_6/plot_scaleout_ycsb.py"
for s in "$@"; do
  [ -e "$ST/ycsb/$s" ] && python3 "$PLOT" "$ST/ycsb" "$ST/lin" "$here/figures/tputlat_$s.png" "$s" >/dev/null 2>&1 || true
done
python3 "$PLOT" "$ST/ycsb" "$ST/lin" "$here/figures/tputlat_all.png" >/dev/null 2>&1 || true
python3 "$T/scaleout/suite_table.py" "$here" || true
