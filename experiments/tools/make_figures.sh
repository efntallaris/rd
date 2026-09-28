#!/usr/bin/env bash
# make_figures.sh — the standard figure set for one collected run.
#
# Usage (from anywhere):
#   experiments/tools/make_figures.sh <run-dir> <out-dir> [extra plot_ycsb_timeseries args]
#
#   <run-dir>  a collected run: experiments/<id>/logs/<run>/ (has ycsb/ and logs/)
#   <out-dir>  where the figures go, usually experiments/<id>/figures/
#
# Produces, when the run has the data for it:
#   <run>_window.txt         cold-excluded migration window (migration_window.py)
#   <run>_ycsb.png           throughput + latency over time, migration band
#   <run>_gantt.png          per-phase migration Gantt from donor + recipient logs
#   <run>_full_run.png       throughput, latency, CPU and cluster NIC traffic
# A figure whose inputs are missing is skipped with a note; the others still run.
set -uo pipefail

if [ $# -lt 2 ]; then sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; exit 1; fi

tools=$(cd "$(dirname "$0")" && pwd)
run=$(cd "$1" && pwd) || exit 1
out="$2"; shift 2
mkdir -p "$out"; out=$(cd "$out" && pwd)
name=$(basename "$run")

step() {  # step <label> <command...>
  local label=$1; shift
  if "$@"; then echo "  ok    $label"; else echo "  skip  $label (see messages above)"; fi
}

echo "==> $name -> $out"
step window   bash -c "python3 '$tools/migration_window.py' '$run' > '$out/${name}_window.txt'"
step ycsb     python3 "$tools/plot_ycsb_timeseries.py" "$run" --output "$out/${name}_ycsb.png" "$@"
step gantt    python3 "$tools/plot_phase_gantt.py"     "$run" "$out/${name}_gantt.png"
step full_run python3 "$tools/plot_full_run.py"        "$run" "$out/${name}_full_run.png"
