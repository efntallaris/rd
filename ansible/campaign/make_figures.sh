#!/bin/bash
# make_figures.sh <3to4|3to6> <small|big> <data dir> <lincheck out root> <figure dir>
#   <data dir>          per-scenario YCSB data as saved by run_campaign.sh (lin_healthy/, crash_s1/, ...)
#   <lincheck out root> the suite's OUT_ROOT (HEALTHY_q*/, S1_q*/, ... and summary.txt)
# Writes <setup>_ycsb.png (all scenarios), <setup>_gantt_HEALTHY.png, <setup>_summary.txt.
set -u
setup=$1; prof=$2; data=$3; lin=$4; fig=$5
E=/users/entall/rd/experiments; P=$E/2026-10-02_scaleout_3_to_6; T=$E/tools/scaleout
export MPLCONFIGDIR=${MPLCONFIGDIR:-/tmp/mplconfig_$USER}; mkdir -p "$MPLCONFIGDIR"
tmp=$(mktemp -d); sudo mkdir -p "$fig"
if [ "$setup" = 3to4 ]; then
  x=$([ "$prof" = big ] && echo 40,100 || echo 10,60)
  SETUP=3to4 XLIM=$x python3 $P/plot_scaleout_ycsb.py "$data" "$lin" $tmp/y.png 2>&1 | grep -v findfont
  $T/chunkgantt.sh "$(ls -d $lin/HEALTHY_* | tail -1)" $tmp/g.png
  pre=""
else
  x=$([ "$prof" = big ] && echo 40,110 || echo 10,60)
  XLIM=$x python3 $P/plot_scaleout_ycsb.py "$data" "$lin" $tmp/y.png 2>&1 | grep -v findfont
  python3 $T/gantt_pairs.py 3to6 "$(ls -d $lin/HEALTHY_* | tail -1)" $tmp/g.png "3 -> 6, $prof, fault-free"
  pre=scaleout3to6_
fi
{ echo "# $setup $prof: result, migration window, client throughput (downtime.py)"; sed 's/ q=yes rc=\([0-9]*\) OVERALL: \([A-Z]*\) | \(.*\) | migration/ rc=\1 \2 | \3 | migration/' "$lin/summary.txt" | cut -c1-200
  for d in $(ls -d $lin/*_q*_* | sort -t_ -k3,4); do s=$(basename $d | cut -d_ -f1)
    r=$([ "$s" = HEALTHY ] && echo lin_healthy || echo crash_$(echo "$s" | tr 'A-Z' 'a-z'))
    [ -d "$data/$pre$r" ] || continue
    echo "== $s window $(grep FULL "$data/$pre$r/migration_window.txt" | grep -oE '[0-9.]+s ' | head -1)"
    python3 $T/downtime.py $setup $d "$data/$pre$r" | grep -E '^(kills|elections|ycsb)' | cut -c1-220
  done; } > $tmp/s.txt 2>&1
sudo cp $tmp/y.png "$fig/${setup}_ycsb.png"; sudo cp $tmp/g.png "$fig/${setup}_gantt_HEALTHY.png" 2>/dev/null; sudo cp $tmp/s.txt "$fig/${setup}_summary.txt"
rm -rf $tmp; echo "figures in $fig: ${setup}_ycsb.png ${setup}_gantt_HEALTHY.png ${setup}_summary.txt"
