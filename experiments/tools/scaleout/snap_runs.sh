#!/bin/bash
# snap_runs.sh <prefix> <out_dir>: copy the YCSB output and migration window of every
# /tmp/experiments/<prefix>* run into <out_dir> (those directories are overwritten by the next run
# of the same scenario). Prefixes: "scaleout3to6_" (3 -> 6), "lin_healthy" / "crash_s" (3 -> 4).
P=$1; O=$2; mkdir -p "$O"
for d in /tmp/experiments/${P}*; do
  [ -d "$d/ycsb" ] || continue
  b=$(basename "$d"); mkdir -p "$O/$b"
  cp -r "$d/ycsb" "$d/migration_window.txt" "$O/$b/" 2>/dev/null
  echo "$b $(grep FULL "$d/migration_window.txt" 2>/dev/null | grep -oE '[0-9.]+s ')"
done
