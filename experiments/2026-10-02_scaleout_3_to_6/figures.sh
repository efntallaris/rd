#!/usr/bin/env bash
# Rebuild figures/ from every run under logs/.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$here/figures"
for run in "$here"/logs/*/; do
  [ -d "$run" ] || continue
  name=$(basename "$run")
  python3 "$here/../tools/migration_window.py" "$run" > "$here/figures/${name}_window.txt"
  python3 "$here/../2026-10-01_protocol_fixes_quick/tools/plot_total_ycsb.py" "$run" \
    "$here/figures/${name}_total_ycsb.png" "3 -> 6 scale-out: $name"
done
