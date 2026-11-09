#!/usr/bin/env bash
# House-style figures of ONE finished scenario run, without touching any experiment's data/:
#
#   figures_for_run.sh <run dir> [out root]
#     <run dir>  = $OUT_ROOT/<S>_q<yes|no>_<ts>  (what run_lin_scenario.sh writes)
#     out root   = default: the run dir's parent
#
# 1. snapshot: copies the playbook's /tmp/experiments/<exp> of that scenario (server logs, YCSB
#    output) to <out root>/panel_data/<run dir name>/exp, if it belongs to this run (the next run
#    of the same scenario overwrites /tmp/experiments, so the panel calls this right after the
#    scenario's summary line appears);
# 2. the figures of experiments/2026-11-07_crash_suite_commit_point/figures.sh, into
#    <out root>/figures/: tputlat_<S>.png/.pdf (throughput + latency incl. failed reads,
#    plot_scaleout_ycsb.py) and, for a crash scenario, <S>_recovery.png (detailed timeline),
#    <S>_table.txt, paper_<S>_recovery.pdf/.png (3.33 in) and paper_<S>_recovery_wide (7 in);
#    gantt_<S>.png for every scenario (gantt_pairs.py, per-pair phases from the server logs).
# Status in <out root>/figures/.status_<run dir name>, output in <S>_figlog.txt.
set -uo pipefail
run=$(cd "$1" && pwd) || exit 2
out=$(cd "${2:-$(dirname "$run")}" && pwd)
here=$(cd "$(dirname "$0")" && pwd)
E=$(cd "$here/../.." && pwd)
T="$E/tools"
export CMU_DIR="$T/fonts/cmu" GRAY=1
base=$(basename "$run"); s=${base%%_q*}
fig="$out/figures"; mkdir -p "$fig"
st="$fig/.status_$base"; log="$fig/${s}_figlog.txt"
echo running > "$st"
exec > "$log" 2>&1
echo "figures for $run ($(date -u +%FT%TZ))"

# ---- 1. snapshot of /tmp/experiments/<exp>
snap="$out/panel_data/$base/exp"
if [ -n "${FIG_EXP_SRC:-}" ] && [ -d "$FIG_EXP_SRC/ycsb" ] && [ ! -d "$snap/ycsb" ]; then
  snap="$FIG_EXP_SRC"; echo "using the experiment's copy $snap"     # older run: experiments/<e>/data/<S>/exp
fi
if [ ! -d "$snap/ycsb" ]; then
  [ "$s" = HEALTHY ] && name=lin_healthy || name="crash_${s,,}"
  src=""
  # the run's start: the UTC timestamp in its name (the dir's mtime moves to the end of the run)
  tsn=${base##*_q*_}; tsn=${base: -15}
  start=$(date -u -d "${tsn:0:8} ${tsn:9:2}:${tsn:11:2}:${tsn:13:2}" +%s 2>/dev/null || stat -c %Y "$run")
  for c in "/tmp/experiments/scaleout3to6_$name" "/tmp/experiments/$name"; do
    [ -d "$c/ycsb" ] || continue
    # it must have been written by this run (after the run dir was created)
    newest=$(find "$c/ycsb" -type f -printf '%T@\n' 2>/dev/null | sort -n | tail -1 | cut -d. -f1)
    if [ -n "$newest" ] && [ "$newest" -ge "$((start - 60))" ]; then src="$c"; break; fi
    echo "skip $c: written before this run (newest file $(date -u -d @"${newest:-0}" +%T))"
  done
  if [ -z "$src" ]; then
    echo "no /tmp/experiments copy for this run (overwritten by a later run of $s?)"
    echo "no data: /tmp/experiments was overwritten by a later $s" > "$st"; exit 1
  fi
  mkdir -p "$(dirname "$snap")"
  cp -r "$src" "$snap" 2>/dev/null || { sudo cp -r "$src" "$snap" && sudo chown -R "$(id -u):$(id -g)" "$snap"; }
  echo "snapshot $src -> $snap ($(du -sh "$snap" | cut -f1))"
fi

# ---- 2. figures (same scripts and order as the experiment's figures.sh)
[ -w "$snap" ] && { python3 "$T/migration_window.py" "$snap" > "$snap/migration_window.txt" 2>/dev/null || true; }
ok=0
if [ "$s" != HEALTHY ]; then
  if python3 "$T/scaleout/recovery_breakdown.py" "$s" "$run" "$snap" "$fig/${s}_recovery.png" > "$fig/${s}_table.txt" 2>&1; then
    tail -2 "$fig/${s}_table.txt"
    if [ -f "$fig/${s}_recovery.json" ]; then
      python3 "$T/scaleout/paper_recovery.py" "$fig/${s}_recovery.json" "$fig/paper_${s}_recovery.pdf" 3.33 && ok=$((ok + 1))
      python3 "$T/scaleout/paper_recovery.py" "$fig/${s}_recovery.json" "$fig/paper_${s}_recovery_wide.pdf" 7
    fi
  else
    echo "recovery_breakdown failed:"; tail -5 "$fig/${s}_table.txt"
  fi
fi
# per-pair migration Gantt from the server logs in the run dir (campaign figure, gantt_pairs.py)
setup=3to6; [ -n "${src:-}" ] && [[ "$src" != *scaleout3to6_* ]] && setup=3to4
grep -q 'scaleout3to6' "$run/run.log" "$run/playbook.log" 2>/dev/null || setup=3to4
python3 "$T/scaleout/gantt_pairs.py" "$setup" "$run" "$fig/gantt_$s.png" "$s" || echo "gantt failed"
stage=$(mktemp -d); mkdir -p "$stage/ycsb" "$stage/lin"
ln -s "$snap" "$stage/ycsb/$s"; ln -s "$run" "$stage/lin/$base"
python3 "$E/2026-10-02_scaleout_3_to_6/plot_scaleout_ycsb.py" "$stage/ycsb" "$stage/lin" "$fig/tputlat_$s.png" "$s" && ok=$((ok + 1))
rm -rf "$stage"
ls -1 "$fig" | grep -E "(^|_)${s}(_|\.)"
[ -f "$fig/tputlat_$s.png" ] && echo done > "$st" || echo "failed: see ${s}_figlog.txt" > "$st"
