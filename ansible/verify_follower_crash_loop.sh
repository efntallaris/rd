#!/bin/bash
# Run the chunked 2-round migration experiment N times, and AFTER each run
# (before the next run's teardown wipes the on-host logs) capture crash
# signatures + landing-flag-guard hits from the two recipient followers
# (redis4, ycsb1) and the YCSB result summary. Confidence check for the
# intermittent r_allocator follower crash fix.
set -uo pipefail
cd "$(dirname "$0")"
N="${1:-2}"
OUT=/tmp/follower_crash_check
mkdir -p "$OUT"

for i in $(seq 1 "$N"); do
  echo "############ ITERATION $i / $N ############"
  bash experiments/custom_reshard_v2_orch_raft_chunked/run_prod.sh \
      > "$OUT/run_$i.log" 2>&1
  rc=$?
  echo "iteration $i: run_prod.sh exit=$rc"

  # YCSB summary from the just-finished run
  RES=/tmp/experiments/custom_reshard_v2_orch_raft_chunked_workloada_prod/ycsb/ycsb0/tmp/ycsb_output_ycsb0
  echo "--- YCSB summary (iter $i) ---"            >  "$OUT/summary_$i.txt"
  grep -E "^\[OVERALL\]|Return=ERROR|^\[READ-FAILED\], Operations|^\[UPDATE-FAILED\], Operations" \
      "$RES" 2>/dev/null                            >> "$OUT/summary_$i.txt"

  # Recipient-follower crash + guard scan (on-host, before next teardown)
  ansible 'redis4:ycsb1' -i inventory.ini -m shell -a '
    for L in /tmp/redis_logs/*sg4*.log; do
      [ -f "$L" ] || continue
      echo "== $(hostname):$L =="
      echo -n "crash_sigs="; grep -cE "REDIS BUG REPORT|Crashed by signal|SIGSEGV|heap corruption|signal handler" "$L"
      echo -n "guard_hits="; grep -cE "landing block already registered" "$L"
      echo -n "chain_apply_summaries="; grep -cE "CHAIN apply: sess=" "$L"
      echo -n "sessions_forwarded="; grep -hoE "CHAIN-FORWARDED: sess=[0-9]+" "$L" | sort -u | tr "\n" " "; echo
      echo -n "reg_failed="; grep -cE "register_existing_block failed" "$L"
    done
  ' >> "$OUT/summary_$i.txt" 2>&1

  echo "===== summary iter $i ====="
  cat "$OUT/summary_$i.txt"
done
echo "ALL DONE. Per-iteration summaries in $OUT/summary_*.txt"
