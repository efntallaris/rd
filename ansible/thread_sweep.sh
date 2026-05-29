#!/bin/bash
# Thread sweep on the chunked 2-round migration experiment. For each thread
# count: run the full workload (incl. migration), copy the results aside (the
# shared results dir is overwritten each run), and capture the YCSB summary +
# recipient-follower crash check. Tests whether the 4-shardgroup cluster scales
# past the ~115K the 50-thread run was latency-capped at, and stress-tests the
# follower crash fix under heavier load.
set -uo pipefail
cd "$(dirname "$0")"
THREADS_LIST="${*:-100 200 400}"
OUT=/tmp/thread_sweep
mkdir -p "$OUT"
VARIANT=custom_reshard_v2_orch_raft_chunked
WORKLOAD=workloada_prod
RESDIR=/tmp/experiments/${VARIANT}_${WORKLOAD}

for T in $THREADS_LIST; do
  echo "############ THREADS=$T ############"
  ansible-playbook -i inventory.ini experiments/${VARIANT}/workload.yml \
    -e redis_variant=custom \
    -e pre_reshard_pause=10 \
    -e experiment_name=${VARIANT}_${WORKLOAD} \
    -e redis_workload=${WORKLOAD} \
    -e ycsb_threads_run="$T" \
    > "$OUT/run_t${T}.log" 2>&1
  echo "threads=$T run exit=$?"

  rm -rf "$OUT/results_t${T}"
  cp -r "$RESDIR" "$OUT/results_t${T}" 2>/dev/null || echo "WARN: no results dir to copy"

  RES="$OUT/results_t${T}/ycsb/ycsb0/tmp/ycsb_output_ycsb0"
  {
    echo "--- threads=$T summary ---"
    grep -E "^\[OVERALL\]|Return=ERROR|READ-FAILED\], Operations|UPDATE-FAILED\], Operations" "$RES" 2>/dev/null
  } | tee "$OUT/summary_t${T}.txt"

  # follower crash check (residual on-host logs, before next run wipes them)
  echo "--- follower crash check threads=$T ---" | tee -a "$OUT/summary_t${T}.txt"
  ansible 'redis4:ycsb1' -i inventory.ini -m shell -a 'grep -cE "REDIS BUG REPORT|Crashed by signal|SIGSEGV|signal handler" /tmp/redis_logs/*sg4*.log; true' \
    2>/dev/null | grep -vE "DEPRECATION|python|backward|docs.ansible|feature will|deprecation|prior Ansible|reference_app" \
    | tee -a "$OUT/summary_t${T}.txt"
done
echo "SWEEP DONE. Results in $OUT (results_t*, summary_t*.txt)"
