#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_all_scenarios.sh — run the full AqRaft crash campaign and collect results
# into a single directory for analysis + artifact generation.
#
#   HEALTHY  : clean reshard, no crash (the reference baseline)
#   S1..S5   : the crash scenarios (see scenarios.env / HOWTO_CRASH.md)
#
# Usage:
#   ./run_all_scenarios.sh                       # all, defaults below
#   ORDER="HEALTHY S2 S5" ./run_all_scenarios.sh # a subset
#   WORKLOAD=workloada_debug_500k YCSB_THREADS=50 PRE_PAUSE=20 ./run_all_scenarios.sh
#
# Env:
#   ORDER          scenarios to run          (default "HEALTHY S1 S2 S3 S4 S5")
#   WORKLOAD       ycsb workload             (default workloada_prod_30m_run10min)
#   YCSB_THREADS   client threads            (default 100)
#   PRE_PAUSE      steady-state seconds before reshard fires (default 60)
#   RESULTS        output dir                (default /tmp/crash_inject/campaign)
# ---------------------------------------------------------------------------
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ANS="$(cd "$HERE/.." && pwd)"
ORDER=(${ORDER:-HEALTHY S1 S2 S3 S4 S5})
WORKLOAD="${WORKLOAD:-workloada_prod_30m_run10min}"
YCSB_THREADS="${YCSB_THREADS:-100}"
PRE_PAUSE="${PRE_PAUSE:-60}"
RESULTS="${RESULTS:-/tmp/crash_inject/campaign}"
SSH="sudo ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=8"
mkdir -p "$RESULTS"; LOG="$RESULTS/sweep.log"; : > "$LOG"
say(){ echo "[campaign $(date -u +%H:%M:%S)] $*" | tee -a "$LOG"; }

collect(){ # <label> <experiment_name>
  local lab=$1 exp=$2 d="$RESULTS/$1"; mkdir -p "$d"
  for c in ycsb0 ycsb1; do
    local cf; cf=$(sudo find "/tmp/experiments/$exp/ycsb" -name "ycsb_output_$c" 2>/dev/null | head -1)
    if [ -n "$cf" ] && sudo test -s "$cf"; then sudo cat "$cf" > "$d/ycsb_$c.txt" 2>/dev/null
    else $SSH "$c" "cat /tmp/ycsb_output_$c 2>/dev/null" > "$d/ycsb_$c.txt" 2>/dev/null || true; fi
  done
  # recipient leader snapshot (armed by crash_inject at t_kill+60s) else live
  local snap; snap=$(ls -t /tmp/crash_inject/${lab}-*_leaderlog.snap 2>/dev/null | head -1)
  if [ -n "$snap" ] && [ -s "$snap" ]; then cp "$snap" "$d/redis3_sg4.log"
  else $SSH redis3 "cat /tmp/redis_logs/redis3_sg4.log 2>/dev/null" > "$d/redis3_sg4.log" 2>/dev/null || true; fi
  for h in redis4 redis5; do $SSH "$h" "cat /tmp/redis_logs/${h}_sg4.log 2>/dev/null" > "$d/${h}_sg4.log" 2>/dev/null || true; done
  for h in redis0 redis1 redis2; do $SSH "$h" "cat /tmp/redis_logs/${h}_sg1.log 2>/dev/null" > "$d/${h}_sg1.log" 2>/dev/null || true; done
  cp "$RESULTS/${lab}_run.log" "$d/run.log" 2>/dev/null || true
  say "  collected $lab: ycsb0=$(grep -acE 'sec:.*current' "$d/ycsb_ycsb0.txt" 2>/dev/null)"
}

say "campaign start: order=[${ORDER[*]}] workload=$WORKLOAD threads=$YCSB_THREADS pre_pause=$PRE_PAUSE"
for R in "${ORDER[@]}"; do
  for c in ycsb0 ycsb1; do $SSH $c "sudo rm -f /tmp/ycsb_output_* 2>/dev/null"; done
  if [ "$R" = "HEALTHY" ]; then
    say "RUN HEALTHY (no crash)"; cd "$ANS"
    sudo rm -rf /tmp/experiments/campaign_heal 2>/dev/null
    sudo ansible-playbook -i inventory.ini experiments/custom_reshard_v2_orch_raft_chunked/workload_nround.yml \
      -e redis_variant=custom -e pre_reshard_pause="$PRE_PAUSE" -e n_rounds=1 -e rdma_migration_peer_stagger_ms=0 \
      -e ycsb_slotpoll_ms=100 -e ycsb_threads_run="$YCSB_THREADS" -e rdma_chain_pipeline=yes -e rdma_chain_xsession=yes \
      -e rdma_async_apply=yes -e rdma_transfer_chunk_slots=342 -e rdma_naive_durability=no \
      -e '{"rdma_follower_proxy": "no"}' -e redis_workload="$WORKLOAD" \
      -e experiment_name=campaign_heal > "$RESULTS/HEALTHY_run.log" 2>&1 || say "  HEALTHY rc=$?"
    collect HEALTHY campaign_heal
  else
    say "RUN $R (crash)"; cd "$HERE"
    YCSB_THREADS="$YCSB_THREADS" WORKLOAD="$WORKLOAD" PRE_PAUSE="$PRE_PAUSE" \
      ./run_crash_scenario.sh "$R" > "$RESULTS/${R}_run.log" 2>&1 || say "  $R rc=$?"
    collect "$R" "crash_$(echo "$R" | tr A-Z a-z)"
  fi
done
say "CAMPAIGN DONE -> $RESULTS"
