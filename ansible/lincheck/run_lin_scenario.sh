#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_lin_scenario.sh <HEALTHY|S1..S5>
#
# Linearizability run: the usual reshard (+ crash injection for S1..S5), with
# LinHistoryClient recording a per-operation history on ycsb0 the whole time,
# then a final read pass after the migration has finished and faults healed,
# then lincheck (Porcupine, per key) on the history.
#
#   1. background: wait for the YCSB run phase to start on ycsb0 (cluster up,
#      data loaded, before the pre-reshard pause ends) and start the client;
#   2. HEALTHY: run workload_nround.yml directly; S1..S5: run_crash_scenario.sh
#      (injector + playbook + verdict.sh), both with raft_quorum_reads;
#   3. touch the stop file -> client stops the workload, does the final pass;
#   4. fetch the history and run lincheck;
#   5. postcheck.py: migration durable on the current sg4 leader, no crashes,
#      live sg4 replicas identical, donors agree, no migrated key lost.
# OVERALL = 4 AND 5. How to run / read results: HOWTO_LINCHECK.md.
#
# Env: PROFILE (quick|full), QUORUM_READS (yes), LIN_KEYS (200), LIN_THREADS (32), LIN_RATE (50),
#      LIN_DEL_PCT (10), LIN_CTR_KEYS (0), LIN_INCR_PCT (0), WORKLOAD,
#      YCSB_THREADS, PRE_PAUSE, EXTRA_ANSIBLE_ARGS, OUT_ROOT (/tmp/lincheck/runs)
# Other topologies (experiments/custom_scaleout_3to6/run_scenario.sh sets these):
#      WORKLOAD_PLAYBOOK, SCENARIOS_ENV, EXP_PREFIX (see run_crash_scenario.sh), LIN_SLOT_RANGES
#      (slots the test keys hash into), AQ_TOPOLOGY (see postcheck.py)
# Result: $OUT_ROOT/<scenario>_q<yes|no>_<ts>/{history.jsonl,lincheck.txt,...}
# ---------------------------------------------------------------------------
set -uo pipefail

SCENARIO="${1:-}"
if [[ ! "$SCENARIO" =~ ^(HEALTHY|S[1-9])$ ]]; then
  echo "usage: $0 <HEALTHY|S1..S5>" >&2; exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ANSIBLE_DIR="$REPO_ROOT/ansible"
LINCHECK="${LINCHECK:-$REPO_ROOT/lincheck/lincheck}"
QR="${QUORUM_READS:-yes}"
TS="$(date -u +%Y%m%d_%H%M%S)"
OUT="${OUT_ROOT:-/tmp/lincheck/runs}/${SCENARIO}_q${QR}_${TS}"
CLIENT_HOST="${CLIENT_HOST:-ycsb0}"
SSH="sudo ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=8"
CP="/users/entall/ycsb_client/redis-binding/lib/*:/users/entall/ycsb_client/lib/*"
RH=/tmp/lin_history.jsonl
RSTOP=/tmp/lin_stop
RLOG=/tmp/lin_client.log
# PROFILE=quick (default): 300k keys loaded fresh, 90 s YCSB run — a full
# migration + check in a few minutes, for correctness iteration.
# PROFILE=full: the 30M snapshot restore + 3-min run (the paper-scale setup).
PROFILE="${PROFILE:-quick}"
COMMON_EXTRA="-e rdma_src_prereg_slots=1365 -e rdma_landing_prereg_pools=3 -e rdma_chain_ack_via_raft=yes"
if [ "$PROFILE" = "full" ]; then
  EXTRA="${EXTRA_ANSIBLE_ARGS:--e dataset_snapshot=ws30m $COMMON_EXTRA}"
  WORKLOAD="${WORKLOAD:-workloada_prod_30m_run3min}"
  YCSB_THREADS="${YCSB_THREADS:-100}"
  PRE_PAUSE="${PRE_PAUSE:-60}"
else
  EXTRA="${EXTRA_ANSIBLE_ARGS:-$COMMON_EXTRA}"
  WORKLOAD="${WORKLOAD:-workloada_lin_quick}"
  YCSB_THREADS="${YCSB_THREADS:-32}"
  PRE_PAUSE="${PRE_PAUSE:-20}"
fi
EXTRA="$EXTRA -e raft_quorum_reads=$QR"

mkdir -p "$OUT"
say() { echo "[lin $(date -u +%H:%M:%S)] $*" | tee -a "$OUT/run.log"; }

LAUNCHER_PID=""
GRAB_PID=""
RUN_SID=""
cleanup() {
  [ -n "$LAUNCHER_PID" ] && kill "$LAUNCHER_PID" 2>/dev/null
  [ -n "$GRAB_PID" ] && kill "$GRAB_PID" 2>/dev/null
  # A leftover injector would kill a node in the NEXT run. Only touch the one
  # this run started (same session id): other campaigns share this host.
  if [ -n "$RUN_SID" ]; then
    for p in $(ps -eo pid=,sid=,args= | awk -v s="$RUN_SID" '$2==s && /crash_inject\.sh/ {print $1}'); do
      sudo kill "$p" 2>/dev/null
    done
  fi
  $SSH "$CLIENT_HOST" "pkill -f '[L]inHistoryClient'" 2>/dev/null
  kill_migration_loop
}
# The reshard task's bash loop (on this host) outlives a killed ansible-playbook
# and keeps re-driving MGN-RECOVER into whatever cluster runs next.
kill_migration_loop() {
  for p in $(ps -eo pid=,args= | awk '/PS=1365; NR=|scaleout_pairs\.py (prepare|migrate)/ && !/awk/ {print $1}'); do sudo kill "$p" 2>/dev/null; done
}
trap cleanup EXIT
trap 'say "interrupted"; exit 130' INT TERM

say "scenario=$SCENARIO profile=$PROFILE workload=$WORKLOAD quorum_reads=$QR out=$OUT"
kill_migration_loop
$SSH "$CLIENT_HOST" "pkill -f '[L]inHistoryClient'; rm -f $RH $RSTOP $RLOG" 2>/dev/null

# 1. start the history client once the YCSB run phase is up on the client host
(
  for i in $(seq 1 360); do   # up to 30 min (dataset restore)
    if $SSH "$CLIENT_HOST" "pgrep -f 'site[.]ycsb[.]Client -t' >/dev/null" 2>/dev/null; then break; fi
    sleep 5
  done
  $SSH "$CLIENT_HOST" "nohup setsid java -Xmx4g -cp '$CP' site.ycsb.db.LinHistoryClient \
      --seed-host redis0 --seed-port 8000 --keys ${LIN_KEYS:-200} --threads ${LIN_THREADS:-32} \
      --rate ${LIN_RATE:-50} --del-pct ${LIN_DEL_PCT:-10} --ctr-keys ${LIN_CTR_KEYS:-0} \
      --incr-pct ${LIN_INCR_PCT:-0} ${LIN_SLOT_RANGES:+--slot-ranges $LIN_SLOT_RANGES} --out $RH --stop-file $RSTOP --max-duration-sec 5400 \
      > $RLOG 2>&1 < /dev/null &"
  echo "[lin $(date -u +%H:%M:%S)] history client started on $CLIENT_HOST" >> "$OUT/run.log"
) &
LAUNCHER_PID=$!

# Node logs: the playbook's collect_results deletes /tmp/redis_logs on the hosts
# (and run_crash_scenario then overwrites the fetched sg4 leader log with an
# empty file). Copy every host's logs every 10 s during the run, keeping the
# larger copy, into $OUT/logs/<host>/.
grab_logs() {
  for h in ${LOG_HOSTS:-redis0 redis1 redis2 redis3 redis4 redis5}; do
    mkdir -p "$OUT/logs/$h"
    t=$(mktemp -d)
    $SSH "$h" 'tar -C /tmp/redis_logs -cf - . 2>/dev/null' 2>/dev/null | tar -C "$t" -xf - 2>/dev/null
    for f in "$t"/*.log; do
      [ -s "$f" ] || continue
      b=$(basename "$f")
      # Logs already on the host when this run began are the PREVIOUS run's
      # (usually larger than the new ones, so "keep the larger copy" would keep
      # them for the whole run). Remember their first line on the first pass and
      # never collect a file that still starts with it.
      if [ ! -e "$OUT/logs/$h/.stale_done" ]; then head -n1 "$f" > "$OUT/logs/$h/.stale_$b"; continue; fi
      [ -e "$OUT/logs/$h/.stale_$b" ] && [ "$(head -n1 "$f")" = "$(cat "$OUT/logs/$h/.stale_$b")" ] && continue
      [ "$(stat -c %s "$f")" -gt "$(stat -c %s "$OUT/logs/$h/$b" 2>/dev/null || echo 0)" ] && cp "$f" "$OUT/logs/$h/$b"
    done
    rm -rf "$t"
    touch "$OUT/logs/$h/.stale_done"
  done
}
( while true; do grab_logs; sleep 10; done ) &
GRAB_PID=$!

# 2. the run itself
if [ "$SCENARIO" = "HEALTHY" ]; then
  for c in ycsb0 ycsb1; do $SSH $c "rm -f /tmp/ycsb_output_*" 2>/dev/null; done
  cd "$ANSIBLE_DIR"
  sudo ansible-playbook -i "${INVENTORY:-inventory.ini}" "${WORKLOAD_PLAYBOOK:-experiments/custom_reshard_v2_orch_raft_chunked/workload_nround.yml}" \
    -e redis_variant=custom -e rdma_chain_pipeline=yes -e rdma_chain_xsession=yes -e rdma_async_apply=yes \
    -e rdma_transfer_chunk_slots=342 -e rdma_naive_durability="${RDMA_NAIVE:-no}" -e '{"rdma_follower_proxy":"no"}' \
    -e redis_workload="$WORKLOAD" -e pre_reshard_pause="$PRE_PAUSE" -e n_rounds="${ROUNDS:-1}" \
    -e rdma_migration_peer_stagger_ms=0 -e ycsb_slotpoll_ms=100 -e ycsb_threads_run="$YCSB_THREADS" $EXTRA \
    -e experiment_name="${EXP_PREFIX:-}lin_healthy" > "$OUT/playbook.log" 2>&1
  PB_RC=$?
  say "playbook rc=$PB_RC"
else
  for c in ycsb0 ycsb1; do $SSH $c "rm -f /tmp/ycsb_output_*" 2>/dev/null; done
  cd "$ANSIBLE_DIR/crash"
  WORKLOAD="$WORKLOAD" YCSB_THREADS="$YCSB_THREADS" PRE_PAUSE="$PRE_PAUSE" EXTRA_ANSIBLE_ARGS="$EXTRA" \
    setsid ./run_crash_scenario.sh "$SCENARIO" > "$OUT/playbook.log" 2>&1 &
  RUN_SID=$!
  wait "$RUN_SID"
  say "run_crash_scenario rc=$? ($(grep -aoE 'playbook exited rc=[0-9]+' "$OUT/playbook.log" | tail -1))"
  PB_RC=$(grep -aoE 'playbook exited rc=[0-9]+' "$OUT/playbook.log" | tail -1 | grep -oE '[0-9]+$')
  PB_RC=${PB_RC:-99}
  grep -aE "VERDICT|PASS|FAIL" "$OUT/playbook.log" | tail -5 >> "$OUT/run.log"
  # Injector record of THIS run (its own result file under /tmp/crash_inject is
  # root-owned; the same lines are on its stdout in playbook.log).
  grep -aE '^\[crash_inject .*(MARKER HIT|KILLED|TIMEOUT|gave up|not alive)' "$OUT/playbook.log" | tee -a "$OUT/run.log"
fi
wait "$LAUNCHER_PID" 2>/dev/null; LAUNCHER_PID=""

# 3. stop the workload; final read pass
sleep 10
$SSH "$CLIENT_HOST" "touch $RSTOP"
for i in $(seq 1 120); do
  $SSH "$CLIENT_HOST" "pgrep -f '[L]inHistoryClient' >/dev/null" 2>/dev/null || break
  sleep 5
done
$SSH "$CLIENT_HOST" "cat $RH" > "$OUT/history.jsonl"
$SSH "$CLIENT_HOST" "cat $RLOG" > "$OUT/client.log"
tail -3 "$OUT/client.log" | tee -a "$OUT/run.log"

# 4. check
"$LINCHECK" -history "$OUT/history.jsonl" -out "$OUT/check" -timeout "${CHECK_TIMEOUT:-120s}" > "$OUT/lincheck.txt" 2>&1
RC=$?
cat "$OUT/lincheck.txt" | tee -a "$OUT/run.log"
say "lincheck rc=$RC"

# 5. migration + fault tolerance: migration finished and durable on the current
#    sg4 leader, no crashes, live sg4 replicas identical, no migrated key lost.
python3 "$SCRIPT_DIR/postcheck.py" --playbook-rc "$PB_RC" --logs-dir "$OUT/logs" > "$OUT/postcheck.txt" 2>&1
PC=$?
cat "$OUT/postcheck.txt" | tee -a "$OUT/run.log"
OVERALL=PASS; { [ $RC -ne 0 ] || [ $PC -ne 0 ]; } && OVERALL=FAIL
say "OVERALL: $OVERALL (lincheck rc=$RC, postcheck rc=$PC)"
[ "$OVERALL" = PASS ] && exit 0
[ $RC -ne 0 ] && exit $RC
exit 1
