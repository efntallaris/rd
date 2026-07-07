#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# verdict.sh <S1|S2|S3|S4> [experiment_name]
#
# Read-only observation collector for a crash-scenario run. Gathers crash
# signatures, Raft leader changes, chain-degrade markers, recipient DBSIZE,
# and YCSB throughput/errors, then prints an OBSERVATIONS block plus the
# baseline expectation for that scenario. S3 (the control) also prints a hard
# PASS/FAIL; S1/S2/S4 print observations against the documented baseline gap
# (they are EXPECTED to show the gap until the Phase-B source fixes land).
# ---------------------------------------------------------------------------
set -uo pipefail

SCENARIO="${1:?usage: verdict.sh <S1|S2|S3|S4> [exp_name]}"
EXP="${2:-crash_${SCENARIO,,}}"
B="/tmp/experiments/$EXP"
SSH="sudo ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"
CRASH_RE="ASSERTION FAILED|REDIS BUG|Crashed by signal|SIGSEGV|dict.c:548"

ssh_grep_c() { $SSH "$1" "grep -icE '$3' '$2' 2>/dev/null" 2>/dev/null || echo "?"; }
ssh_cmd()    { $SSH "$1" "$2" 2>/dev/null || echo "(unreachable)"; }

hr() { printf '%s\n' "------------------------------------------------------------"; }
echo; hr; echo " VERDICT — $SCENARIO   exp=$EXP"; hr

# --- crash signatures across the involved instances -------------------------
echo "[crash signatures] (want 0)"
for spec in "redis3 /tmp/redis_logs/redis3_sg4.log" \
            "redis4 /tmp/redis_logs/redis4_sg4.log" \
            "redis5 /tmp/redis_logs/redis5_sg4.log" \
            "redis0 /tmp/redis_logs/redis0_sg1.log" \
            "redis1 /tmp/redis_logs/redis1_sg1.log"; do
  set -- $spec
  echo "   $1 $(basename "$2"): $(ssh_grep_c "$1" "$2" "$CRASH_RE")"
done

# --- Raft leader elections on sg4 (a follower promoted?) --------------------
echo "[sg4 leader changes] 'Node is now a leader' (S1 expects >=1 on redis4/5)"
for h in redis3 redis4 redis5; do
  echo "   $h: $(ssh_grep_c "$h" "/tmp/redis_logs/${h}_sg4.log" 'State change: Node is now a leader')"
done

# --- chain degrade marker (S4) ---------------------------------------------
echo "[chain degrade] 'firing MGN_INDX_UPD anyway' on redis3 (S4 baseline: >0)"
echo "   redis3: $(ssh_grep_c redis3 /tmp/redis_logs/redis3_sg4.log 'firing MGN_INDX_UPD anyway')"

# --- recipient integrity ----------------------------------------------------
echo "[recipient DBSIZE] redis3:8000 (expect ~7,492,752 on a clean/rolled-forward run)"
echo "   $(ssh_cmd redis3 "/users/entall/rd/redis/src/redis-cli -p 8000 DBSIZE")"
echo "[bg-merge moved/skipped] (recipient adopted keys)"
echo "   $(ssh_cmd redis3 "grep 'AqRaft bg-merge' /tmp/redis_logs/redis3_sg4.log | tail -1 | grep -oE 'moved=[0-9]+ skipped=[0-9]+'")"

# --- YCSB throughput + errors (LAST test block) -----------------------------
echo "[ycsb] last-block throughput + UPDATE errors (per client)"
for c in ycsb0 ycsb1; do
  f="$B/ycsb/$c/tmp/ycsb_output_$c"
  if [ -r "$f" ]; then
    s=$(grep -nE "Command line:" "$f" | tail -1 | cut -d: -f1)
    blk=$(sed -n "${s:-1},\$p" "$f")
    thr=$(echo "$blk" | grep -E "^\[OVERALL\], Throughput" | tail -1)
    uerr=$(echo "$blk" | grep -cE "^\[UPDATE\].*Return=ERROR")
    rerr=$(echo "$blk" | grep -cE "^\[READ\].*Return=ERROR")
    echo "   $c: ${thr:-<no throughput line>}  | UPDATE-err lines=$uerr READ-err lines=$rerr"
  else
    echo "   $c: (no output at $f)"
  fi
done

# --- migration status -------------------------------------------------------
echo "[migration] TXN_DONE / MIGRATE-ALL-STATUS markers on recipient"
echo "   redis3 TXN_DONE count: $(ssh_grep_c redis3 /tmp/redis_logs/redis3_sg4.log 'MGN_TXN_DONE|TXN_DONE')"

# --- injection record -------------------------------------------------------
echo "[injection] $(cat /tmp/crash_inject/${SCENARIO}*-*.txt 2>/dev/null | grep -E '^result=' | tail -1 || echo '(no record)')"

hr
case "$SCENARIO" in
  S1) echo " BASELINE EXPECTATION: recipient down; a follower may log 'now a leader'";
      echo " but migration NOT resumed -> UPDATE errors + short DBSIZE. (Gap until Phase-B #1.)";;
  S2) echo " BASELINE EXPECTATION: donor RDMA_MIG_FAILED; recipient batch stuck;";
      echo " orchestration FAILED. (Gap until Phase-B #2.)";;
  S3) echo " CONTROL — should PASS TODAY: migration DONE, DBSIZE intact, crash-sig=0,";
      echo " no sg4 leader change from a donor-follower kill.";
      echo "   >>> Inspect the block above: crash-sigs 0 + DBSIZE ~7.49M + UPDATE-err ~0 = PASS.";;
  S4) echo " BASELINE EXPECTATION: migration completes but 'firing MGN_INDX_UPD anyway' > 0";
      echo " (chain degraded to Raft-only; killed follower missing bytes). (Gap until Phase-B #4.)";;
esac
hr; echo
