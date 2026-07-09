#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# verdict.sh <S1|S2|S3|S4> [experiment_name]
#
# Read-only observation collector for a crash-scenario run. Reads redis logs
# from the COLLECTED experiment dir (/tmp/experiments/<exp>/logs/...) — the
# live /tmp/redis_logs is unreliable by verdict time (leader log gets wiped).
# The base collect_results skips the sg4 followers redis4/redis5, so those are
# fetched live (dead redis4's log is frozen; redis5 is alive) into a cache.
#
# Prints an OBSERVATIONS block plus the baseline expectation. S3 (control)
# should pass on the current binary; S1/S2/S4 reproduce the documented gaps
# until the Phase-B source fixes land.
# ---------------------------------------------------------------------------
set -uo pipefail

SCENARIO="${1:?usage: verdict.sh <S1|S2|S3|S4> [exp_name]}"
EXP="${2:-crash_${SCENARIO,,}}"
B="/tmp/experiments/$EXP"
LOGROOT="$B/logs"
FETCH="/tmp/crash_inject/${EXP}_logs"; mkdir -p "$FETCH"
SSHO="-o StrictHostKeyChecking=accept-new -o ConnectTimeout=8"
CRASH_RE="ASSERTION FAILED|REDIS BUG|Crashed by signal|SIGSEGV|dict.c:548"

# resolve_log <host> <sg> -> prints a local readable path (collected first,
# else a fresh live fetch), or empty if neither is available.
resolve_log() {
  local host="$1" sg="$2"
  # Recipient LEADER (redis3) log is wiped at end-of-run; prefer the injector's
  # durable leader-log snapshot (crash_inject.sh, taken ~120s after the kill) if
  # present and non-empty — the collected/live copies are empty/stale by now.
  if [ "$host" = "redis3" ]; then
    local snap
    snap=$(ls -1 /tmp/crash_inject/${SCENARIO}*_leaderlog.snap 2>/dev/null | head -1)
    if [ -n "$snap" ] && sudo test -s "$snap" 2>/dev/null; then echo "$snap"; return; fi
  fi
  local col="$LOGROOT/$host/tmp/redis_logs/${host}_${sg}.log"
  if sudo test -f "$col" 2>/dev/null && [ "$(sudo wc -l < "$col" 2>/dev/null || echo 0)" -gt 0 ]; then echo "$col"; return; fi
  local dst="$FETCH/${host}_${sg}.log"
  if sudo ssh $SSHO "$host" "test -f /tmp/redis_logs/${host}_${sg}.log" 2>/dev/null; then
    sudo ssh $SSHO "$host" "cat /tmp/redis_logs/${host}_${sg}.log" >"$dst" 2>/dev/null && echo "$dst" && return
  fi
  echo ""
}
grepc() { local p="$1" re="$2" n; [ -n "$p" ] || { echo "n/a"; return; }
  n=$(sudo grep -icaE "$re" "$p" 2>/dev/null || true); echo "${n:-0}"; }

hr() { printf '%s\n' "------------------------------------------------------------"; }
echo; hr; echo " VERDICT — $SCENARIO   exp=$EXP"; hr

# --- crash signatures -------------------------------------------------------
echo "[crash signatures] (want 0)"
for spec in "redis3 sg4" "redis4 sg4" "redis5 sg4" "redis0 sg1" "redis1 sg1"; do
  set -- $spec; p=$(resolve_log "$1" "$2")
  echo "   ${1}_${2}: $(grepc "$p" "$CRASH_RE")   ${p:+[$(basename "$(dirname "$(dirname "$(dirname "$p")")")" 2>/dev/null || echo live)]}"
done

# --- Raft leader elections on sg4 ------------------------------------------
echo "[sg4 leader changes] 'Node is now a leader'  (S1 expects >=1 on redis4/5)"
for h in redis3 redis4 redis5; do
  p=$(resolve_log "$h" sg4); echo "   $h: $(grepc "$p" 'State change: Node is now a leader')"
done

# --- chain degrade (S4): BOTH paths — 5s-timeout ('anyway') AND the
#     synchronous forward-failure ('immediately as fallback', fires when the
#     pipelined forward to a dead F1 errors: Connection reset / poll_send). ---
p3=$(resolve_log redis3 sg4)
echo "[chain degrade] 'firing MGN_INDX_UPD (anyway|immediately)' on redis3  (S4 baseline: >0)"
echo "   redis3 total:      $(grepc "$p3" 'firing MGN_INDX_UPD')"
echo "   redis3 immediate:  $(grepc "$p3" 'firing MGN_INDX_UPD immediately')"
echo "   redis3 5s-timeout: $(grepc "$p3" 'firing MGN_INDX_UPD anyway')"

# --- migration completion ---------------------------------------------------
echo "[migration] markers on recipient (redis3)"
echo "   TXN_DONE:  $(grepc "$p3" 'TXN_DONE')   INDX_UPD applied: $(grepc "$p3" 'MGN_INDX_UPD applied')"
echo "   bg-merge:  $( [ -n "$p3" ] && sudo grep -a 'AqRaft bg-merge' "$p3" 2>/dev/null | tail -1 | grep -oE 'moved=[0-9]+ skipped=[0-9]+' || echo 'n/a')"

# --- recipient integrity (live; cluster usually still up at verdict) --------
echo "[recipient DBSIZE] redis3:8000 (expect ~7,492,752)"
echo "   $(sudo ssh $SSHO redis3 "/users/entall/rd/redis/src/redis-cli -p 8000 DBSIZE" 2>/dev/null || echo '(unreachable)')"

# --- YCSB throughput + errors (LAST test block) -----------------------------
echo "[ycsb] last-block throughput + error lines (per client)"
for c in ycsb0 ycsb1; do
  f="$B/ycsb/$c/tmp/ycsb_output_$c"
  if [ -r "$f" ]; then
    s=$(grep -nE "Command line:" "$f" | tail -1 | cut -d: -f1)
    blk=$(sed -n "${s:-1},\$p" "$f")
    thr=$(echo "$blk" | grep -E "^\[OVERALL\], Throughput" | tail -1)
    uerr=$(echo "$blk" | grep -cE "^\[UPDATE\].*Return=ERROR")
    rerr=$(echo "$blk" | grep -cE "^\[READ\].*Return=ERROR")
    echo "   $c: ${thr:-<none>}  | UPDATE-err=$uerr READ-err=$rerr"
  else
    echo "   $c: (no output at $f)"
  fi
done

# --- injection record -------------------------------------------------------
echo "[injection] $(grep -E '^result=' /tmp/crash_inject/${SCENARIO}*-*.txt 2>/dev/null | tail -1 || echo '(no record)')"

hr
case "$SCENARIO" in
  S1) echo " BASELINE EXPECTATION: recipient down; a follower may log 'now a leader'";
      echo " but migration NOT resumed -> UPDATE errors + short DBSIZE. (Gap until Phase-B #1.)";;
  S2) echo " BASELINE EXPECTATION: donor RDMA_MIG_FAILED; recipient batch stuck;";
      echo " orchestration FAILED. (Gap until Phase-B #2.)";;
  S3) echo " CONTROL — should PASS TODAY: crash-sig=0, DBSIZE ~7.49M, UPDATE-err ~0,";
      echo " no sg4 leader change, degrade=0.";;
  S4) echo " BASELINE EXPECTATION: migration completes but 'firing MGN_INDX_UPD anyway' > 0";
      echo " (chain degraded to Raft-only; killed follower missing bytes). (Gap until Phase-B #4.)";;
esac
hr; echo
