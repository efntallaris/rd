#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# crash_inject.sh — shared fault-injection killer for AqRaft crash scenarios.
#
# Watches ARM_LOG on ARM_HOST until ARM_MARKER has appeared ARM_COUNT times
# (only NEW lines), then `kill -9` the PID in PIDFILE on TARGET_HOST.
# The arm SOURCE (log to watch) and the kill TARGET (pidfile) may differ —
# e.g. S3/S4 arm off the recipient leader's log but kill a follower.
#
# Designed to be launched in the BACKGROUND just before the base 30M reshard
# run (the migration is a single Ansible task, so injection must be
# out-of-band). Privileged ops use `sudo ssh` per the HOWTO convention.
#
# Env (required):  ARM_HOST ARM_LOG ARM_MARKER TARGET_HOST PIDFILE
# Env (optional):
#   ARM_COUNT        Nth marker occurrence to fire on   (default 1)
#   PIDFILE_EXTRA    more pidfiles on TARGET_HOST (space separated), killed by
#                    the same kill -9 as PIDFILE: a whole-host crash when the
#                    host runs several instances            (default none)
#   LABEL            human tag for result files         (default inject)
#   MAX_WAIT         seconds to wait for the marker      (default 1800)
#   RESTART          yes|no — relaunch after kill        (default no)
#   POST_KILL_DELAY  seconds before RESTART              (default 0)
#   RESULT_DIR       where to write results              (default /tmp/crash_inject)
#   SSH_OPTS         extra ssh options
# Exit: 0 killed, 2 marker never seen, 3 pidfile empty/missing.
# ---------------------------------------------------------------------------
set -uo pipefail

: "${ARM_HOST:?set ARM_HOST}"
: "${ARM_LOG:?set ARM_LOG}"
: "${ARM_MARKER:?set ARM_MARKER}"
: "${TARGET_HOST:?set TARGET_HOST}"
: "${PIDFILE:?set PIDFILE}"

ARM_COUNT="${ARM_COUNT:-1}"
LABEL="${LABEL:-inject}"
MAX_WAIT="${MAX_WAIT:-1800}"
RESTART="${RESTART:-no}"
POST_KILL_DELAY="${POST_KILL_DELAY:-0}"
RESULT_DIR="${RESULT_DIR:-/tmp/crash_inject}"
SSH_OPTS="${SSH_OPTS:--o StrictHostKeyChecking=accept-new -o ConnectTimeout=10}"

mkdir -p "$RESULT_DIR"
RES="$RESULT_DIR/${LABEL}.txt"

log() { echo "[crash_inject $(date -u +%H:%M:%S)] $*" | tee -a "$RES" >&2; }

log "==== scenario=$LABEL ===="
log "ARM   : $ARM_HOST:$ARM_LOG  marker=/$ARM_MARKER/  x$ARM_COUNT"
log "TARGET: $TARGET_HOST  kill -9 \$(cat $PIDFILE)"
# NOTE (2026-11-05): ssh ControlMaster pre-warming was tried here to shrink the
# double-crash skew further. It is NOT worth it — with the parallel one-round-trip
# kills below the skew is already +-3 ms over 3 dummy-process trials, and the
# pre-warmed runs measured 58/0/-1 ms, i.e. no better and occasionally worse.
log "waiting for marker (max ${MAX_WAIT}s)..."

# Remote POLL loop (single ssh), two phases:
#   Phase 0 (fresh-log gate): the injector is armed BEFORE the playbook starts,
#   so the arm log (and pidfile) on disk are STALE leftovers of the previous
#   run — grepping them would fire instantly on old markers and then kill a
#   dead pid (observed: phantom kill). clean_runtime wipes the log dir and the
#   fresh boot recreates the log; gate on that wipe -> reappear transition
#   before counting anything.
#   Phase 1 (marker count): re-grep the FRESH log every 0.3s; exit 0 once the
#   marker appeared >= ARM_COUNT times. grep -c re-opens the file per
#   iteration (immune to rotation) and exits 1 on zero count, so compare the
#   captured number, not grep's exit status.
POLL='log="__LOG__"; n=__N__;
if [ -e "$log" ]; then while [ -e "$log" ]; do sleep 0.3; done; fi
while [ ! -e "$log" ]; do sleep 0.3; done
while :; do c=$(grep -acE "__RE__" "$log" 2>/dev/null); c=${c:-0}; [ "$c" -ge "$n" ] && exit 0; sleep 0.3; done'
POLL="${POLL//__LOG__/$ARM_LOG}"; POLL="${POLL//__N__/$ARM_COUNT}"; POLL="${POLL//__RE__/$ARM_MARKER}"
if timeout "$MAX_WAIT" sudo ssh $SSH_OPTS "$ARM_HOST" "$POLL"; then
  T_ARM=$(date -u +%s.%N)
  log "MARKER HIT (x$ARM_COUNT)  t_arm=$T_ARM"
else
  log "ERROR: marker not seen within ${MAX_WAIT}s — NO KILL performed"
  echo "result=NO_MARKER label=$LABEL" >> "$RES"
  exit 2
fi

# Both targets are killed by ONE ssh round trip each, fired in parallel: the
# remote shell resolves the pidfile, verifies the pid is alive and kill -9s it
# (together with PIDFILE_EXTRA, for a whole-host crash) in the same session.
#
# 2026-11-05: the primary target used to need three SEQUENTIAL ssh connections
# (cat the pidfile, then kill -0, then kill -9) while the second target already
# used one, so S5's "simultaneous" double crash landed 345 ms apart and the
# SECOND target died FIRST (2026-11-04: redis3 at t_kill+0.191, redis0 at
# +0.403, and sg4 was 0.33 s into its election before sg1 even died). One round
# trip each also makes verify-then-kill atomic on the target rather than two
# handshakes apart, which is what the stale-pidfile guard below wants anyway.
killTarget() {   # host pidfile extra -> "OK <pid> <extra...>" | "NOPID" | "STALE <pid>"
  local host="$1" pf="$2" extra="${3:-/dev/null}"
  sudo ssh $SSH_OPTS "$host" "
    p=\$(cat '$pf' 2>/dev/null | tr -d '[:space:]')
    [ -z \"\$p\" ] && { echo NOPID; exit 0; }
    kill -0 \"\$p\" 2>/dev/null || { echo \"STALE \$p\"; exit 0; }
    e=\$(cat $extra 2>/dev/null | tr '\n' ' ')
    kill -9 \$p \$e 2>/dev/null
    echo \"OK \$p \$e\""
}

R1="$RESULT_DIR/.kill1.$$"; R2="$RESULT_DIR/.kill2.$$"
( killTarget "$TARGET_HOST" "$PIDFILE" "${PIDFILE_EXTRA:-/dev/null}" > "$R1"
  date -u +%s.%N >> "$R1" ) &
K1=$!
if [ -n "${TARGET_HOST2:-}" ] && [ -n "${PIDFILE2:-}" ]; then
  ( killTarget "$TARGET_HOST2" "$PIDFILE2" > "$R2"
    date -u +%s.%N >> "$R2" ) &
  K2=$!
fi
wait "$K1"; [ -n "${K2:-}" ] && wait "$K2"

read -r ST1 PID1 EXTRA1 < "$R1"; T_KILL=$(tail -1 "$R1"); rm -f "$R1"
case "${ST1:-}" in
  NOPID)
    log "ERROR: pidfile $PIDFILE empty/missing on $TARGET_HOST - NO KILL"
    echo "result=NO_PID label=$LABEL" >> "$RES"; exit 3 ;;
  STALE)
    log "ERROR: pid ${PID1:-?} from $PIDFILE is NOT alive on $TARGET_HOST - stale pidfile, NO KILL"
    echo "result=STALE_PID label=$LABEL target=$TARGET_HOST pid=${PID1:-}" >> "$RES"; exit 3 ;;
  OK) : ;;
  *)
    log "ERROR: no answer from $TARGET_HOST for $PIDFILE (ssh failed?) - NO KILL"
    echo "result=NO_PID label=$LABEL" >> "$RES"; exit 3 ;;
esac
PID="$PID1"
[ -n "${PIDFILE_EXTRA:-}" ] && log "also killed on $TARGET_HOST: $PIDFILE_EXTRA -> pids ${EXTRA1:-<none>}"
log "KILLED $TARGET_HOST pid=$PID (verified alive)  t_kill=$T_KILL"
echo "result=KILLED label=$LABEL target=$TARGET_HOST pid=$PID t_arm=$T_ARM t_kill=$T_KILL" >> "$RES"

if [ -n "${K2:-}" ]; then
  read -r ST2 PID2 _ < "$R2"; T_KILL2=$(tail -1 "$R2"); rm -f "$R2"
  if [ "${ST2:-}" = OK ]; then
    log "KILLED $TARGET_HOST2 pid=$PID2 (verified alive)  t_kill=$T_KILL2"
    log "double-crash skew: $(awk "BEGIN{printf \"%+.0f ms ($TARGET_HOST2 relative to $TARGET_HOST)\", ($T_KILL2-$T_KILL)*1000}")"
  else
    log "ERROR: second target $TARGET_HOST2:$PIDFILE2 not alive (${ST2:-no answer}) -- NO KILL"
  fi
fi

# Durable leader-log snapshot. The arm-host (recipient LEADER) log is WIPED at
# end-of-run (the instance is restarted at teardown), destroying the re-form /
# CHAIN-ACK evidence before verdict.sh reads it (observed 2026-07-08: collected
# redis3 log = 0 bytes). Snapshot it after the kill to a path that survives.
#
# DETACHED (setsid) + t_kill+60s: for LEADER-kill scenarios (S1/S2) the migration
# FAILS FAST, so the wrapper reaps this injector early — a synchronous sleep was
# killed before it fired (observed 2026-07-09: S1/S2 snapshots empty). Detaching
# survives the reap; 60s (< the ~90s fail-fast playbook end) means the snapshot is
# ready BEFORE verdict.sh runs, yet still captures re-form+ack+RECP_DURABLE (all within
# ~30s of the kill). For follower-kill S4 the run lasts ~10min so 60s is ample too.
LEADER_SNAP="$RESULT_DIR/${LABEL}_leaderlog.snap"
log "arming DETACHED leader-log snapshot of $ARM_HOST:$ARM_LOG at t_kill+60s -> $LEADER_SNAP"
setsid bash -c "sleep 60; sudo ssh $SSH_OPTS '$ARM_HOST' \"cat '$ARM_LOG'\" > '$LEADER_SNAP' 2>/dev/null; echo \"[crash_inject snapshot] captured \$(wc -l < '$LEADER_SNAP' 2>/dev/null || echo 0) lines -> $LEADER_SNAP\" >> '$RES'" </dev/null >/dev/null 2>&1 &
disown 2>/dev/null || true
log "leader-log snapshot detached (fires at t_kill+60s regardless of run outcome)"

if [ "$RESTART" = "yes" ]; then
  log "RESTART=yes; sleeping ${POST_KILL_DELAY}s before relaunch"
  sleep "$POST_KILL_DELAY"
  # v2 (recovery-validation): replay the exact redis-server launch command
  # for TARGET_HOST's sg instance so the node rejoins via Raft. Not wired in
  # Phase-A baseline (no per-instance boot task exists); tracked in the plan.
  log "NOTE: single-instance relaunch not wired in v1 — skipping (baseline runs use RESTART=no)"
fi

log "done."
