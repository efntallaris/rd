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

PID=$(sudo ssh $SSH_OPTS "$TARGET_HOST" "cat '$PIDFILE' 2>/dev/null" | tr -d '[:space:]')
if [ -z "${PID:-}" ]; then
  log "ERROR: pidfile $PIDFILE empty/missing on $TARGET_HOST — NO KILL"
  echo "result=NO_PID label=$LABEL" >> "$RES"
  exit 3
fi

# Verify the pid is ALIVE before killing — a stale pidfile from a previous
# run points at a dead pid and kill -9 would be a silent no-op reported as
# a kill (observed: phantom kill -> scenario silently not exercised).
if ! sudo ssh $SSH_OPTS "$TARGET_HOST" "kill -0 $PID 2>/dev/null"; then
  log "ERROR: pid $PID from $PIDFILE is NOT alive on $TARGET_HOST — stale pidfile, NO KILL"
  echo "result=STALE_PID label=$LABEL target=$TARGET_HOST pid=$PID" >> "$RES"
  exit 3
fi
sudo ssh $SSH_OPTS "$TARGET_HOST" "kill -9 $PID"
T_KILL=$(date -u +%s.%N)
log "KILLED $TARGET_HOST pid=$PID (verified alive)  t_kill=$T_KILL"
echo "result=KILLED label=$LABEL target=$TARGET_HOST pid=$PID t_arm=$T_ARM t_kill=$T_KILL" >> "$RES"

# Durable leader-log snapshot. The arm-host (recipient LEADER) log is WIPED at
# end-of-run (the instance is restarted at teardown), destroying the re-form /
# CHAIN-ACK evidence before verdict.sh reads it (observed 2026-07-08: collected
# redis3 log = 0 bytes). Snapshot it ~120s after the kill — re-form + finalize
# complete within ~30s, the wipe is ~10min later — to a path that survives.
LEADER_SNAP="$RESULT_DIR/${LABEL}_leaderlog.snap"
log "arming leader-log snapshot of $ARM_HOST:$ARM_LOG at t_kill+120s -> $LEADER_SNAP"
sleep 120
sudo ssh $SSH_OPTS "$ARM_HOST" "cat '$ARM_LOG'" > "$LEADER_SNAP" 2>/dev/null
log "leader-log snapshot captured ($(wc -l < "$LEADER_SNAP" 2>/dev/null || echo 0) lines)"

if [ "$RESTART" = "yes" ]; then
  log "RESTART=yes; sleeping ${POST_KILL_DELAY}s before relaunch"
  sleep "$POST_KILL_DELAY"
  # v2 (recovery-validation): replay the exact redis-server launch command
  # for TARGET_HOST's sg instance so the node rejoins via Raft. Not wired in
  # Phase-A baseline (no per-instance boot task exists); tracked in the plan.
  log "NOTE: single-instance relaunch not wired in v1 — skipping (baseline runs use RESTART=no)"
fi

log "done."
