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

# Follow only NEW lines (-n0) so a stale prior-run marker cannot fire
# instantly. grep -m<N> exits after the Nth match, closing the pipe (the
# remote tail dies on SIGPIPE). No pipefail inside this subshell so the
# pipeline's exit status is grep's (0 on match), not the SIGPIPE'd ssh.
if timeout "$MAX_WAIT" bash -c \
   "sudo ssh $SSH_OPTS '$ARM_HOST' \"tail -Fn0 '$ARM_LOG'\" | grep -m'$ARM_COUNT' -E '$ARM_MARKER' >/dev/null"; then
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

sudo ssh $SSH_OPTS "$TARGET_HOST" "kill -9 $PID"
T_KILL=$(date -u +%s.%N)
log "KILLED $TARGET_HOST pid=$PID  t_kill=$T_KILL"
echo "result=KILLED label=$LABEL target=$TARGET_HOST pid=$PID t_arm=$T_ARM t_kill=$T_KILL" >> "$RES"

if [ "$RESTART" = "yes" ]; then
  log "RESTART=yes; sleeping ${POST_KILL_DELAY}s before relaunch"
  sleep "$POST_KILL_DELAY"
  # v2 (recovery-validation): replay the exact redis-server launch command
  # for TARGET_HOST's sg instance so the node rejoins via Raft. Not wired in
  # Phase-A baseline (no per-instance boot task exists); tracked in the plan.
  log "NOTE: single-instance relaunch not wired in v1 — skipping (baseline runs use RESTART=no)"
fi

log "done."
