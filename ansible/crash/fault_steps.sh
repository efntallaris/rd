#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# fault_steps.sh -- multi-step fault injector (crash scenarios C1-C16). The
# single-kill scenarios use crash_inject.sh; this one runs a list of steps:
#
#   STEPS="arm redis3 sg4 DONE-SLOTS-CHUNK 4; kill redis3 sg4; armany 'probe peers' 1 redis4:sg4 redis5:sg4; kill @"
#
#   arm HOST SG REGEX [N]       wait until REGEX has appeared N times (default 1) in
#                               HOST's SG log, counting only lines written after this
#                               step starts (the first arm also waits for the fresh log
#                               of this run, as crash_inject.sh does)
#   armany REGEX N HOST:SG ...  the same over several logs; the first that reaches N
#                               becomes "@" for the next steps
#   kill HOST SG | kill @       kill -9
#   restart HOST SG | restart @ wait until the killed process is gone, relaunch it with its
#                               start script /tmp/redis_start_<host>_<sg>.sh (written by
#                               tasks/cluster/start_redisraft_instances.yml; the raft log on
#                               disk is kept, so it rejoins its group)
#   stop HOST SG SECS           SIGSTOP the instance, SIGCONT it after SECS (slow /
#                               unresponsive node; returns at once)
#   partition HOSTA HOSTB SECS  drop all IP traffic between the two hosts for SECS (returns
#                               at once). The NICs are RoCE: queue pairs that already exist
#                               keep working, only TCP (commands, queries, new connections) is cut
#   cmd HOST SG ARGS...         run a Redis command on that instance (e.g. CONFIG SET
#                               raft.election-timeout 2000 to keep a node from being elected)
#   waitsync HOSTA SGA HOSTB SGB SECS
#                               wait (at most SECS) until both instances hold the same number
#                               of keys (read locally, RAFT.DEBUG EXEC INFO keyspace): a
#                               restarted replica has caught up
#   sleep SECS
#
# Log lines match crash_inject.sh ("MARKER HIT", "KILLED <host> pid=.. t_kill=..") so
# verdict.sh and the recovery tools read both. Env: STEPS, LABEL, RESULT_DIR, MAX_WAIT.
# Exit: 0 all steps done, 2 a marker never seen, 3 a target not alive.
# ---------------------------------------------------------------------------
set -uo pipefail
: "${STEPS:?set STEPS}"
LABEL="${LABEL:-steps}"
MAX_WAIT="${MAX_WAIT:-1800}"
RESULT_DIR="${RESULT_DIR:-/tmp/crash_inject}"
SSH="sudo ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"
mkdir -p "$RESULT_DIR"
RES="$RESULT_DIR/${LABEL}.txt"
AT_HOST=""; AT_SG=""
FIRST_ARM=1

log() { echo "[crash_inject $(date -u +%H:%M:%S)] $*" | tee -a "$RES" >&2; }
logf() { echo "/tmp/redis_logs/${1}_${2}.log"; }
pidf() { echo "/tmp/redis_${1}_${2}.pid"; }
ip_of() { echo "10.10.1.$(( ${1#redis} + 1 ))"; }
resolve() {  # "@" -> the host/sg the last armany hit
  if [ "$1" = "@" ]; then H="$AT_HOST"; G="$AT_SG"; else H="$1"; G="$2"; fi
}

# Watch one or more HOST:SG logs for REGEX (one ssh poller each). watch_start returns once
# every poller has taken its baseline count, so a step started after it cannot be missed:
# the next arm's watch is started BEFORE the step that triggers it (e.g. the kill whose
# election it waits for), see the main loop. watch_wait sets HIT to the first HOST:SG to
# reach N new matches (empty: none within MAX_WAIT).
WATCH_DIR=""; WATCH_PIDS=()
watch_start() {
  local re="$1" n="$2"; shift 2
  WATCH_DIR=$(mktemp -d); WATCH_PIDS=()
  for hs in "$@"; do
    local h=${hs%%:*} g=${hs##*:} lf; lf=$(logf "$h" "$g")
    local P='log="__LOG__"; n=__N__; re="__RE__";'
    if [ "$FIRST_ARM" = 1 ]; then
      P+=' if [ -e "$log" ]; then while [ -e "$log" ]; do sleep 0.3; done; fi; while [ ! -e "$log" ]; do sleep 0.3; done; base=0;'
    else
      P+=' base=$(grep -acE "$re" "$log" 2>/dev/null); base=${base:-0};'
    fi
    P+=' echo READY; while :; do c=$(grep -acE "$re" "$log" 2>/dev/null); c=${c:-0}; [ $((c - base)) -ge "$n" ] && { echo HIT; exit 0; }; sleep 0.2; done'
    P="${P//__LOG__/$lf}"; P="${P//__N__/$n}"; P="${P//__RE__/$re}"
    timeout "$MAX_WAIT" $SSH "$h" "$P" > "$WATCH_DIR/out.$h:$g" 2>/dev/null &
    WATCH_PIDS+=($!)
  done
  [ "$FIRST_ARM" = 1 ] && return    # the fresh-log gate says READY only once the run's log exists
  local t0=$SECONDS
  while [ $((SECONDS - t0)) -lt 20 ]; do
    local ready=0; for f in "$WATCH_DIR"/out.*; do grep -qs READY "$f" && ready=$((ready + 1)); done
    [ $ready -ge $# ] && return
    sleep 0.05
  done
  log "WARNING: not every watcher of /$re/ was ready after 20 s"
}
watch_wait() {
  HIT=""
  while :; do
    local f; f=$(grep -l HIT "$WATCH_DIR"/out.* 2>/dev/null | head -1)
    if [ -n "$f" ]; then HIT=${f##*/out.}; break; fi
    local alive=0; for p in "${WATCH_PIDS[@]}"; do kill -0 "$p" 2>/dev/null && alive=1; done
    [ $alive = 0 ] && break
    sleep 0.05
  done
  for p in "${WATCH_PIDS[@]}"; do kill "$p" 2>/dev/null; done
  rm -rf "$WATCH_DIR"; WATCH_DIR=""; WATCH_PIDS=()
}
# arm/armany arguments -> "REGEX N HOST:SG ..." for watch_start
watch_args() {
  local op=$1; shift
  if [ "$op" = arm ]; then WA=("$3" "${4:-1}" "$1:$2"); else WA=("$@"); fi
}

do_kill() {
  resolve "$@"
  local pf; pf=$(pidf "$H" "$G")
  local pid; pid=$($SSH "$H" "p=\$(cat $pf 2>/dev/null); kill -0 \$p 2>/dev/null && echo \$p" | tr -d '[:space:]')
  if [ -z "$pid" ]; then log "ERROR: $H $G ($pf) not alive -- NO KILL"; echo "result=STALE_PID label=$LABEL target=$H" >> "$RES"; exit 3; fi
  $SSH "$H" "kill -9 $pid"
  local t; t=$(date -u +%s.%N)
  log "KILLED $H pid=$pid ($G) (verified alive)  t_kill=$t"
  echo "result=KILLED label=$LABEL target=$H pid=$pid sg=$G t_kill=$t" >> "$RES"
  printf -v "KILLED_PID_${H}_${G}" '%s' "$pid"
}

do_restart() {
  resolve "$@"
  local v="KILLED_PID_${H}_${G}"; local pid="${!v:-0}"
  # A process with ~15 GB of RDMA-pinned memory takes ~1.2 s to exit, holding its port.
  $SSH "$H" "for i in \$(seq 1 300); do kill -0 $pid 2>/dev/null || break; sleep 0.1; done
             ulimit -n 65535; bash /tmp/redis_start_${H}_${G}.sh" \
    || { log "ERROR: restart of $H $G failed"; return 1; }
  local np; np=$($SSH "$H" "sleep 0.5; cat $(pidf "$H" "$G")" | tr -d '[:space:]')
  log "RESTARTED $H $G pid=$np  t_restart=$(date -u +%s.%N)"
}

do_stop() {
  local h="$1" g="$2" secs="$3"
  local pid; pid=$($SSH "$h" "cat $(pidf "$h" "$g")" | tr -d '[:space:]')
  $SSH "$h" "kill -STOP $pid; nohup setsid bash -c 'sleep $secs; kill -CONT $pid' >/dev/null 2>&1 &"
  log "STOPPED $h pid=$pid ($g) for ${secs}s  t_stop=$(date -u +%s.%N)"
  ( sleep "$secs"; log "CONTINUED $h pid=$pid ($g)  t_cont=$(date -u +%s.%N)" ) &
}

do_partition() {
  local a="$1" b="$2" secs="$3" ib; ib=$(ip_of "$b")
  local add="iptables -I INPUT -s $ib -j DROP; iptables -I OUTPUT -d $ib -j DROP"
  local del="iptables -D INPUT -s $ib -j DROP; iptables -D OUTPUT -d $ib -j DROP"
  # the heal runs on the host itself, so it happens even if this script is killed
  $SSH "$a" "$add; nohup setsid bash -c 'sleep $secs; $del' >/dev/null 2>&1 &"
  log "PARTITIONED $a <-> $b (TCP/IP to $ib, RDMA queue pairs unaffected) for ${secs}s  t_part=$(date -u +%s.%N)"
  ( sleep "$secs"; log "HEALED $a <-> $b  t_heal=$(date -u +%s.%N)" ) &
}

port_of() { echo $(( 8000 + ( ${1#sg} - 1 ) % 3 )); }   # sg1..3 / sg4..6 -> 8000..8002
do_cmd() {
  local h="$1" g="$2"; shift 2
  local out; out=$($SSH "$h" "/users/entall/rd/redis_bin/bin/redis-cli -p $(port_of "$g") $*" 2>&1 | tr '\n' ' ')
  log "CMD $h $g $* -> $out"
}
do_waitsync() {
  local ha="$1" ga="$2" hb="$3" gb="$4" secs="$5" t0=$SECONDS a b
  while :; do
    a=$($SSH "$ha" "/users/entall/rd/redis_bin/bin/redis-cli -p $(port_of "$ga") RAFT.DEBUG EXEC INFO keyspace" 2>/dev/null | grep -o "keys=[0-9]*" | head -1)
    b=$($SSH "$hb" "/users/entall/rd/redis_bin/bin/redis-cli -p $(port_of "$gb") RAFT.DEBUG EXEC INFO keyspace" 2>/dev/null | grep -o "keys=[0-9]*" | head -1)
    a=${a:-keys=0}; b=${b:-keys=0}   # an empty keyspace has no keys= line
    if [ "$a" = "$b" ]; then log "IN SYNC $ha $ga = $hb $gb ($a keys) after $((SECONDS - t0))s  t_sync=$(date -u +%s.%N)"; return; fi
    if [ $((SECONDS - t0)) -ge "$secs" ]; then log "NOT IN SYNC after ${secs}s: $ha $ga $a keys, $hb $gb $b keys"; return; fi
    sleep 0.5
  done
}

log "==== scenario=$LABEL (steps) ===="
log "STEPS: $STEPS"
IFS=';' read -ra ST <<< "$STEPS"
PRE=""    # index of the step whose watch is already running
for ((i = 0; i < ${#ST[@]}; i++)); do
  eval "set -- ${ST[i]}"
  [ $# -eq 0 ] && continue
  op=$1; shift
  # the next step waits for a log line this one may cause: start watching first
  if [[ "$op" != arm* ]] && [ $((i + 1)) -lt ${#ST[@]} ]; then
    eval "nx=(${ST[i+1]})"
    if [[ "${nx[0]}" == arm* ]]; then watch_args "${nx[@]}"; watch_start "${WA[@]}"; PRE=$((i + 1)); fi
  fi
  case "$op" in
    arm|armany)
             watch_args "$op" "$@"
             [ "$PRE" = "$i" ] || watch_start "${WA[@]}"
             PRE=""
             watch_wait
             [ -z "$HIT" ] && { log "ERROR: /${WA[0]}/ not seen on ${WA[*]:2} within ${MAX_WAIT}s -- stopping"; echo "result=NO_MARKER label=$LABEL" >> "$RES"; exit 2; }
             AT_HOST=${HIT%%:*}; AT_SG=${HIT##*:}
             FIRST_ARM=0
             log "MARKER HIT $AT_HOST $AT_SG /${WA[0]}/ x${WA[1]}  t_arm=$(date -u +%s.%N)" ;;
    kill)    do_kill "$@" ;;
    restart) do_restart "$@" ;;
    stop)    do_stop "$@" ;;
    partition) do_partition "$@" ;;
    waitsync) do_waitsync "$@" ;;
    cmd)     do_cmd "$@" ;;
    sleep)   sleep "$1" ;;
    *)       log "ERROR: unknown step '$op'"; exit 1 ;;
  esac
done
wait
log "done."
