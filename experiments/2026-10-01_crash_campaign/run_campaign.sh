#!/usr/bin/env bash
# Crash campaign on the post-lincheck build: HEALTHY + S1..S5 at 30M, for client
# performance and per-phase timing. Run ON THE CONTROLLER (redis0) as root:
#   sudo setsid nohup experiments/2026-10-01_crash_campaign/run_campaign.sh >/dev/null 2>&1 &
# Progress: $O/driver.log. Per run: $O/runs/<S>/{run.log, grab/<host>/*.log, exp/}.
#   ORDER="S1 S2"  WORKLOAD=...  YCSB_THREADS=...  PRE_PAUSE=...  QUORUM_READS=no|yes
#   EXTRA_MORE="-e var=value ..."   (extra ansible vars, e.g. rdma_warm_reg_batch / rdma_warm_reg_pause_us)
set -uo pipefail
REPO=/users/entall/rd
O=${O:-/tmp/crash_inject/final_1001}
ORDER=${ORDER:-"HEALTHY S1 S2 S3 S4 S5"}
WORKLOAD=${WORKLOAD:-workloada_prod_30m_run3min}
YCSB_THREADS=${YCSB_THREADS:-100}
PRE_PAUSE=${PRE_PAUSE:-60}
QUORUM_READS=${QUORUM_READS:-no}
EXTRA="-e dataset_snapshot=ws30m -e rdma_src_prereg_slots=1365 -e rdma_landing_prereg_pools=3 -e rdma_chain_ack_via_raft=yes -e raft_quorum_reads=$QUORUM_READS ${EXTRA_MORE:-}"
SSH="ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=8"
LOCK=/tmp/cluster_in_use
mkdir -p "$O/runs"
say(){ echo "[final $(date -u +%H:%M:%S)] $*" >> "$O/driver.log"; }

# Node logs: collect_results deletes /tmp/redis_logs on the hosts and
# run_crash_scenario then blanks the sg4 leader's copy, so grab every 15 s.
grabber(){ local d=$1; mkdir -p "$d"
  while [ -f "$d/.grab" ]; do
    for h in redis0 redis1 redis2 redis3 redis4 redis5; do
      t=$(mktemp -d); $SSH $h 'tar -C /tmp/redis_logs -cf - . 2>/dev/null' 2>/dev/null | tar -C "$t" -xf - 2>/dev/null
      mkdir -p "$d/$h"
      for f in "$t"/*.log; do [ -s "$f" ] || continue; b=$(basename "$f")
        [ "$(stat -c %s "$f")" -gt "$(stat -c %s "$d/$h/$b" 2>/dev/null || echo 0)" ] && cp "$f" "$d/$h/$b"; done
      rm -rf "$t"
    done; sleep 15
  done; }
start_grab(){ mkdir -p "$O/runs/$1/grab"; touch "$O/runs/$1/grab/.grab"; (grabber "$O/runs/$1/grab") & (ops_sampler "$O/runs/$1") & }
# Per-instance command rate: total_commands_processed of every redis instance,
# read locally (RAFT.DEBUG EXEC, no Raft round trip), once a second -> ops.txt
#   <unix_ts> <host>:<port> <total_commands_processed>
ops_sampler(){ local d=$1; local cli=$REPO/redis/src/redis-cli
  while [ -f "$d/grab/.grab" ]; do
    ts=$(date +%s.%N)
    for hp in redis0:8000 redis0:8001 redis0:8002 redis1:8000 redis1:8001 redis1:8002 \
              redis2:8000 redis2:8001 redis2:8002 redis3:8000 redis4:8000 redis5:8000; do
      v=$(timeout 1 $cli -h ${hp%:*} -p ${hp#*:} RAFT.DEBUG EXEC INFO stats 2>/dev/null | tr -d '\r' | awk -F: '/^total_commands_processed:/{print $2}')
      [ -n "$v" ] && echo "$ts $hp $v"
    done >> "$d/ops.txt"
    # every ~10 s: per-command counts/cost and error counts of the four leaders
    #   "### <unix_ts> <host>:<port>" followed by INFO commandstats + errorstats
    k=$(( ${k:-0} + 1 ))
    if [ $(( k % 10 )) -eq 1 ]; then
      for hp in redis0:8000 redis1:8001 redis2:8002 redis3:8000; do
        echo "### $ts $hp"
        timeout 2 $cli -h ${hp%:*} -p ${hp#*:} RAFT.DEBUG EXEC INFO commandstats 2>/dev/null | tr -d '\r' | grep '^cmdstat_'
        timeout 2 $cli -h ${hp%:*} -p ${hp#*:} RAFT.DEBUG EXEC INFO errorstats 2>/dev/null | tr -d '\r' | grep '^errorstat_'
      done >> "$d/cmdstats.txt"
      # per-THREAD CPU of the four leaders (1 s pidstat sample each, in parallel)
      for hs in redis0:sg1 redis1:sg2 redis2:sg3 redis3:sg4; do
        h=${hs%:*}; g=${hs#*:}
        ( $SSH $h "pidstat -t -p \$(cat /tmp/redis_${h}_${g}.pid 2>/dev/null) 1 1 2>/dev/null | grep -v Average" 2>/dev/null \
            | sed "s/^/$ts $h:$g /" >> "$d/threads.txt" ) &
      done
    fi
    sleep 1
  done; }
stop_grab(){ sleep 20; rm -f "$O/runs/$1/grab/.grab"; }
# A leftover injector or reshard bash loop hits the NEXT run (see HOWTO_LINCHECK.md).
reap(){ for p in $(ps -eo pid=,args= | awk '/crash\/crash_inject\.sh|PS=1365; NR=/ && !/awk/ {print $1}'); do kill "$p" 2>/dev/null; done; }
save_exp(){ local s=$1 e=$2
  rm -rf "$O/runs/$s/exp"; cp -a "/tmp/experiments/$e" "$O/runs/$s/exp" 2>/dev/null
  for c in ycsb0 ycsb1; do f="$O/runs/$s/exp/ycsb/$c/tmp/ycsb_output_$c"; mkdir -p "$(dirname "$f")"
    [ -s "$f" ] || $SSH $c "cat /tmp/ycsb_output_$c" > "$f" 2>/dev/null; done
  say "  $s: $(grep -aoE 'playbook exited rc=[0-9]+|playbook rc=[0-9]+' "$O/runs/$s/run.log" | tail -1) $(grep -aE 'KILLED' "$O/runs/$s/run.log" | head -1 | cut -c1-80)"; }

# wait for a free cluster
while ps -eo args | awk '{for (i = 1; i <= 3 && i <= NF; i++) if ($i ~ /(^|\/)ansible-playbook$|crash_inject\.sh$|camp[0-9a-z]*_driver\.sh$|run_lin_scenario\.sh$/) f = 1} END {exit !f}'; do sleep 30; done
echo "crash campaign final_1001 (session rd-c7) pid=$$ started $(date -u +%FT%TZ)" > $LOCK
trap 'rm -f $LOCK; reap' EXIT
say "start: ORDER=$ORDER WORKLOAD=$WORKLOAD threads=$YCSB_THREADS pre_pause=$PRE_PAUSE quorum_reads=$QUORUM_READS"
say "build: $(git -C $REPO -c safe.directory='*' rev-parse --short HEAD) + working tree; redis-server $(stat -c %y $REPO/redis_bin/bin/redis-server | cut -c1-19)"
for c in ycsb0 ycsb1; do $SSH $c "test -s /users/entall/rd/workloads/$WORKLOAD" || { say "STOP: $WORKLOAD missing on $c"; exit 1; }; done

for S in $ORDER; do
  reap
  mkdir -p "$O/runs/$S"; start_grab "$S"
  for c in ycsb0 ycsb1; do $SSH $c "rm -f /tmp/ycsb_output_*"; done
  say "run $S"
  if [ "$S" = "HEALTHY" ]; then
    rm -rf /tmp/experiments/final_heal
    ( cd $REPO/ansible && ansible-playbook -i inventory.ini experiments/custom_reshard_v2_orch_raft_chunked/workload_nround.yml \
        -e redis_variant=custom -e rdma_chain_pipeline=yes -e rdma_chain_xsession=yes -e rdma_async_apply=yes \
        -e rdma_transfer_chunk_slots=342 -e rdma_naive_durability=no -e '{"rdma_follower_proxy":"no"}' \
        -e redis_workload=$WORKLOAD -e pre_reshard_pause=$PRE_PAUSE -e n_rounds=1 \
        -e rdma_migration_peer_stagger_ms=0 -e ycsb_slotpoll_ms=100 -e ycsb_threads_run=$YCSB_THREADS $EXTRA \
        -e experiment_name=final_heal; echo "playbook rc=$?" ) > "$O/runs/$S/run.log" 2>&1
    stop_grab "$S"; save_exp "$S" final_heal
  else
    ( cd $REPO/ansible/crash && WORKLOAD=$WORKLOAD YCSB_THREADS=$YCSB_THREADS PRE_PAUSE=$PRE_PAUSE \
        EXTRA_ANSIBLE_ARGS="$EXTRA" ./run_crash_scenario.sh $S ) > "$O/runs/$S/run.log" 2>&1
    stop_grab "$S"; save_exp "$S" "crash_${S,,}"
  fi
done
reap
say "CAMPAIGN DONE"
