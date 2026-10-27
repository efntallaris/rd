#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_crash_scenario.sh <S1|S2|S3|S4>
#
# One-command driver for an AqRaft crash scenario:
#   1. background crash_inject.sh (armed off a recipient-log marker),
#   2. run the validated 30M background-merge reshard (HOWTO_30M_BGMERGE §1)
#      with experiment_name=crash_<scenario>,
#   3. run verdict.sh to print PASS/FAIL.
#
# Runs the base reshard exactly as the HOWTO; the only added variable is the
# backgrounded kill. Privileged ops use sudo (passwordless here). The kill
# fires mid-migration off the Nth DONE-SLOTS-CHUNK / forward marker.
#
# Env passthrough: RESTART, POST_KILL_DELAY, MAX_WAIT (see crash_inject.sh).
# A scenario that sets STEPS (C1-C16) runs fault_steps.sh instead of crash_inject.sh.
# HOOKS="host:name[:ms] ..." (from the scenario) creates /tmp/rdma_hook_<name> on each host
# before the run (content: pause in ms): a one-shot pause in the server code that makes a
# window of a few ms wide enough to crash in (see rdmaTestHook in redis/src/cluster_rdma.c).
# The files are removed when the run ends. SCEN_EXTRA_ARGS: extra ansible args of the scenario.
# Other topologies (e.g. experiments/custom_scaleout_3to6): SCENARIOS_ENV is the
# scenario file to source, WORKLOAD_PLAYBOOK the playbook to run (path relative
# to ansible/), RECIPIENT_LOGS the "host:sg" logs to fetch for the verdict
# ("none": the playbook collects them itself), EXP_PREFIX a prefix for
# experiment_name.
# ---------------------------------------------------------------------------
set -euo pipefail

SCENARIO="${1:-}"
if [[ ! "$SCENARIO" =~ ^(S[1-9]|C[1-9][0-9]?)$ ]]; then
  echo "usage: $0 <S1..S9|C1..C16>" >&2; exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ANSIBLE_DIR="$REPO_ROOT/ansible"
RESULT_DIR="/tmp/crash_inject"
EXP_NAME="${EXP_PREFIX:-}crash_${SCENARIO,,}"
[ "${RDMA_NAIVE:-no}" = "yes" ] && EXP_NAME="${EXP_NAME}_naive"  # paper: naive durability baseline arm

# shellcheck source=/dev/null
STEPS=""; HOOKS=""; SCEN_EXTRA_ARGS=""
source "${SCENARIOS_ENV:-$SCRIPT_DIR/scenarios.env}"
SSHQ="sudo ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=8"
hooks() {  # hooks set|clear
  for hk in $HOOKS; do
    IFS=: read -r h name ms <<< "$hk"
    if [ "$1" = set ]; then $SSHQ "$h" "echo ${ms:-1500} > /tmp/rdma_hook_$name"
    else $SSHQ "$h" "rm -f /tmp/rdma_hook_$name"; fi
  done
}

echo "=========================================================="
echo " AqRaft crash scenario $SCENARIO  ($LABEL)"
if [ -n "$STEPS" ]; then echo " steps: $STEPS"; else echo " kill $TARGET_HOST:$PIDFILE  @  ${ARM_COUNT}x /$ARM_MARKER/"; fi
[ -n "$HOOKS" ] && echo " hooks: $HOOKS"
[ -n "$SCEN_EXTRA_ARGS" ] && echo " extra ansible args: $SCEN_EXTRA_ARGS"
echo " experiment_name=$EXP_NAME"
echo "=========================================================="

mkdir -p "$RESULT_DIR"
hooks clear; hooks set
trap 'hooks clear' EXIT

# 1. Arm the killer in the background (blocks on the marker, then kills).
if [ -n "$STEPS" ]; then
STEPS="$STEPS" LABEL="$LABEL" MAX_WAIT="${MAX_WAIT:-1800}" RESULT_DIR="$RESULT_DIR" \
  bash "$SCRIPT_DIR/fault_steps.sh" &
INJ_PID=$!
else
LABEL="$LABEL" ARM_HOST="$ARM_HOST" ARM_LOG="$ARM_LOG" \
  ARM_MARKER="$ARM_MARKER" ARM_COUNT="$ARM_COUNT" \
  TARGET_HOST="$TARGET_HOST" PIDFILE="$PIDFILE" \
  TARGET_HOST2="${TARGET_HOST2:-}" PIDFILE2="${PIDFILE2:-}" PIDFILE_EXTRA="${PIDFILE_EXTRA:-}" \
  RESTART="${RESTART:-no}" POST_KILL_DELAY="${POST_KILL_DELAY:-0}" \
  MAX_WAIT="${MAX_WAIT:-1800}" RESULT_DIR="$RESULT_DIR" \
  bash "$SCRIPT_DIR/crash_inject.sh" &
INJ_PID=$!
fi
echo "[run] crash_inject armed (pid=$INJ_PID); starting base reshard..."

# 2. Run the base 30M background-merge reshard (HOWTO_30M_BGMERGE §1).
#    Only experiment_name changes vs the validated command.
cd "$ANSIBLE_DIR"
set +e
sudo ansible-playbook -i "${INVENTORY:-inventory.ini}" \
  "${WORKLOAD_PLAYBOOK:-experiments/custom_reshard_v2_orch_raft_chunked/workload_nround.yml}" \
  -e redis_variant=custom -e n_rounds=1 \
  -e rdma_migration_peer_stagger_ms=0 \
  -e ycsb_slotpoll_ms=100 -e ycsb_threads_run="${YCSB_THREADS:-50}" -e rdma_chain_pipeline=yes -e rdma_chain_xsession=yes \
  -e rdma_async_apply=yes -e rdma_transfer_chunk_slots=342 \
  -e rdma_naive_durability="${RDMA_NAIVE:-no}" \
  -e pre_reshard_pause="${PRE_PAUSE:-20}" \
  -e '{"rdma_follower_proxy": "no"}' \
  -e redis_workload="${WORKLOAD:-workloada_prod_30m_run10min}" \
  -e experiment_name="$EXP_NAME" ${EXTRA_ANSIBLE_ARGS:-} $SCEN_EXTRA_ARGS
PLAYBOOK_RC=$?
set -e
echo "[run] playbook exited rc=$PLAYBOOK_RC"

# Reap the injector if it is still waiting (marker never came, etc.).
if kill -0 "$INJ_PID" 2>/dev/null; then
  echo "[run] injector still running — waiting up to 30s"
  ( sleep 30; kill "$INJ_PID" 2>/dev/null ) &
  wait "$INJ_PID" 2>/dev/null || true
fi

# 2b. The base collect_results skips the sg4 followers redis4/redis5 — grab
#     their logs into the experiment dir (dead node's log is frozen; the
#     survivor's is live) so the verdict + posterity have them.
for hs in ${RECIPIENT_LOGS:-redis3:sg4 redis4:sg4 redis5:sg4}; do  # redis3 added: base collection left a STALE leader log (false PASS 2026-07-08)
  [ "$hs" = none ] && continue
  h=${hs%%:*}; g=${hs##*:}
  d="/tmp/experiments/$EXP_NAME/logs/$h/tmp/redis_logs"
  sudo mkdir -p "$d" 2>/dev/null || true
  sudo ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=8 "$h" \
    "cat /tmp/redis_logs/${h}_${g}.log 2>/dev/null" 2>/dev/null \
    | sudo tee "$d/${h}_${g}.log" >/dev/null 2>&1 || true
done

# 3. Verdict.
echo "[run] collecting verdict..."
bash "$SCRIPT_DIR/verdict.sh" "$SCENARIO" "$EXP_NAME" || true

echo "[run] injection record: $RESULT_DIR/${LABEL}.txt"
echo "[run] done ($SCENARIO)."
