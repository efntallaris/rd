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
# ---------------------------------------------------------------------------
set -euo pipefail

SCENARIO="${1:-}"
if [[ ! "$SCENARIO" =~ ^S[1-4]$ ]]; then
  echo "usage: $0 <S1|S2|S3|S4>" >&2; exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ANSIBLE_DIR="$REPO_ROOT/ansible"
RESULT_DIR="/tmp/crash_inject"
EXP_NAME="crash_${SCENARIO,,}"
[ "${RDMA_NAIVE:-no}" = "yes" ] && EXP_NAME="${EXP_NAME}_naive"  # paper: naive durability baseline arm

# shellcheck source=/dev/null
source "$SCRIPT_DIR/scenarios.env"

echo "=========================================================="
echo " AqRaft crash scenario $SCENARIO  ($LABEL)"
echo " kill $TARGET_HOST:$PIDFILE  @  ${ARM_COUNT}x /$ARM_MARKER/"
echo " experiment_name=$EXP_NAME"
echo "=========================================================="

mkdir -p "$RESULT_DIR"

# 1. Arm the killer in the background (blocks on the marker, then kills).
LABEL="$LABEL" ARM_HOST="$ARM_HOST" ARM_LOG="$ARM_LOG" \
  ARM_MARKER="$ARM_MARKER" ARM_COUNT="$ARM_COUNT" \
  TARGET_HOST="$TARGET_HOST" PIDFILE="$PIDFILE" \
  RESTART="${RESTART:-no}" POST_KILL_DELAY="${POST_KILL_DELAY:-0}" \
  MAX_WAIT="${MAX_WAIT:-1800}" RESULT_DIR="$RESULT_DIR" \
  bash "$SCRIPT_DIR/crash_inject.sh" &
INJ_PID=$!
echo "[run] crash_inject armed (pid=$INJ_PID); starting base reshard..."

# 2. Run the base 30M background-merge reshard (HOWTO_30M_BGMERGE §1).
#    Only experiment_name changes vs the validated command.
cd "$ANSIBLE_DIR"
set +e
sudo ansible-playbook -i inventory.ini \
  experiments/custom_reshard_v2_orch_raft_chunked/workload_nround.yml \
  -e redis_variant=custom -e pre_reshard_pause=20 -e n_rounds=1 \
  -e rdma_migration_peer_stagger_ms=0 \
  -e ycsb_slotpoll_ms=100 -e rdma_chain_pipeline=yes -e rdma_chain_xsession=yes \
  -e rdma_async_apply=yes -e rdma_transfer_chunk_slots=342 \
  -e rdma_naive_durability="${RDMA_NAIVE:-no}" \
  -e '{"rdma_follower_proxy": "no"}' \
  -e redis_workload=workloada_debug \
  -e experiment_name="$EXP_NAME"
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
for h in redis3 redis4 redis5; do  # redis3 added: base collection left a STALE leader log (false PASS 2026-07-08)
  d="/tmp/experiments/$EXP_NAME/logs/$h/tmp/redis_logs"
  sudo mkdir -p "$d" 2>/dev/null || true
  sudo ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=8 "$h" \
    "cat /tmp/redis_logs/${h}_sg4.log 2>/dev/null" 2>/dev/null \
    | sudo tee "$d/${h}_sg4.log" >/dev/null 2>&1 || true
done

# 3. Verdict.
echo "[run] collecting verdict..."
bash "$SCRIPT_DIR/verdict.sh" "$SCENARIO" "$EXP_NAME" || true

echo "[run] injection record: $RESULT_DIR/${LABEL}.txt"
echo "[run] done ($SCENARIO)."
