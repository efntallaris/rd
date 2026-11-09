#!/usr/bin/env bash
# S1-S9 of the 3 -> 6 crash suite with the migration phases SERIALIZED: one DONE-SLOTS-CHUNK
# announce per round, so the recipient leader starts forwarding (replication) and merging
# (index update) only after the donor's copy of that round has finished.
#
# CHUNK_SLOTS (= rdma-transfer-chunk-slots) is the slots per announced chunk. The default 12
# gives 57 announces per 683-slot round, which is what makes copy / replication / index update
# overlap. A K at or above the round size gives exactly one announce, at the end of the copy.
# (cluster-rdma-transfer-overlap stays on: this is the same code path, not the legacy protocol.)
#
# Because a recipient leader now logs one DONE-SLOTS-CHUNK per ROUND, S1 and S6 arm on the 1st
# instead of the 4th (CHUNK_ARM_COUNT), so the kill still lands in a round's copy rather than
# after the last round.
#
# Run ON THE CONTROLLER (redis0):   ./run.sh [scenario ...]
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)

SUITE=(HEALTHY S1 S2 S3 S4 S5 S6 S7 S8 S9)
[ $# -eq 0 ] && set -- "${SUITE[@]}"

export ROUNDS="${ROUNDS:-4}"
export SCALEOUT_MODE="${SCALEOUT_MODE:-pairwise}"
export CHUNK_SLOTS="${CHUNK_SLOTS:-2730}"      # >= one round => one announce per round
export CHUNK_ARM_COUNT="${CHUNK_ARM_COUNT:-1}" # S1/S6: 1 chunk per round, so arm on the 1st
export ELECTION_TIMEOUT_MS="${ELECTION_TIMEOUT_MS:-500}"
export PROFILE="${PROFILE:-quick}"
export WORKLOAD="${WORKLOAD:-workloada_lin_quick_uniform}"
export YCSB_THREADS="${YCSB_THREADS:-400}"
export OUT_ROOT="${OUT_ROOT:-/tmp/lincheck/serialized_$(date -u +%Y%m%d)}"
export SCALEOUT_EXTRA_ARGS="${SCALEOUT_EXTRA_ARGS:-} -e raft_election_timeout=$ELECTION_TIMEOUT_MS"

echo "OUT_ROOT=$OUT_ROOT  ROUNDS=$ROUNDS MODE=$SCALEOUT_MODE CHUNK_SLOTS=$CHUNK_SLOTS (serialized) CHUNK_ARM_COUNT=$CHUNK_ARM_COUNT"
echo "scenarios: $*"

"$here/../../ansible/experiments/custom_scaleout_3to6/run_all.sh" "$@"

for s in "$@"; do
  exp=scaleout3to6_crash_${s,,}; [ "$s" = HEALTHY ] && exp=scaleout3to6_lin_healthy
  lin=$(ls -1dt "$OUT_ROOT"/${s}_q*_* 2>/dev/null | head -1)
  sudo rm -rf "$here/data/$s"; sudo mkdir -p "$here/data/$s"
  [ -d "/tmp/experiments/$exp" ] && sudo cp -r "/tmp/experiments/$exp" "$here/data/$s/exp"
  [ -n "$lin" ] && sudo cp -r "$lin" "$here/data/$s/lincheck"
done

sudo cp "$OUT_ROOT/summary.txt" "$here/data/summary.txt"
tail -n $# "$OUT_ROOT/summary.txt"
"$here/figures.sh" "$@"
