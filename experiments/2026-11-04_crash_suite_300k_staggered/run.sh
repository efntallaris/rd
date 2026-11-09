#!/usr/bin/env bash
# S1-S9 of the 3 -> 6 crash suite with the migration phases STAGGERED.
#
# CHUNK_SLOTS (= rdma-transfer-chunk-slots) is the slots per announced chunk, and the recipient
# leader cannot start forwarding a round until its FIRST chunk has landed and been announced.
# So the chunk size sets the lag between the copy and the replication / index update:
#
#   K=12   (57 chunks/round)  first chunk is ~2% of the round -> ~10 ms lag, phases look flush
#   K=171  (4 chunks/round)   first chunk is ~25% of the round -> ~128 ms lag  <-- this suite
#   K=342  (2 chunks/round)   first chunk is ~50% of the round -> ~253 ms lag
#   K>=683 (1 chunk/round)    nothing is announced until the copy ends -> fully serialized
#
# With K=171 a round is 4 chunks, so S1/S6's 4th DONE-SLOTS-CHUNK falls at the end of round 1
# and the kill lands in round 2 -- the kill point scenarios.env documents for S1. CHUNK_ARM_COUNT
# therefore stays at its default of 4.
#
# Run ON THE CONTROLLER (redis0):   ./run.sh [scenario ...]
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)

SUITE=(HEALTHY S1 S2 S3 S4 S5 S6 S7 S8 S9)
[ $# -eq 0 ] && set -- "${SUITE[@]}"

export ROUNDS="${ROUNDS:-4}"
export SCALEOUT_MODE="${SCALEOUT_MODE:-pairwise}"
export CHUNK_SLOTS="${CHUNK_SLOTS:-171}"       # 4 chunks per 683-slot round; first chunk ~25%
export ELECTION_TIMEOUT_MS="${ELECTION_TIMEOUT_MS:-500}"
export PROFILE="${PROFILE:-quick}"
export WORKLOAD="${WORKLOAD:-workloada_lin_quick_uniform}"
export YCSB_THREADS="${YCSB_THREADS:-400}"
export OUT_ROOT="${OUT_ROOT:-/tmp/lincheck/staggered_$(date -u +%Y%m%d)}"
export SCALEOUT_EXTRA_ARGS="${SCALEOUT_EXTRA_ARGS:-} -e raft_election_timeout=$ELECTION_TIMEOUT_MS"

echo "OUT_ROOT=$OUT_ROOT  ROUNDS=$ROUNDS MODE=$SCALEOUT_MODE CHUNK_SLOTS=$CHUNK_SLOTS (staggered)"
echo "scenarios: $*"

"$here/../../ansible/experiments/custom_scaleout_3to6/run_all.sh" "$@"

for s in "$@"; do
  exp=scaleout3to6_crash_${s,,}; [ "$s" = HEALTHY ] && exp=scaleout3to6_lin_healthy
  lin=$(ls -1dt "$OUT_ROOT"/${s}_q*_* 2>/dev/null | head -1)
  sudo rm -rf "$here/data/$s"; sudo mkdir -p "$here/data/$s"
  [ -d "/tmp/experiments/$exp" ] && sudo cp -r "/tmp/experiments/$exp" "$here/data/$s/exp"
  [ -n "$lin" ] && sudo cp -r "$lin" "$here/data/$s/lincheck"
done

# MERGE this run's rows into data/summary.txt, one row per scenario, keeping the
# suite order. A plain copy (what this was until 2026-11-05) replaces the whole
# file, so re-running ONE scenario left the suite table with a single row and
# suite_table.py silently reported a 1-scenario suite.
sudo python3 - "$OUT_ROOT/summary.txt" "$here/data/summary.txt" <<'PY'
import io, os, sys
fresh, dest = sys.argv[1], sys.argv[2]
order = "HEALTHY S1 S2 S3 S4 S5 S6 S7 S8 S9".split()
def scen(l):
    f = l.split()
    return f[1] if len(f) > 1 else ""
rows = {}
for src in (dest, fresh):                      # fresh wins over what is on disk
    if os.path.exists(src):
        for l in io.open(src, errors="ignore"):
            if scen(l):
                rows[scen(l)] = l.rstrip("\n") + "\n"
keys = [s for s in order if s in rows] + sorted(k for k in rows if k not in order)
io.open(dest, "w").writelines(rows[k] for k in keys)
print("summary.txt: %d scenario rows (%s)" % (len(keys), " ".join(keys)))
PY
tail -n $# "$OUT_ROOT/summary.txt"
"$here/figures.sh" "$@"
