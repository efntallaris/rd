#!/usr/bin/env bash
# Crash-recovery suite of the 3 -> 6 scale-out, quick profile (300k keys).
# Run ON THE CONTROLLER (redis0), from anywhere:   ./run.sh [scenario ...]
# Default: the 25-run suite (HEALTHY + C1-C17 + S1-S5,S8,S9). ~6 min per run.
# Copies each run's data into data/<S>/{lincheck,exp}, then draws the figures.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)

SUITE=(HEALTHY C1 C2 C3 C4 C5 C6 C7 C8 C9 C10 C11 C12 C13 C14 C15 C16 C17 S1 S2 S3 S4 S5 S8 S9)
[ $# -eq 0 ] && set -- "${SUITE[@]}"

export ROUNDS="${ROUNDS:-4}"
export SCALEOUT_MODE="${SCALEOUT_MODE:-pairwise}"
export CHUNK_SLOTS="${CHUNK_SLOTS:-12}"
export ELECTION_TIMEOUT_MS="${ELECTION_TIMEOUT_MS:-500}"
export PROFILE="${PROFILE:-quick}"
export WORKLOAD="${WORKLOAD:-workloada_lin_quick_uniform}"
export YCSB_THREADS="${YCSB_THREADS:-400}"
export OUT_ROOT="${OUT_ROOT:-/tmp/lincheck/crash_recovery_$(date -u +%Y%m%d)}"
export SCALEOUT_EXTRA_ARGS="${SCALEOUT_EXTRA_ARGS:-} -e raft_election_timeout=$ELECTION_TIMEOUT_MS"

echo "OUT_ROOT=$OUT_ROOT  ROUNDS=$ROUNDS MODE=$SCALEOUT_MODE CHUNK_SLOTS=$CHUNK_SLOTS PROFILE=$PROFILE"
echo "scenarios: $*"

"$here/../../ansible/experiments/custom_scaleout_3to6/run_all.sh" "$@"

# keep each run's data before the next one overwrites /tmp
for s in "$@"; do
  exp=scaleout3to6_crash_${s,,}; [ "$s" = HEALTHY ] && exp=scaleout3to6_lin_healthy
  lin=$(ls -1dt "$OUT_ROOT"/${s}_q*_* 2>/dev/null | head -1)
  sudo rm -rf "$here/data/$s"; sudo mkdir -p "$here/data/$s"
  [ -d "/tmp/experiments/$exp" ] && sudo cp -r "/tmp/experiments/$exp" "$here/data/$s/exp"
  [ -n "$lin" ] && sudo cp -r "$lin" "$here/data/$s/lincheck"
done

# keep the per-run verdicts next to the data (build_suite_page.py reads data/summary.txt)
sudo cp "$OUT_ROOT/summary.txt" "$here/data/summary_$(basename "$OUT_ROOT").txt"
cat "$here"/data/summary_*.txt | sudo tee "$here/data/summary.txt" > /dev/null

tail -n $# "$OUT_ROOT/summary.txt"
"$here/figures.sh" "$@"
