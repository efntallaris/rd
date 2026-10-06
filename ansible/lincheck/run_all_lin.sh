#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_all_lin.sh [scenario ...]   (default: HEALTHY S1 S2 S3 S4 S5)
#
# Runs run_lin_scenario.sh for each scenario x QUORUM_READS_LIST (default
# "yes") x REPS (default 1), holding /tmp/cluster_in_use, and appends one line
# per run to $OUT_ROOT/summary.txt:
#   <ts> <scenario> q=<yes|no> rc=<lincheck rc> <lincheck summary line> <dir>
# lincheck rc: 0 PASS, 1 FAIL, 3 INCONCLUSIVE (check timed out), other = error
# ---------------------------------------------------------------------------
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_ROOT="${OUT_ROOT:-/tmp/lincheck/runs}"
export OUT_ROOT
LOCK=/tmp/cluster_in_use
SCEN=("$@")
[ ${#SCEN[@]} -eq 0 ] && SCEN=(HEALTHY S1 S2 S3 S4 S5)
mkdir -p "$OUT_ROOT"

if [ -e "$LOCK" ] && ! grep -q "lincheck" "$LOCK"; then
  echo "cluster is in use: $(cat $LOCK)" >&2; exit 1
fi
echo "lincheck campaign (session rd-c7) pid=$$ started $(date -u +%FT%TZ)" > "$LOCK"
trap 'rm -f "$LOCK"' EXIT

for rep in $(seq 1 "${REPS:-1}"); do
  for qr in ${QUORUM_READS_LIST:-yes}; do
    for s in "${SCEN[@]}"; do
      # Don't start on top of another campaign (they don't honor $LOCK).
      # Match on the program tokens only: a shell whose command TEXT mentions
      # ansible-playbook (e.g. the one that launched us) must not count.
      while ps -eo args | awk '{for (i = 1; i <= 3 && i <= NF; i++) if ($i ~ /(^|\/)ansible-playbook$|crash_inject\.sh$|camp[0-9a-z]*_driver\.sh$/) f = 1} END {exit !f}'; do
        sleep 30
      done
      QUORUM_READS=$qr "$SCRIPT_DIR/run_lin_scenario.sh" "$s" > /dev/null 2>&1
      rc=$?
      d=$(ls -1dt "$OUT_ROOT"/${s}_q${qr}_* 2>/dev/null | head -1)
      line=$(grep -E "^linearizable keys" "$d/lincheck.txt" 2>/dev/null)
      pc=$(grep -E "^POSTCHECK (migration|crashes|replicas|donors|bulk)" "$d/postcheck.txt" 2>/dev/null | awk '{printf "%s=%s ", $2, $3}')
      ov=$(grep -oE "OVERALL: [A-Z]+" "$d/run.log" 2>/dev/null | tail -1)
      echo "$(date -u +%FT%TZ) $s q=$qr rc=$rc ${ov:-OVERALL: ?} | ${line:-no-check-output} | ${pc:-no-postcheck} $d" >> "$OUT_ROOT/summary.txt"
    done
  done
done
