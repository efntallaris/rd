#!/usr/bin/env bash
# S6 (recipient host) and S7 (donor host) are still defined in scenarios.env but not
# in the default list since 2026-10-04 (user: may come back later); run them by name.
# run_all.sh [scenario ...]   (default: HEALTHY S1 S2 S3 S4 S5 S9)   S5 = both leaders of pair A (S8 until 2026-10-05)
# The scenarios in a row (ansible/lincheck/run_all_lin.sh); one line per run in
# $OUT_ROOT/summary.txt (default /tmp/lincheck/scaleout3to6).
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/env.sh"
export OUT_ROOT="${OUT_ROOT:-/tmp/lincheck/scaleout3to6}"
[ $# -eq 0 ] && set -- HEALTHY S1 S2 S3 S4 S5 S9
exec "$HERE/../../lincheck/run_all_lin.sh" "$@"
