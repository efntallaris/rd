#!/usr/bin/env bash
# run_scenario.sh <HEALTHY|S1..S9>
# One 3 -> 6 scale-out run with the linearizability + fault-tolerance checks
# (ansible/lincheck/run_lin_scenario.sh), on this experiment's topology and
# scenarios (scenarios.env). See README.md.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/env.sh"
exec "$HERE/../../lincheck/run_lin_scenario.sh" "$@"
