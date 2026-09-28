#!/usr/bin/env bash
# Rebuild figures/ from every run under logs/.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
for run in "$here"/logs/*/; do
  [ -d "$run" ] && "$here/../tools/make_figures.sh" "$run" "$here/figures"
done
