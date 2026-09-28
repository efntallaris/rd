#!/usr/bin/env bash
# fetch_logs.sh — copy raw run output from the cluster controller into an
# experiment folder, so the logs live next to the figures they produce.
#
# Usage (from the repo root):
#   CONTROLLER=<ssh-target> experiments/tools/fetch_logs.sh <experiment-id> <run> [<run> ...]
#
#   <experiment-id>  folder under experiments/, e.g. 2026-07_crash_campaign
#   <run>            a run name under /tmp/experiments on the controller
#                    (the ansible experiment_name, e.g. crash_s1, bgm_30m_4ck),
#                    or an absolute remote path (e.g. /tmp/crash_inject/campaign)
#
# Each run lands in experiments/<experiment-id>/logs/<basename of run>/ with the
# same layout the controller has (ycsb/, logs/<host>/...), which is what the
# plotting tools expect. Remote files are root-owned, so rsync runs under sudo
# on the controller.
#
# Example:
#   CONTROLLER=entall@redis0.example.net experiments/tools/fetch_logs.sh \
#       2026-07_crash_campaign crash_s1 crash_s2 /tmp/crash_inject/campaign
set -euo pipefail

if [ -z "${CONTROLLER:-}" ] || [ $# -lt 2 ]; then
  sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
fi

root=$(cd "$(dirname "$0")/../.." && pwd)
exp="$1"; shift
dest_base="$root/experiments/$exp/logs"
[ -d "$root/experiments/$exp" ] || { echo "no such experiment folder: experiments/$exp" >&2; exit 1; }
mkdir -p "$dest_base"

for run in "$@"; do
  case "$run" in
    /*) src="$run" ;;
    *)  src="/tmp/experiments/$run" ;;
  esac
  name=$(basename "$src")
  echo "==> $CONTROLLER:$src  ->  experiments/$exp/logs/$name/"
  rsync -az --rsync-path="sudo rsync" \
        "$CONTROLLER:$src/" "$dest_base/$name/"
done

du -sh "$dest_base"/* 2>/dev/null || true
