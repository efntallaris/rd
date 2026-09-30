#!/usr/bin/env bash
# install_run.sh <S> <run> — copy a driver run dir (/tmp/crash_inject/final/runs/<S>) into
# logs/final_<run>, filling redis logs the playbook did not collect from the grabber copies.
set -u
here=$(cd "$(dirname "$0")" && pwd); S=$1; run=$2
src=/tmp/crash_inject/final/runs/$S; dst=$here/logs/final_$run
[ -d "$dst" ] && { rm -rf "${dst}_prev"; mv "$dst" "${dst}_prev"; }
mkdir -p "$dst"; cp -a "$src"/exp/. "$dst"/; cp -a "$src"/run.log "$src"/*.txt "$dst"/ 2>/dev/null
for hd in "$src"/grab/*/; do
  h=$(basename "$hd"); d="$dst/logs/$h/tmp/redis_logs"
  for f in "$hd"*.log; do [ -s "$f" ] || continue; [ -s "$d/$(basename "$f")" ] && continue
    mkdir -p "$d"; cp "$f" "$d/"; echo "filled $h/$(basename "$f")"; done
done
chown -R entall "$dst"
