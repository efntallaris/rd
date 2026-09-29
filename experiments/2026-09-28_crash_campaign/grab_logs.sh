#!/usr/bin/env bash
# grab_logs.sh <S...> — side job for the campaign. While each scenario runs, copy
# /tmp/redis_logs from every redis host into /tmp/crash_inject/campaign/<S>/full/<host>/ every
# 15 s, keeping the largest copy of each file. The recipient leader's log is wiped at the end of
# a run (and a failed playbook skips the base collection), so this is the only complete copy.
set -uo pipefail
SSH="ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=8"
grab(){ # <S>
  for h in redis0 redis1 redis2 redis3 redis4 redis5; do
    d=/tmp/crash_inject/campaign/$1/full/$h; t=$(mktemp -d); mkdir -p "$d"
    $SSH $h 'tar -C /tmp/redis_logs -cf - . 2>/dev/null' | tar -C "$t" -xf - 2>/dev/null
    for f in "$t"/*.log; do
      [ -s "$f" ] || continue
      b=$(basename "$f"); old=$(stat -c %s "$d/$b" 2>/dev/null || echo 0)
      [ "$(stat -c %s "$f")" -gt "$old" ] && cp "$f" "$d/$b"
    done
    rm -rf "$t"
  done
}
for S in "$@"; do
  f=/tmp/crash_inject/campaign/${S}_run.log
  echo "[grab $(date -u +%H:%M:%S)] waiting for $S to start"
  until [ -f "$f" ]; do sleep 5; done
  sleep 120   # let the playbook restart the cluster so the previous run's logs are gone
  until grep -q 'playbook exited' "$f"; do grab "$S"; sleep 15; done
  echo "[grab $(date -u +%H:%M:%S)] $S: $(find /tmp/crash_inject/campaign/$S/full -name '*.log' -size +0 | wc -l) non-empty logs"
done
