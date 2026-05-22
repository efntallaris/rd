#!/bin/bash
# Sample CLUSTER SLOTSTATE from each redis node every N ms.
# Output format (one line per non-stable slot per sample):
#   <unix_ms> <host> <slot> <state> <peer_or_empty>
# Slots not present in a sample (= STABLE) are not logged.
# Exit on SIGTERM. PID file at /tmp/slot_poller.pid.

OUT="${1:-/tmp/slot_ownership.log}"
HOSTS="${2:-redis0 redis1 redis2 redis3}"
INTERVAL_MS="${3:-50}"
SLOT_LO="${4:-0}"
SLOT_HI="${5:-127}"
REDIS_CLI="${REDIS_CLI:-/users/entall/rd/redis_bin/bin/redis-cli}"

: > "$OUT"
echo $$ > /tmp/slot_poller.pid
trap "exit 0" SIGTERM SIGINT

# Parse redis-cli CLUSTER SLOTSTATE — a nested array. Each element is
# itself [slot, state, peer]. redis-cli renders this as:
#   1) 1) (integer) 0
#      2) (integer) 1
#      3) "redis3:8000"
#   2) 1) (integer) 5
#      ...
# We pipe it through python for parsing instead of awk.
parse_slotstate() {
  python3 - "$1" "$2" "$3" "$4" <<'PY'
import re, sys
ts, host, lo, hi = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
state_name = {0: 'STABLE', 1: 'MIGRATING', 2: 'MIGRATED'}
lines = sys.stdin.read().splitlines()
# Walk through 3-tuples. Each "outer" element starts with N) followed
# by 1) <slot>, 2) <state>, 3) <peer>.
tup = []
for line in lines:
    s = line.strip()
    # match "  1) (integer) 0" or "  3) \"redis3:8000\""
    m = re.match(r'^[0-9]+\)\s+(?:\(integer\)\s+(-?\d+)|"([^"]*)")\s*$', s)
    if m:
        v = m.group(1) if m.group(1) is not None else m.group(2)
        # The OUTER index "N)" precedes a nested "1) ..." line in same line:
        # we don't need to distinguish — every interior triple is in order.
        tup.append(v)
        if len(tup) == 3:
            try:
                slot = int(tup[0])
                st   = int(tup[1])
                peer = tup[2] if not tup[2].lstrip('-').isdigit() else ''
            except ValueError:
                tup = []
                continue
            if lo <= slot <= hi:
                print(f"{ts} {host} {slot} {state_name.get(st,'?')} {peer or '-'}")
            tup = []
PY
}

SLEEP_SEC=$(awk -v ms="$INTERVAL_MS" 'BEGIN{printf "%.3f", ms/1000.0}')

while true; do
  NOW_MS=$(date +%s%3N)
  for h in $HOSTS; do
    OUT_TXT=$(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=2 \
      "$h" "$REDIS_CLI -p 8000 CLUSTER SLOTSTATE" 2>/dev/null) || OUT_TXT=""
    if [ -n "$OUT_TXT" ]; then
      echo "$OUT_TXT" | parse_slotstate "$NOW_MS" "$h" "$SLOT_LO" "$SLOT_HI" >> "$OUT"
    fi
  done
  sleep "$SLEEP_SEC"
done
