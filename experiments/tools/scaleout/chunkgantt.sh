#!/bin/bash
# chunkgantt.sh <lincheck run dir> <out.png>: chunk-style phase Gantt (plot_phase_gantt) for a 3 -> 4 run.
# That script reads one fixed leader log per group; after a failover the leader is another host, so
# feed it each group's logs from all hosts merged in time order. Its recipient lanes are matched to
# rounds by order: trust them only for runs without a leader change (HEALTHY, S3, S4).
d=$1; out=$2
t=$(mktemp -d)
for p in sg1:redis0 sg2:redis1 sg3:redis2 sg4:redis3; do sg=${p%%:*}; h=${p##*:}
  mkdir -p $t/logs/$h/tmp/redis_logs
  cat $d/logs/redis*/redis*_$sg.log | grep -E '^[0-9]+:[A-Z] [0-9]{2} [A-Za-z]{3} [0-9]{4} ' | sort -s -k5,5 > $t/logs/$h/tmp/redis_logs/${h}_$sg.log
done
python3 /users/entall/rd/experiments/2026-10-01_protocol_fixes_quick/tools/plot_phase_gantt.patched.py $t $out 2>&1 | tail -2
rm -rf $t
