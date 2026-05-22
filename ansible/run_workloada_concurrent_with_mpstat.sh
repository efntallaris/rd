#!/bin/bash
# Run concurrent-YCSB workloada and capture mpstat live from redis3 throughout
# the run, so we can see CPU usage step up after each migration.
set -o pipefail
cd /users/entall/rd/ansible
export ANSIBLE_FORCE_COLOR=false

TS=$(date +%Y%m%d_%H%M%S)
LOG="/tmp/workloada_conc_${TS}.log"
MPSTAT="/tmp/redis3_mpstat_${TS}.txt"
PRESERVE="/tmp/preserved_logs_conc_${TS}"

# Full scale
sudo sed -i \
  -e 's/^recordcount=.*/recordcount=10000000/' \
  -e 's/^operationcount=.*/operationcount=40000000/' \
  /rd/workloads/workloada
sudo scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
  /rd/workloads/workloada ycsb0:/users/entall/rd/workloads/workloada >/dev/null 2>&1

# In a sidecar, ssh to redis3 and tail mpstat output every second. Wait until
# redis3 starts serving (after add_migrate_nodes), then poll for mpstat data
# and stream timestamped CPU% to our local file.
(
  while true; do
    sudo ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null redis3 '
      if [ -f /users/entall/systat_logs/redis3_mpstat.txt ]; then
        tail -1 /users/entall/systat_logs/redis3_mpstat.txt
      else echo "(waiting for mpstat to start)"; fi' 2>/dev/null
    sleep 2
  done
) > "$MPSTAT" 2>&1 &
SIDECAR_PID=$!

echo ">>> launching concurrent workloada at full scale"
echo ">>> ansible log -> $LOG"
echo ">>> mpstat live -> $MPSTAT"

sudo ansible-playbook -i inventory.ini \
  experiments/custom_reshard_v2/workload.yml \
  -e "redis_variant=custom" \
  -e "experiment_name=workloada_conc_${TS}" \
  -e "redis_workload=workloada" \
  -e "pre_reshard_pause=10" \
  2>&1 | tee "$LOG"
rc=${PIPESTATUS[0]}

kill $SIDECAR_PID 2>/dev/null

# Always preserve mpstat + ifstat from ALL hosts (irrespective of playbook rc),
# because collect_results wipes the original files in teardown.
mkdir -p "$PRESERVE"
for h in redis0 redis1 redis2 redis3; do
  sudo rsync -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
    -a "$h:/users/entall/systat_logs/" "$PRESERVE/$h/systat/" 2>/dev/null || true
  sudo rsync -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
    -a "$h:/tmp/redis_logs/" "$PRESERVE/$h/redis_logs/" 2>/dev/null || true
done
sudo rsync -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
  ycsb0:/tmp/ycsb_output_ycsb0 "$PRESERVE/" 2>/dev/null || true
echo ">>> preserved at $PRESERVE"
echo ">>> rc=$rc"
exit $rc
