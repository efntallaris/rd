#!/bin/bash
# Like run_workloada_concurrent_with_mpstat.sh but drives TWO YCSB clients
# (ycsb0 + ycsb1) via inventory_2ycsb.ini. The run playbook's start_ycsb_async
# / wait_ycsb already fan out to all ycsb_nodes, so both clients run the same
# workload concurrently (~2x offered load on the cluster).
set -o pipefail
cd /users/entall/rd/ansible
export ANSIBLE_FORCE_COLOR=false

TS=$(date +%Y%m%d_%H%M%S)
LOG="/tmp/workloada_2ycsb_${TS}.log"
MPSTAT="/tmp/redis3_mpstat_2ycsb_${TS}.txt"
PRESERVE="/tmp/preserved_logs_2ycsb_${TS}"

# Full scale workload
sudo sed -i \
  -e 's/^recordcount=.*/recordcount=10000000/' \
  -e 's/^operationcount=.*/operationcount=40000000/' \
  /rd/workloads/workloada

# Push workload to BOTH ycsb hosts (create dir first on ycsb1)
for y in ycsb0 ycsb1; do
  sudo ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "$y" \
    "mkdir -p /users/entall/rd/workloads" >/dev/null 2>&1
  sudo scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    /rd/workloads/workloada "$y:/users/entall/rd/workloads/workloada" >/dev/null 2>&1
done

# redis3 mpstat sidecar
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

echo ">>> launching 2-YCSB concurrent workloada at full scale"
echo ">>> ansible log -> $LOG"

sudo ansible-playbook -i inventory_2ycsb.ini \
  experiments/custom_reshard_v2/workload.yml \
  -e "redis_variant=custom" \
  -e "experiment_name=workloada_2ycsb_${TS}" \
  -e "redis_workload=workloada" \
  -e "pre_reshard_pause=10" \
  2>&1 | tee "$LOG"
rc=${PIPESTATUS[0]}

kill $SIDECAR_PID 2>/dev/null

mkdir -p "$PRESERVE"
for h in redis0 redis1 redis2 redis3; do
  sudo rsync -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
    -a "$h:/users/entall/systat_logs/" "$PRESERVE/$h/systat/" 2>/dev/null || true
  sudo rsync -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
    -a "$h:/tmp/redis_logs/" "$PRESERVE/$h/redis_logs/" 2>/dev/null || true
done
for y in ycsb0 ycsb1; do
  sudo rsync -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
    "$y:/tmp/ycsb_output_$y" "$PRESERVE/" 2>/dev/null || true
done
echo ">>> preserved at $PRESERVE"
echo ">>> rc=$rc"
exit $rc
