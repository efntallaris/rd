#!/bin/bash
set -o pipefail
cd /users/entall/rd/ansible
export ANSIBLE_FORCE_COLOR=false
TS=$(date +%Y%m%d_%H%M%S)
LOG="/tmp/workloada_1ycsb_chunked_${TS}.log"
PRESERVE="/tmp/preserved_logs_1ycsb_chunked_${TS}"
sudo sed -i -e 's/^recordcount=.*/recordcount=10000000/' -e 's/^operationcount=.*/operationcount=40000000/' /rd/workloads/workloada
sudo scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null /rd/workloads/workloada ycsb0:/users/entall/rd/workloads/workloada >/dev/null 2>&1
echo ">>> launching 1-YCSB chunked at $(date)"
sudo ansible-playbook -i inventory.ini experiments/custom_reshard_v2_chunked/workload.yml \
  -e "redis_variant=custom" -e "experiment_name=workloada_1ycsb_chunked_${TS}" \
  -e "redis_workload=workloada" -e "pre_reshard_pause=10" 2>&1 | tee "$LOG"
rc=${PIPESTATUS[0]}
mkdir -p "$PRESERVE"
for h in redis0 redis1 redis2 redis3; do
  sudo rsync -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" -a "$h:/users/entall/systat_logs/" "$PRESERVE/$h/systat/" 2>/dev/null || true
  sudo rsync -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" -a "$h:/tmp/redis_logs/" "$PRESERVE/$h/redis_logs/" 2>/dev/null || true
done
echo ">>> done rc=$rc"
exit $rc
