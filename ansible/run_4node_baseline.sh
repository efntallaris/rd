#!/bin/bash
# 4-node-from-start YCSB workloada baseline. No migration.
# Mirrors run_workloada_concurrent_with_mpstat.sh, but uses
# inventory_4nodes.ini so the cluster boots with redis0..3 as masters.

set -o pipefail
cd /users/entall/rd/ansible
export ANSIBLE_FORCE_COLOR=false

TS=$(date +%Y%m%d_%H%M%S)
LOG="/tmp/4node_baseline_${TS}.log"
PRESERVE="/tmp/preserved_logs_4node_${TS}"

# Same scale as the migration test for an apples-to-apples comparison.
sudo sed -i \
  -e 's/^recordcount=.*/recordcount=10000000/' \
  -e 's/^operationcount=.*/operationcount=20000000/' \
  /rd/workloads/workloada
sudo scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
  /rd/workloads/workloada ycsb0:/users/entall/rd/workloads/workloada >/dev/null 2>&1

echo ">>> launching 4-node-baseline workloada"
echo ">>> ansible log -> $LOG"

sudo ansible-playbook -i inventory_4nodes.ini \
  experiments/custom_4node_baseline/workload.yml \
  -e "redis_variant=custom" \
  -e "experiment_name=4node_baseline_${TS}" \
  -e "redis_workload=workloada" \
  2>&1 | tee "$LOG"
rc=${PIPESTATUS[0]}

# Always preserve mpstat + ifstat from ALL hosts (collect_results wipes
# the originals during teardown).
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
