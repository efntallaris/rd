#!/bin/bash
# Run workloada at full scale (50M records / 18M ops) and, if the playbook
# fails, snapshot ALL redis logs from every node to /tmp/preserved_logs_<ts>/
# BEFORE the next clean_runtime can wipe them.

set -o pipefail

cd /users/entall/rd/ansible
export ANSIBLE_FORCE_COLOR=false

TS=$(date +%Y%m%d_%H%M%S)
LOG="/tmp/workloada_full_${TS}.log"
PRESERVE="/tmp/preserved_logs_${TS}"

# Make sure workloada is at full scale.
sudo sed -i \
  -e 's/^recordcount=.*/recordcount=10000000/' \
  -e 's/^operationcount=.*/operationcount=3600000/' \
  /rd/workloads/workloada
for h in ycsb0 redis0 redis1 redis2 redis3; do
  sudo scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    /rd/workloads/workloada $h:/users/entall/rd/workloads/workloada >/dev/null 2>&1
done

echo ">>> running workloada at full scale; logs -> $LOG"
sudo ansible-playbook -i inventory.ini \
  experiments/custom_reshard_v2/workload.yml \
  -e "redis_variant=custom" \
  -e "experiment_name=workloada_full_${TS}" \
  -e "redis_workload=workloada" \
  2>&1 | tee "$LOG"
rc=${PIPESTATUS[0]}

if [ $rc -ne 0 ]; then
  echo ">>> playbook failed (rc=$rc) — preserving redis logs to $PRESERVE"
  mkdir -p "$PRESERVE"
  for h in redis0 redis1 redis2 redis3; do
    echo ">>> grabbing $h logs"
    sudo rsync -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
      -a "$h:/tmp/redis_logs/" "$PRESERVE/$h/" 2>/dev/null || true
    sudo ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $h \
      "ps aux | grep redis-server | grep -v grep" \
      > "$PRESERVE/$h.ps.txt" 2>/dev/null || true
  done
  echo ">>> preserved logs at $PRESERVE"
else
  echo ">>> workloada completed cleanly (rc=0)"
fi

echo ">>> overall rc=$rc"
exit $rc
