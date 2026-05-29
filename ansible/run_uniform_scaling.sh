#!/bin/bash
# Uniform-distribution scale-out comparison at 200 threads:
#  1) clean 3-SG baseline (no migration)
#  2) 3->4 SG migration run (long pre-pause + long run)
# Mirrors the zipfian runs so the +33% scaling hypothesis can be tested:
# if uniform scales ~+33% but zipfian only +18.5%, access skew is the cause.
set -uo pipefail
cd "$(dirname "$0")"
WL=workloada_scaletest_uniform
EXP=experiments/custom_reshard_v2_orch_raft_chunked

echo "############ 3-SG BASELINE (uniform, no migration) ############"
ansible-playbook -i inventory.ini "${EXP}/workload_baseline_3sg.yml" \
  -e redis_variant=custom \
  -e experiment_name=baseline_3sg_uniform_200t \
  -e redis_workload="${WL}" \
  -e ycsb_threads_run=200 > /tmp/baseline_3sg_uniform_200t.log 2>&1
echo "baseline exit=$?"

echo "############ 3->4 SG MIGRATION (uniform) ############"
ansible-playbook -i inventory.ini "${EXP}/workload.yml" \
  -e redis_variant=custom \
  -e pre_reshard_pause=140 \
  -e experiment_name=scaletest_4sg_uniform_200t \
  -e redis_workload="${WL}" \
  -e ycsb_threads_run=200 > /tmp/scaletest_4sg_uniform_200t.log 2>&1
echo "migration exit=$?"
echo "DONE"
