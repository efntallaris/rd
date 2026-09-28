#!/usr/bin/env bash
# Run ON THE CONTROLLER (redis0) as root, from the repo root. Replace with the exact command.
set -euo pipefail
cd "$(dirname "$0")/../../ansible"
sudo ansible-playbook -i inventory.ini experiments/custom_reshard_v2_orch_raft_chunked/workload_nround.yml \
  -e experiment_name=CHANGE_ME
