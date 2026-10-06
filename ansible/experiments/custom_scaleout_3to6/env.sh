# Environment that points the lincheck / crash harness (ansible/lincheck,
# ansible/crash) at the 3 -> 6 scale-out. Sourced by run_scenario.sh and run_all.sh.
#
# Env: ROUNDS (default 6), SCALEOUT_MODE (sequential|pipelined|parallel), CHUNK_SLOTS (57), SCALEOUT_EXTRA_ARGS (more -e flags),
#      DATASET_SNAPSHOT (PROFILE=full only, default ws30m_3to6), plus everything
#      run_lin_scenario.sh takes (PROFILE, WORKLOAD, YCSB_THREADS, ROUNDS, ...).
SCALEOUT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export INVENTORY=inventory_scale3to6.ini
export WORKLOAD_PLAYBOOK=experiments/custom_scaleout_3to6/workload.yml
export SCENARIOS_ENV="$SCALEOUT_DIR/scenarios.env"
export EXP_PREFIX=scaleout3to6_
export RECIPIENT_LOGS=none
export AQ_TOPOLOGY=/tmp/aq_scaleout_topology.json
export LIN_SLOT_RANGES="0-2729,5461-8190,10922-13651"   # = scaleout_pairs.py plan
# Rounds. A recipient instance registers 8 chain pools (+ up to 8 landing pools on
# the leader), each sized for ONE round's slots, and a recipient host runs three
# instances: about 34 x slots-per-round x 2 MiB per host. One round of 2730 slots
# is > 100 GB and takes the 62 GB hosts down (2026-10-02). 6 rounds of 455 slots
# is ~32 GB. Keep slots-per-round <= 510 and ROUNDS < 8 (pools are not reused).
export ROUNDS="${ROUNDS:-6}"
# Load (quick profile). run_lin_scenario.sh's defaults (32 threads per client,
# zipfian) leave the servers idle: the clients are the limit and the run shows
# ~95k -> ~125k ops/s. 400 threads per client saturate the leaders before and
# after the scale-out, and uniform keys split every donor's load evenly with
# its recipient: ~155k -> ~305k ops/s (2026-10-03; zipfian + 200 threads gave
# ~148k -> ~258k). NB the YCSB output files hold every status line twice;
# count each second once. workloada_lin_quick_uniform is workloada_lin_quick with
# requestdistribution=uniform; like the other workload files it lives on
# ycsb0/ycsb1 under /users/entall/rd/workloads (not in the repo).
if [ "${PROFILE:-quick}" != "full" ]; then
  export WORKLOAD="${WORKLOAD:-workloada_lin_quick_uniform}"
  export YCSB_THREADS="${YCSB_THREADS:-400}"
fi
SCALEOUT_ROUND_SLOTS=$(( (2730 + ROUNDS - 1) / ROUNDS ))
# This replaces run_lin_scenario.sh's default -e flags (prereg sized for 1365
# slots and three donors into one recipient). n_rounds is repeated here because
# run_crash_scenario.sh passes n_rounds=1 and the later -e wins.
EXTRA_ANSIBLE_ARGS="-e @$SCALEOUT_DIR/topology.yml -e n_rounds=$ROUNDS -e rdma_src_prereg_slots=2730"
# Speed (2026-10-03, see README "Migration duration"): one landing pool per round
# registered at start-up (a pool is not reusable, and registering one inside a
# round costs 0.3 s), and 57-slot chunks so the leader -> follower -> tail copies
# follow the donor's copy closely (run_lin_scenario.sh passes 342).
EXTRA_ANSIBLE_ARGS="$EXTRA_ANSIBLE_ARGS -e rdma_landing_prereg_pools=$(( ROUNDS < 8 ? ROUNDS : 8 )) -e rdma_landing_prereg_slots=$SCALEOUT_ROUND_SLOTS"
EXTRA_ANSIBLE_ARGS="$EXTRA_ANSIBLE_ARGS -e rdma_transfer_chunk_slots=${CHUNK_SLOTS:-57}"
# Client threads open their connections to every recipient replica at start-up (FIXES_LOG 39).
EXTRA_ANSIBLE_ARGS="$EXTRA_ANSIBLE_ARGS -e ycsb_preconnect=10.10.1.4:8000,10.10.1.5:8000,10.10.1.6:8000,10.10.1.4:8001,10.10.1.5:8001,10.10.1.6:8001,10.10.1.4:8002,10.10.1.5:8002,10.10.1.6:8002"
# 8 merge threads on the recipient leader (server default 3). 16 merges faster but starves the
# leader's request handling while it runs (dips to ~110k for 0.2 s on 30M); see FIXES_LOG 35, 40.
EXTRA_ANSIBLE_ARGS="$EXTRA_ANSIBLE_ARGS -e rdma_backpatch_pool_size=${MERGE_THREADS:-8}"
EXTRA_ANSIBLE_ARGS="$EXTRA_ANSIBLE_ARGS -e rdma_chain_ack_via_raft=yes -e scaleout_mode=${SCALEOUT_MODE:-sequential}"
[ "${PROFILE:-quick}" = "full" ] && EXTRA_ANSIBLE_ARGS="$EXTRA_ANSIBLE_ARGS -e dataset_snapshot=${DATASET_SNAPSHOT:-ws30m_3to6}"
export EXTRA_ANSIBLE_ARGS="$EXTRA_ANSIBLE_ARGS ${SCALEOUT_EXTRA_ARGS:-}"
