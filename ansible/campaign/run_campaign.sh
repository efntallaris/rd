#!/bin/bash
# run_campaign.sh <small|big> <3to4|3to6|both> <name> [scenario ...]
#
# Runs the scale-out suites exactly as on 2026-10-05 (small) and 2026-10-06 (big), saves the client
# data of every run before the next one overwrites it, and draws the figures.
#   small = quick profile (300k keys, ~5.5 min per scenario)
#   big   = 30M dataset, 200 threads per client, 3-minute runs (~17 min per scenario for 3 -> 4,
#           ~23 min for 3 -> 6)
# Default scenarios: HEALTHY S1 S2 S3 S4 S5 (+ S9 for 3 -> 6; it runs but is not drawn).
# Output:
#   /tmp/lincheck/<name>/<setup>/            run directories, summary.txt (one line per run)
#   $DATA/<name>/<setup>/                    YCSB output + migration window per scenario
#   $DATA/<name>/figures/                    <setup>_ycsb.png, <setup>_gantt_HEALTHY.png, <setup>_summary.txt
# Run it detached and poll summary.txt:
#   setsid nohup ./run_campaign.sh big both mycampaign > /tmp/mycampaign.log 2>&1 < /dev/null &
set -u
prof=${1:?small|big}; which=${2:?3to4|3to6|both}; name=${3:?campaign name}; shift 3
HERE=$(cd "$(dirname "$0")" && pwd); A=$(dirname "$HERE")
DATA=${DATA:-/users/entall/rd/experiments/2026-10-02_scaleout_3_to_6/data}
OUT=/tmp/lincheck/$name; D=$DATA/$name
w() { while pgrep -f 'run_all_lin.sh' > /dev/null || [ -e /tmp/cluster_in_use ]; do sleep 10; done; }
snap() {   # <setup> <prefix> <scenario...>: copy /tmp/experiments/<prefix><run> into $D/<setup>/
  local setup=$1 pre=$2; shift 2
  for s in "$@"; do
    r=$([ "$s" = HEALTHY ] && echo lin_healthy || echo crash_$(echo "$s" | tr 'A-Z' 'a-z'))
    sudo mkdir -p "$D/$setup/$pre$r"
    sudo cp -r /tmp/experiments/$pre$r/ycsb /tmp/experiments/$pre$r/migration_window.txt "$D/$setup/$pre$r/" 2>/dev/null
  done
}
sudo mkdir -p "$D/figures"
if [ "$which" = 3to4 ] || [ "$which" = both ]; then
  S=("$@"); [ ${#S[@]} -eq 0 ] && S=(HEALTHY S1 S2 S3 S4 S5)
  cd "$A/lincheck"
  if [ "$prof" = big ]; then
    PROFILE=full YCSB_THREADS=200 OUT_ROOT=$OUT/3to4 \
      EXTRA_ANSIBLE_ARGS="-e dataset_snapshot=ws30m -e rdma_src_prereg_slots=1365 -e rdma_landing_prereg_pools=3 -e rdma_chain_ack_via_raft=yes -e rdma_backpatch_pool_size=8 -e rdma_transfer_chunk_slots=57 -e ycsb_preconnect=10.10.1.4:8000,10.10.1.5:8000,10.10.1.6:8000 ${EXTRA_34:-}" \
      ./run_all_lin.sh "${S[@]}"
  else
    if [ -n "${EXTRA_34:-}" ]; then
      EXTRA_ANSIBLE_ARGS="-e rdma_src_prereg_slots=1365 -e rdma_landing_prereg_pools=3 -e rdma_chain_ack_via_raft=yes $EXTRA_34" OUT_ROOT=$OUT/3to4 ./run_all_lin.sh "${S[@]}"
    else
      OUT_ROOT=$OUT/3to4 ./run_all_lin.sh "${S[@]}"
    fi
  fi
  w; snap 3to4 "" "${S[@]}"
  "$HERE/make_figures.sh" 3to4 "$prof" "$D/3to4" "$OUT/3to4" "$D/figures"
fi
if [ "$which" = 3to6 ] || [ "$which" = both ]; then
  S=("$@"); [ ${#S[@]} -eq 0 ] && S=(HEALTHY S1 S2 S3 S4 S5 S9)
  cd "$A/experiments/custom_scaleout_3to6"
  if [ "$prof" = big ]; then
    PROFILE=full YCSB_THREADS=200 SCALEOUT_EXTRA_ARGS="${EXTRA_36:-}" OUT_ROOT=$OUT/3to6 ./run_all.sh "${S[@]}"
  else
    SCALEOUT_EXTRA_ARGS="${EXTRA_36:-}" OUT_ROOT=$OUT/3to6 ./run_all.sh "${S[@]}"
  fi
  w; snap 3to6 scaleout3to6_ "${S[@]}"
  "$HERE/make_figures.sh" 3to6 "$prof" "$D/3to6" "$OUT/3to6" "$D/figures"
fi
echo "campaign $name done: summaries in $OUT/*/summary.txt, data and figures in $D"
