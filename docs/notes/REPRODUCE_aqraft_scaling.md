# AqRaft migration — scaling & figure reproduction guide

How to build, deploy, run, and reproduce the migration/scaling experiments and
their figures. Branch: `aqueduct_broken`.

## What's in this work

Two server-side fixes (in `redis/src/cluster_rdma.c` + `redis/src/cluster.h`),
on top of the pipelined-transfer / completion-driven / 3-flag-DONE base:

1. **Round 2 — `chain_durable` early dispatch.** The sequencer dispatches the
   next donor on a new non-terminal `CHAIN_DURABLE` signal (merge_done &&
   chain_acked) instead of waiting for the full 3-flag terminal. The 3-flag
   DONE invariant on slot release is unchanged. (~14 `chain_durable` refs.)
2. **Lever 4 — landing-pool reclaim race fix.** `_Atomic int
   landing_pool_released` + CAS-guarded `backpatchReleaseLandingPool()`: the
   donor landing pool is reclaimed only once the main-thread merge has drained
   (`merge_done`), never while merges still read from it. Fixes the recipient
   SIGSEGV in `r_allocator_insert_kvobj` that made round-2 migrations FAIL under
   load. (~5 `landing_pool_released` refs.)

## Environment

- Root-owned tree under `/users/entall/rd`. Edits to `redis/src/*` and the
  ansible tree need `sudo` (the file tools run unprivileged). `git`
  stash/apply/checkout that rewrites those files also needs `sudo`.
- 7-host cluster via `ansible/inventory.ini`: redis0/1/2 (donor shardgroups
  sg1/2/3), redis3+redis4+ycsb1 (recipient sg4: leader + 2 chain followers),
  ycsb0 (YCSB driver). `ansible/ansible.cfg` sets host_key_checking=False,
  remote_user=root. Raw ssh/scp/rsync to these hosts need
  `-o StrictHostKeyChecking=accept-new`.

## Build + deploy

```bash
cd /users/entall/rd
sudo make -j -C redis/src redis-server                       # ~build, RC=0
cd ansible
sudo ansible-playbook -i inventory.ini tasks/build/deploy_redis_binary.yml
# YCSB client (only when ycsb_client/ java changes):
sudo ansible-playbook -i inventory.ini tasks/build/build_ycsb.yml
```

## Workload files (gotcha)

`build_ycsb.yml` copies `/rd/workloads/*` to the YCSB node. The canonical
workloads live in `ycsb_client/workloads/` — stage before first run:
```bash
sudo cp ycsb_client/workloads/workloada      /rd/workloads/workloada
sudo cp ycsb_client/workloads/workloada_prod /rd/workloads/workloada_prod
```

## Run a migration experiment

```bash
cd /users/entall/rd/ansible
sudo ansible-playbook -i inventory.ini \
  experiments/custom_reshard_v2_orch_raft_chunked/workload.yml \
  -e redis_variant=custom \
  -e pre_reshard_pause=130 \          # CRITICAL: see note below
  -e rdma_migration_peer_stagger_ms=0 \   # 0 = completion-driven sequencer
  -e experiment_name=prod_chunked_during \
  -e redis_workload=workloada_prod \
  -e ycsb_threads_run=200
```

**CRITICAL — `pre_reshard_pause`:** YCSB's timeseries does ~0 ops for the first
~84s (200-thread warmup). If the migration fires before that (small pause like
40), reads never overlap the migration window — `twoSided=0`, you measure
nothing, and OVERALL throughput looks artificially low (~106K). Use
`pre_reshard_pause=130` on a long workload (`workloada_prod`, 300s) so the
migration lands at ~t=130s, DURING live reads. This is the only correct way to
measure migration-window behavior.

Variants:
- non-chunked (1 migration event): `experiments/custom_reshard_v2_orch_raft/workload.yml`
- 3-shard baseline (no migration): `.../custom_reshard_v2_orch_raft_chunked/workload_baseline_3sg.yml`
- 4-way even-partition baseline (no migration): same baseline_3sg.yml plus
  `-e @vars_4way_partition.yml` (4 disjoint shardgroups sg1/2/3/5).

## Verify gates

```bash
B=/tmp/experiments/<experiment_name>
F=$B/ycsb/ycsb0/tmp/ycsb_output_ycsb0
grep -aoE "ROUND [0-9] FINAL_STATE=[A-Za-z]+" /tmp/<run>.log        # both DONE
grep -ac "crashed by signal" $B/logs/redis3/tmp/redis_logs/redis3_sg4.log  # 0
grep -aoE 'Throughput\(ops/sec\), [0-9.]+|\[READ\], AverageLatency\(us\), [0-9.]+|\[READ\], Return=(OK|ERROR), [0-9]+' "$F"
# per-round migration wall time:
grep -aoE "orch_id=[0-9]+ (DONE|FAILED) .* elapsed=[0-9]+s" $B/logs/redis0/tmp/redis_logs/redis0_sg1.log
# client double-read routing audit (every 3s):
grep -a "AQRAFT-INSTR" "$F" | tail -3
```

## Reproduce the figures

matplotlib is required (`python3 -m pip install --user matplotlib` if missing).
Plot scripts read each run's `ycsb_output_ycsb0`. The `current ops/sec` status
lines give throughput-over-time; `[OVERALL]` gives the headline average (lower,
because it includes the ~84s warmup).

- **`figures/prod_chunked.png`** — production run throughput-over-time + failed-
  read strip + migration band. Parse `<N> sec: ... <tp> current ops/sec` per
  second (dedup by sec), re-zero x to first active sec; mark the migration
  window (~46s into reads for pre_reshard_pause=130).
- **`figures/scaling_3line.png`** — 3-shard baseline vs 4-way partition vs
  3→4 migration, throughput-over-time. Runs: `scaling_baseline_3sg`,
  `scaling_baseline_4way`, `scaling_4sg_migration`.

Headline numbers (prod, chunked, migration-during-reads):
OVERALL ~116K ops/s, READ ~609us, both rounds DONE (13s + 6s), 0 crashes,
~0.31% read errors (concentrated in warmup, not the migration window).
~8.59 GB transferred (4095 slots × 2 MiB full-block; real key data is ~tens of
MB — full-block shipping is a deliberate choice, lever #5).

## Per-migration phase breakdown (one ~4.3s donor cycle, R1)

REGISTER ~0.4s (9%) → RDMA TRANSFER ~1.4s (33%, 1.43 GB) → BACKPATCH merge
~1.2s (28%) → CHAIN forward ~0.7s (16%) → INDX_UPD/ack ~0.55s (13%). Donors are
serialized (completion-driven), so 6 cycles (3 donors × 2 rounds) ≈ 21s total.
Round 2 cycles are faster (~1.8s) ONLY because the chain isn't established for
sess=2 and the ~0.7s chain forward is skipped (lever #4, a correctness gap, not
a real speedup).
