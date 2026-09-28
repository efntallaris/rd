# Per-chunk transfer/backpatch pipelining (2026-06-14)

YCSB timeseries (raw and smoothed) and per-phase Gantt charts for the per-chunk pipelined
reshard, workloads a and b. Run and plot commands:
`ansible/experiments/custom_reshard_v2_orch_raft_chunked/HOWTO.md` (run `perchunk_4chunk_workload<x>`).

- `figures/ycsb_workload{a,b}.pdf`, `figures/ycsb_workload{a,b}_smooth.pdf`
- `figures/gantt_workload{a,b}.pdf`
- `logs/` — empty; fetch `perchunk_4chunk_workloada` / `_workloadb` if they still exist.
