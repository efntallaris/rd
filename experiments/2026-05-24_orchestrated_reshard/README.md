# Orchestrated reshard — what's happening

Server-side orchestrator (`RDMA MIGRATE-ALL`) lands the reshard in
**11 s** vs ~32 s for the serial-tight baseline (≈ duration of one
migration, since all 3 donors run in parallel).

## ⚠ Caveat on this experiment

The orchestrated run's YCSB runner has a **60-minute idle gap** between
its pre-reshard and post-reshard YCSB iterations. So **YCSB was NOT
running during the actual reshard** — there's no continuous throughput
trace across the migration. To compare apples-to-apples against the
serial baseline (which kept YCSB running through the reshard), we need
to rerun with the same runner shape.

What we have:

| Phase  | Wall clock                | n   | Aggregate steady (Kops/s) |
|--------|---------------------------|-----|---------------------------|
| 1      | 17:20:12 – 17:24:04       | 233 | ~346 K (warm-up)          |
| 2      | 17:35:41 – 17:39:24       | 224 | **~360 K (pre-reshard)**  |
| ───    | RESHARD 17:39:59 – 17:40:10 | —   | YCSB idle                 |
| 3      | 18:39:49 – 18:40:01       | 13  | **~366 K (post-reshard)** |

Phase 3 is only 13 samples; not enough to conclude steady-state.

## Figures

### [orch_diagnostic.png](orch_diagnostic.png)

Two-panel diagnostic:

- **(a) YCSB aggregate throughput** — phase 2 (pre-reshard, circles)
  and phase 3 (post-reshard, squares) plotted with a shaded 11s reshard
  band in between. Y-axis 0–500 Kops/s. Pre-reshard hovers ~360 K right
  up to the migration; the 13 post-reshard samples hover ~366 K.
- **(b) Per-host total NIC throughput** during the actual migration
  window. **This is the headline data**: the shaded band marks the
  reshard. redis0/1/2 (donors) burst RDMA traffic; redis3 (recipient)
  receives the burst. Use this panel to inspect whether the recipient
  recovers to a steady-state band after the migration ends.

### Existing zoom plots (kept for reference)

- [timeseries/ycsb_2client_orch.png](timeseries/ycsb_2client_orch.png) —
  concatenated phase 2 + phase 3 via `plot_ycsb_timeseries.py` with
  PLOT_X_MAX widened. The FLIP markers land outside the panel because
  the redis log markers' true wall clock falls in the 60-min idle gap.
- [resources/all_hosts_orch.png](resources/all_hosts_orch.png) —
  per-host CPU/Net/Disk panels from `plot_resources_combined`.
  Resource samples cover the migration window correctly.
- [baseline_serial/](baseline_serial/) — same plots for
  `workloada_2ycsb_20260523_102716` (serial-tight 2-YCSB, no idle gap,
  continuous trace).

## Next step

Re-run orchestrated with the **same runner as the serial baseline**
(continuous YCSB through reshard, no idle gap). Then we can fairly
compare post-reshard plateau on equal footing and decide whether the
parallel-FLIP path actually leaves the cluster in a worse steady state
(my earlier ~360 K vs 438 K claim conflated this run's tiny post-reshard
window with steady-state).
