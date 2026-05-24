# Aqueduct scaling — chunked reshard (2 rounds × 3 sources × 683 slots)

Eight runs mirroring `ansible/results/`, but with the **chunked** migration
schedule: each donor sheds its slots in **two rounds**, alternating donors,
for 6 migrations of ~683 slots each instead of 3 migrations of 1365 slots.

The chunked schedule makes `max(slots_on_busiest_node)` drop **twice**
during the reshard (once at the end of each round) instead of once. That
in turn makes the aggregate-throughput ceiling lift **twice**, so the
client-side throughput curve climbs in a clearer staircase instead of a
single jump at the very last migration.

Same server build as `ansible/results/` (commit on branch
`aqueduct_broken`: shadow-dict backpatch with main-thread merge). Same
cluster (3 redis masters + redis3 target). Same workload + client
configurations.

## Migration schedule

```
Round 1:                                  Round 2:
  M1: redis0 → redis3, 682 slots            M4: redis0 → redis3, 683 slots
  M2: redis1 → redis3, 682 slots            M5: redis1 → redis3, 683 slots
  M3: redis2 → redis3, 682 slots            M6: redis2 → redis3, 683 slots
  ↑ end of round 1: max=4780                ↑ end of round 2: max=4097
    (was 5462 at start)                       (balanced 4-node cluster)
```

Total work moved: 4095 slots (identical to the baseline `ansible/results/`).
Only the *granularity* differs.

## Throughput summary

Start = sec 5–10 (pre-migration). End = sec 90–run-rampdown
(post-round-2, balanced 4-node steady state).

| Workload      | 1 YCSB start | 1 YCSB end | Lift | 2 YCSB start (agg) | 2 YCSB end (agg) | Lift |
|---------------|-------------:|-----------:|-----:|-------------------:|-----------------:|-----:|
| Zipfian 50/50 |        318 K |      364 K | +14% |              353 K |            446 K | +26% |
| Zipfian 95/5  |        339 K |      391 K | +15% |              363 K |            447 K | +23% |
| Uniform 50/50 |        325 K |      369 K | +13% |              342 K |            433 K | +27% |
| Uniform 95/5  |        311 K |      372 K | +19% |              361 K |            436 K | +21% |

All in Kops/sec.

## How to compare against the baseline (1-round) folder

`ansible/results/` has the same 8 experiments with the original 1-round
schedule. The end-state throughputs are essentially identical (the cluster
ends up the same shape either way). The differences are in the **shape of
the climb**:

* **Baseline (1 round):** flat at ~3-node ceiling for ~60 s, then a single
  big step up to ~4-node ceiling at the end of M3.
* **Chunked (2 rounds):** staircase climb — small uplift after round 1
  done (~sec 45) as the ceiling lifts modestly, then more lift as round 2
  redistributes load and finally a second step to the full 4-node ceiling
  at round 2 done (~sec 82).

The chunked timeline is `~80 s` of reshard (6 × ~10 s including 5 s dwells)
vs `~55 s` for the baseline (3 × ~15 s with 10 s dwells) — slightly slower
because of more inter-migration dwell, but the throughput floor stays
above 350 K throughout and approaches 450 K by mid-run.

## Folder layout

```
results_chunked/
├── 1_zipfian_5050/
│   ├── timeseries/
│   │   ├── ycsb_1client.png             YCSB throughput + latency (1 client)
│   │   ├── ycsb_2client_aggregate.png   YCSB throughput + latency (2 clients, combined)
│   │   └── throughput_1v2.png           both client counts overlaid (ylim 300–500)
│   ├── network/
│   │   ├── redis{0..3}_1client.png      single-host RX/TX MB/s (1 client)
│   │   └── redis{0..3}_2client.png      single-host RX/TX MB/s (2 clients)
│   ├── resources/
│   │   ├── all_hosts_1client.png        CPU / Net / Disk × 4 hosts overlaid
│   │   ├── all_hosts_2client.png        same, 2-client run
│   │   ├── redis{0..3}_1client.png      single-host 3-panel
│   │   └── redis{0..3}_2client.png      same, 2-client run
│   └── raw_ycsb/
│       ├── 1client_ycsb0.log
│       ├── 2client_ycsb0.log
│       └── 2client_ycsb1.log
├── 2_uniform_5050/   (same structure)
├── 3_uniform_955/    (same structure)
├── 4_zipfian_955/    (same structure)
└── summary/
    └── throughput_comparison.png        2×2 panel, one cell per workload

```

## Why no extra throughput vs the baseline

The chunked schedule changes the *shape* of the throughput curve but
**not the end-state ceiling**. Once the migration is complete (round 2
done in chunked, M3 done in baseline), both runs converge on the same
4-node balanced ceiling (~430-450 K aggregate at 2 YCSB clients,
~380-390 K at 1 YCSB client). The chunked schedule's value is operational
— smoother throughput during the migration window, visible incremental
benefits per round — not raw peak performance.
