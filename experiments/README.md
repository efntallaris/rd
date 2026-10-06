# Experiments

One folder per experiment: the question, the exact run command, the raw logs, the scripts that
turn the logs into figures, and the figures. Shared analysis/plot scripts live in `tools/`.

(`ansible/experiments/` is different: it holds the ansible workload/playbook configs that
*run* an experiment. This folder holds what came out of it.)

## Layout
```
experiments/
├── README.md                this index
├── tools/                   shared scripts (run from anywhere)
│   ├── fetch_logs.sh        copy /tmp/experiments/<run> from the controller into <id>/logs/
│   ├── make_figures.sh      standard figure set for one run (window, ycsb, gantt, full_run)
│   ├── plot_ycsb_timeseries.py   throughput + latency over time, migration band
│   ├── plot_phase_gantt.py       per-phase migration Gantt from donor/recipient logs
│   ├── plot_full_run.py          throughput, latency, CPU, cluster NIC traffic
│   ├── plot_cpu_tputlat.py       CPU vs throughput/latency
│   ├── migration_window.py       cold-excluded migration window (also used by collect_results.yml)
│   ├── compare_runs.py           throughput + avg latency of two runs, side by side
│   ├── analyze_crash.py          crash campaign metrics + per-scenario figures
│   ├── build_artifact.py         crash campaign HTML report
│   ├── analyze_double_read_trace.py   client double-read trace analysis
│   └── figure_*.py               paper figures (protocol diagram, social-network figure)
├── _template/               copy this to start a new experiment
└── <YYYY-MM-DD>_<name>/
    ├── README.md            question, setup, commit, result
    ├── run.sh               exact command (runs on the controller)
    ├── logs/<run>/          raw output (ycsb/, logs/<host>/)
    ├── figures.sh           logs/ -> figures/
    └── figures/
```

## Workflow
```bash
# 1. on the controller: run it (the experiment's run.sh, or the HOWTO command)
# 2. on your machine, from the repo root: pull the raw output next to the figures
CONTROLLER=<user@controller> experiments/tools/fetch_logs.sh <id> <run> [<run> ...]
# 3. rebuild the figures
experiments/<id>/figures.sh          # or: experiments/tools/make_figures.sh experiments/<id>/logs/<run> experiments/<id>/figures
```
Start a new one with `cp -r experiments/_template experiments/$(date +%F)_<name>`.

Logs can be large (a 30M run's Redis logs are hundreds of MB); check the size with `du -sh`
before committing, and compress or leave out the biggest per-host logs if needed.

## Index

| experiment | what | logs here | figures |
|------------|------|-----------|---------|
| `2026-05-23_scaling_shadow_merge` | 8-run scaling matrix (4 workloads × 1/2 clients), 3 serial migrations; +12–30% lift | raw YCSB logs | – (listed in README, not kept) |
| `2026-05-23_scaling_chunked` | same matrix, chunked schedule (2 rounds × 3 donors × 683 slots) | raw YCSB logs | – |
| `2026-05-23_shadow_compare` | commit-comparison driver + shadow-merge runs 13/14 | driver log | 2 PNG |
| `2026-05-24_orchestrated_reshard` | server-side orchestrator: reshard in 11 s vs ~32 s | – | – |
| `2026-05-26_aqraft_workloads` | first AqRaft runs, YCSB a/b/c | raw YCSB load/run | – |
| `2026-06-03_aqraft_sweeps` | chunked-reshard + warm-sweep variants | – | 8 full_run PDFs |
| `2026-06-09_prod_async_apply` | production migration window, async apply | – | – |
| `2026-06-14_perchunk` | per-chunk pipelining, workloads a/b | – | 6 PDFs |
| `2026-06-20_adopt_vs_copyout` | index update: adopt-in-place vs copy-out (30M) | – | – |
| `2026-06-21_copyout_removed` | regression after removing copy-out (30M) | – | – |
| `2026-07_throughput_rise` | +29% after ownership reconcile (153K → 197K ops/s) | – | 3 PNG |
| `2026-07_crash_campaign` | S1–S5 crash scenarios | **to fetch** (see its README) | pre-fix PNGs |
| `2026-10-01_procs_3_vs_6` | 3 vs 6 redis masters on the same 3 hosts (plain cluster, no Raft): throughput + latency | **not run yet** (`run.sh`) | – |
| `2026-10-02_scaleout_3_to_6` | scale from 3 shardgroups to 6 with the AqRaft migration (sg1→sg4, sg2→sg5, sg3→sg6), healthy + crash scenarios S1–S9: all pass on the final build (20/20) | 6 first-day runs | 6 PNG |

"–" under logs means the raw run output was not kept; only the notes and/or finished figures
survived.
