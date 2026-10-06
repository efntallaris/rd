# 3 vs 6 redis processes on the same 3 hosts (2026-10-01)

**Question:** on fixed hardware (redis0/1/2), does running 2 redis-server masters per host
(6 masters) instead of 1 (3 masters) raise throughput and lower latency?

**Setup:** aqueduct fork (`redis_variant=custom`), plain Redis Cluster — no Raft, no replicas,
no migration. Masters on ports 8000 (and 8001 for the 6-process run) of redis0/1/2, slots split
evenly. 5M keys, 50/50 read/update, uniform, 180 s (`workloada_procs_scaling`), 2 YCSB clients
(ycsb0 + ycsb1) × 200 threads. Uses the binaries already deployed on the hosts (no build step).
Variant: `ansible/experiments/custom_procs_scaling/` (`run_sweep.sh`).
Commit on `aqueduct_broken`: _fill in when run_.

**Result** (run 2026-10-02, one run per configuration):

| metric | 3procs | 6procs | change |
|---|---:|---:|---:|
| throughput (ops/s) | 344,698 | 648,544 | +88.1% |
| READ avg latency (us) | 1,147 | 608 | -47.0% |
| UPDATE avg latency (us) | 1,149 | 609 | -47.0% |

The servers are the bottleneck in both runs: redis-server on redis0 averages 91% of a core per
process with 3 masters and 93% with 6 (pidstat), while ycsb0 is 98% idle (mpstat).

Reading the result: YCSB is closed-loop, so 6 masters only show a gain if the 3 masters are the
bottleneck. Check `logs/<run>/logs/redis*/…/*_pidstat.txt` (redis-server near 100% CPU in the
3-process run) and the clients' mpstat; if the clients saturate first, raise `THREADS`.
Latency is the YCSB average only (the tasks run `measurementtype=timeseries`, no percentiles).

## Layout
- `run.sh` — exact command that produced the runs (runs on the controller); copies each run
  into `logs/` and prints the comparison (`experiments/tools/compare_runs.py`).
- `logs/3procs/`, `logs/6procs/` — raw output (same layout as `/tmp/experiments/<run>`:
  `ycsb/`, `logs/<host>/`).
- `figures.sh` — rebuilds `figures/` from `logs/` and reprints the comparison.
- `figures/` — generated figures (do not edit by hand).
