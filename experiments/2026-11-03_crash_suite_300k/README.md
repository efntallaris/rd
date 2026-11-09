# Crash-recovery suite of the 3 -> 6 scale-out — quick profile (300k keys)

Branch `aqueduct_broken`. Re-run of the suite described in `ansible/RUNBOOK_CRASH_SUITE.md`
on a freshly provisioned cluster (built from scratch 2026-11-03).

```bash
./run.sh                 # the 25-run suite (HEALTHY + C1-C17 + S1-S5,S8,S9), ~6 min per run
./run.sh S6 S7           # only these (S6/S7 are not in the default SUITE list)
./figures.sh             # redraw figures from data/ (no cluster needed)
```

## Results (2026-11-03 / 04)

All 27 runs PASS (lincheck + postcheck): HEALTHY, C1-C17, S1-S9. Per-run verdicts in
`data/summary.txt`, metrics in `figures/suite_table.md`.

The S series on its own (S1-S9 + the HEALTHY baseline): `figures/suite_table_S.md` and the
HTML page `page_s1_s9/index.html`. S6 and S7 (host crashes) were run on 2026-11-04; the
analysis tools had no entry for them, which is why `recovery_breakdown.py` (`DEAD`) and
`build_suite_page.py` (`M`, `ORDER`) now carry one.

Configuration (defaults of `run.sh`): `ROUNDS=4`, `SCALEOUT_MODE=pairwise`, `CHUNK_SLOTS=12`,
`ELECTION_TIMEOUT_MS=500`, `PROFILE=quick`, `WORKLOAD=workloada_lin_quick_uniform`,
`YCSB_THREADS=400` per client.

## Layout

| path | what |
|---|---|
| `data/<S>/lincheck/` | the lincheck/crash run dir (`run.log`, `history.jsonl`, `logs/<host>/`) |
| `data/<S>/exp/` | the YCSB result dir (`ycsb/<host>/tmp/ycsb_output_<host>`, server logs) |
| `figures/<S>_recovery.png` | detailed recovery timeline (`tools/scaleout/recovery_breakdown.py`) |
| `figures/<S>_table.txt` | the phase table of that run |
| `figures/<S>_recovery.json` | input of the paper figure |
| `figures/paper_<S>_recovery.pdf` | paper figure, 3.33 in (one column) + `_wide.pdf` at 7 in |
| `figures/<S>_tput_lat.png` | YCSB throughput + mean read/update latency over time, both clients summed |
| `figures/suite_table.md` / `.csv` | one row per scenario |

## Cluster set-up done for this run (fresh nodes)

- all 8 hosts on `aqueduct_broken` @ `90879ccd`
- `/mnt/aqsnap/redis_data` created on redis0-5 (on the root fs; the 300k profile needs little space)
- redis built on redis0-5 (`tasks/build/build_redis_custom.yml`), redisraft.so built + distributed
- YCSB built on ycsb0/ycsb1, workload files copied to `/users/entall/rd/workloads/`
- `workloada_lin_quick_uniform` created (= `workloada_lin_quick` with `requestdistribution=uniform`)
- `lincheck/lincheck` built (Go 1.23.12 in `~/go-sdk`); self-test as documented
- root's `known_hosts` seeded for all hosts (the YCSB build rsyncs as root without StrictHostKeyChecking=no)
- matplotlib installed for the user on redis0
