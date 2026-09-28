# Crash campaign — AqRaft reshard under node failures (2026-07)

**Question:** does the reshard survive a node crash mid-migration without losing data or faking
durability, and how fast does throughput recover?

**Setup:** 30M keys (debug runs: 3M / 500k), YCSB workload a (50/50), 2 clients, 100 threads.
Donors sg1–sg3 (redis0/1/2), recipient sg4 (redis3 → redis4 → redis5 chain). Kill via
`ansible/crash/crash_inject.sh`, armed on a log marker (`ansible/crash/scenarios.env`).
Branch `aqueduct_broken`; recovery code up to `d53dfcd4`.

| run | kill | expected |
|-----|------|----------|
| HEALTHY (`campaign_heal`) | none | reference |
| S1 (`crash_s1`) | recipient leader redis3 at chunk 6/12 | donor re-ships to promoted leader |
| S2 (`crash_s2`) | donor leader redis0 at TRANSFER | new donor leader resumes |
| S3 (`crash_s3`) | donor follower redis1 | unaffected |
| S4 (`crash_s4`) | recipient follower redis4 | chain re-forms |
| S5 (`crash_s5`) | donor leader redis0 after its DONE | handover only |

Design + results: `ansible/experiments/custom_reshard_v2_orch_raft_chunked/CRASH_SCENARIOS.md`,
`FAULT_TOLERANCE_STATUS.md`.

## Layout
- `run.sh` — runs the campaign on the controller (wraps `ansible/crash/run_all_scenarios.sh`).
- `logs/` — raw output, fetched with `experiments/tools/fetch_logs.sh`:
  - `logs/campaign/` — `/tmp/crash_inject/campaign` (per-scenario ycsb outputs, recipient-leader
    log snapshot, run logs)
  - `logs/crash_s1/` … — `/tmp/experiments/crash_s<N>` (full collected run: ycsb/ + logs/<host>/)
- `figures.sh` — rebuilds every figure below from `logs/`.
- `figures/` — `crash_s*_ycsb_highlighted.png`, `crash_timelines.png` (pre-fix runs, 2026-07-11:
  S1/S2 still show the orchestration abort), `s1_preview.png`, `s1_fixed.png`.

**Logs status:** not fetched yet — the final-campaign logs are still on the controller.
```bash
CONTROLLER=<user@controller> experiments/tools/fetch_logs.sh 2026-07_crash_campaign \
    /tmp/crash_inject/campaign crash_s1 crash_s2 crash_s3 crash_s4 crash_s5 campaign_heal
experiments/2026-07_crash_campaign/figures.sh
```
