# Crash campaign — client performance and time breakdown (2026-10-01)

**Question:** on the build with the linearizability fixes (see `ansible/lincheck/FIXES_LOG.md`),
what do clients see during a 30M reshard under each single-node crash, and where does the
time go?

## Setup

- 30M keys restored from snapshot `ws30m`, `workloada_prod_30m_run3min` (50/50 read/update,
  zipfian, 180 s), 2 YCSB clients × 100 threads, 60 s steady state before the reshard.
- Donors sg1/sg2/sg3 (leaders redis0/1/2), recipient sg4 (redis3 → redis4 → redis5 chain).
  One round, 4,095 slots (1,365 per donor). `--raft.quorum-reads no` (as in earlier campaigns).
- Flags: `rdma_chain_pipeline=yes rdma_chain_xsession=yes rdma_async_apply=yes
  rdma_transfer_chunk_slots=342 rdma_naive_durability=no rdma_chain_ack_via_raft=yes`.
- Build: `f74fa9c8` + working tree (redis-server built 2026-10-01 12:56 UTC).
- One repetition per scenario. No linearizability client (it would add load).

## Reproduce

```bash
cd /users/entall/rd
sudo setsid nohup experiments/2026-10-01_crash_campaign/run_campaign.sh >/dev/null 2>&1 &   # ~37 min
# then assemble logs/final_<run>/ (exp output + node logs grabbed during the run) and:
sudo python3 experiments/2026-10-01_crash_campaign/final_analysis.py experiments/2026-10-01_crash_campaign
sudo python3 experiments/2026-10-01_crash_campaign/breakdown.py experiments/2026-10-01_crash_campaign
sudo python3 experiments/2026-10-01_crash_campaign/recovery_timeline.py experiments/2026-10-01_crash_campaign/logs/final_s1
```
Raw output: `/tmp/crash_inject/final_1001/` (driver log, per-run node logs, experiment dirs).

## Results

All six runs: reshard rc=0, all three donor ranges committed on sg4, 0 crash signatures,
0 faked `INDX_UPD`, 0 YCSB UPDATE errors.

Seconds are relative to the migration start (first donor `PREP`). Baseline = mean throughput
25–8 s before the migration; "after" = 20–100 s after the last `TXN_DONE`.

| run | kill | migration | base Kops/s | min | after | s <50% | back to 90% after kill | Kops lost | READ avg µs pre / worst s | UPDATE avg µs pre / worst s | READ errors |
|---|---|---|---|---|---|---|---|---|---|---|---|
| No fault | — | 6.8 s | 151 | 139 | 169 | 0 | — | 12 | 714 / 831 | 1,921 / 2,043 | 0 |
| S1 recipient leader | +2.5 s (redis3) | 14.6 s | 162 | **0** | 185 | 4 | **5.6 s** | 752 | 669 / 766 | 1,796 / **169,880** | 646 |
| S2 donor leader, mid-transfer | +0.7 s (redis0) | 9.9 s | 150 | 23 | 173 | 2 | 3.0 s | 284 | 716 / 3,267 | 1,940 / 11,989 | 4,339 |
| S3 donor follower | +1.1 s (redis1) | 6.7 s | 161 | 142 | 173 | 0 | 0 | 37 | 674 / 831 | 1,810 / 1,999 | 0 |
| S4 recipient follower | +0.8 s (redis4) | 9.9 s | 155 | 147 | 177 | 0 | 0 | 8 | 698 / 760 | 1,876 / 1,969 | 0 |
| S5 donor leader after its transfer | +3.9 s (redis0) | 6.8 s | 150 | 32 | 174 | 1 | 1.8 s | 128 | 739 / 1,107 | 1,942 / 7,851 | 1,484 |

"Kops lost" = sum over the migration window (+15 s) of (baseline − throughput). "worst s" =
the highest per-second average latency in that window (stalled ops complete together, so
individual ops waited longer than the average shows). READ errors are YCSB reads that
returned an error (requests to a dead node before the client re-routes).

### Where the time goes (no fault)

| donor | flip → transfer starts | transfer (RDMA) | transfer end → `TXN_DONE` (merge, chain ack, `INDX_UPD`) | total |
|---|---|---|---|---|
| sg1 | 0.04 s | 1.02 s | 1.92 s | 2.99 s |
| sg2 | 0.03 s | 1.13 s | 2.59 s | 3.76 s |
| sg3 | 0.03 s | 1.01 s | 3.47 s | 4.52 s |

Donors start one after another (completion-driven dispatch), each as soon as the previous
one's data is chain-durable (~1.1 s apart), so the three transfers overlap the previous
donor's merge. The migration ends with sg3's commit at 6.8 s.

**Warm-up dip.** In every run, including no fault, throughput drops to ~50 Kops/s for ~2 s
starting 3.5 s *before* the migration: the sg4 leader binds its RDMA listeners
(`INIT-SERVER`) and pre-establishes the chain with landing-pool registration
(`CHAIN-WARM`). In a healthy migration it is the only client-visible cost; during
the migration itself latency stays flat.

### Recovery timelines (seconds after the kill)

- **S1, recipient leader** — redis4 elected 0.33 s; donors declare the recipient dead 0.60 s;
  redis4 merges the blocks it holds as a follower (sg1's range, committed by adoption) by
  2.9 s; donors re-home to redis4 at 4.0 s; sg2 re-shipped (commit 7.3 s); sg3 re-driven
  (registration 1.5 s, commit 12.2 s). Clients: 0 ops/s for ~4 s until requests reach the
  new leader; back to 90% at 5.6 s. Migration +7.8 s vs. no fault.
- **S2, donor leader + orchestrator** — redis1 elected sg1 leader and resumed sg1's session
  0.52 s (re-ship transfer 0.59–2.40 s, commit 4.1 s). The orchestrator died with redis0,
  so the playbook's re-drive dispatched sg2 (1.9 s) and sg3 (4.9 s), ~1.5 s later than
  no fault. Clients: dip to 23 Kops/s, back to 90% at 3.0 s; migration +3.1 s.
- **S3, donor follower** — no recovery needed (sg1 keeps 2/3); no client impact.
- **S4, recipient follower** — chain re-formed around redis4 at 2.5 s; the three chain acks
  arrive at 4.4–6.4 s after the migration start instead of 1.4–3.6 s, so commits are ~3 s
  later (migration +3.1 s). No client impact (minimum 147 Kops/s).
- **S5, donor leader after its transfer** — redis1 elected sg1 leader 0.41 s; the migration is
  unaffected (6.8 s). Clients see a 1 s dip to 32 Kops/s: sg1's unmigrated slots are
  unavailable until the new sg1 leader is found.

## Follow-up: follower merge moved off the main thread (2026-10-01, 16:23 UTC)

After the migration the sg4 followers merged ~7.5M keys in the main-thread `mergeBackpatchTick`
(~20% of the main thread for ~28 s). That slowed their AppendEntries replies (sg4 leader
round-trip 0.3 → ~1.4 ms), raised UPDATE latency on the migrated slots and caused the
throughput dip right after the migration. Followers now drain on the apply thread under the
per-slot locks, like the leader (`rdmaFollowerMergeSlotBackground`; see
`ansible/lincheck/FIXES_LOG.md`). Quick linearizability set: all 6 scenarios PASS on this build.

30M no-fault, one run each (`logs/final_healthy` vs `logs/final_healthy_bgf`); seconds after the
migration start:

| | main-thread follower merge | background follower merge |
|---|---|---|
| follower main-thread merge | 28 s at ~20% | none |
| migration | 6.79 s | 6.41 s |
| Kops/s: before / during / +7–20 s / +20–60 s / +60–110 s | 151 / 163 / 162 / 166 / 171 | 154 / 173 / 180 / 184 / 186 |
| UPDATE avg, +7–20 s | 1.81 ms | 1.62 ms |
| READ avg, +7–20 s | 0.64 ms | 0.58 ms |
| sg4 leader AE round-trip, +7–20 s | 1.46 ms | 0.99 ms |

The remaining round-trip increase over the pre-migration 0.3 ms is expected: before the
migration sg4 serves no traffic; afterwards it takes a quarter of the writes. The crash
scenarios have not been re-measured on this build yet.

## Follow-up 2: warm-up dip and why throughput rises only ~12–20% (2026-10-01, 17:22–17:35 UTC)

Two 30M no-fault runs with a per-instance command-rate sampler (`ops.txt`), one with the donor
warm-up registration throttled (`rdma_warm_reg_batch=16`, `rdma_warm_reg_pause_us=10000`).
Figure: `figures/final/perf_followup.png`; script: `perf_followup.py`.

**Warm-up dip = donor memory registration.** `MIGRATE-WARM` registers each migrated slot's live
blocks as RDMA memory regions (~1,365 regions, ~2.9 GB) 3.5 s before the migration. Throttled,
the registration takes longer (sg3 0.6 → 1.8 s, sg2 1.5 → 2.3 s) and the dip becomes shallower
(minimum 59 → 107 Kops/s). The dip follows the registration rate, so registration causes it.

**The servers are saturated; the donors get slower per command after the migration.**

| leader | commands/s before | share | commands/s after | share |
|---|---|---|---|---|
| sg1 | 104K | 34% | 87K | 25% |
| sg2 | 97K | 32% | 81K | 24% |
| sg3 | 105K | 34% | 79K | 23% |
| sg4 | — | — | 95K | 28% |
| all leaders | 305K | | 342K (+12%) | |

(no-throttle run; the throttled run gives the same shares.) Every leader's main thread is at
~100% CPU before and after (pidstat). The load is split almost evenly four ways afterwards, so it
is not an imbalance: each **donor** leader serves ~20% fewer commands per second at the same
100% CPU after the migration (≈100K → ≈80K), i.e. each command costs more on the donors. With 3
leaders at ~100K before and 4 at ~85K after, the total rises only ~12–20% instead of the ideal
33%. Next: compare per-command mix and cost on a donor before/after (`INFO commandstats`), e.g.
redirect/MOVED traffic for migrated slots or per-command checks that only exist after a migration.

## Follow-up 3: early warm-up, and where the capacity limit is (2026-10-01, 19:22–19:51 UTC)

**Early warm-up removes the pre-migration dip.** `-e rdma_prewarm_early=yes` runs
`ansible/tasks/cluster/reshard_prewarm.yml` (donor `MIGRATE-WARM` + recipient `CHAIN-WARM`) before
the workload starts. 30M no-fault: lowest throughput in the 12 s before the migration 59 → 149
Kops/s (baseline 163), 0 s below 80% of baseline, no extra block registration during the transfer.
sg1 (the orchestrator's own group) is still not pre-warmed.

**The bottleneck is each leader's single main thread.** Per-thread CPU (`threads.txt`): the main
thread of every donor leader is at ~100% whenever the workload runs, before and after the
migration; the sg4 leader's is at ~100% after; no other thread is busy. `io-threads` is 1.

**Capacity before/after** (200 client threads per host so the clients are not the limit; 150 s
pause so client start-up is over; `logs/final_perf_th200b`):

| threads/host | before | after | gain | READ avg before → after | UPDATE avg |
|---|---|---|---|---|---|
| 100 | 155 Kops/s | 173 Kops/s | +12% | 0.64 → 0.62 ms | 1.73 → 1.68 ms |
| 200 | 155 Kops/s | 190 Kops/s | +23% | 1.34 → 1.09 ms | 3.83 → 3.13 ms |

Three leaders saturate at ~51.5K ops/s each; four at ~47.4K each: +23% instead of the ideal +33%
because each leader's capacity is ~8% lower after the migration.

**Why per-leader capacity drops** (`cmdstats.txt`, `cmdstats_diff.py`):
- donors return no error replies after the migration and run only `raft`/`set`/`get` — no leftover
  redirect traffic;
- each command costs ~10% more on a donor afterwards (`raft` 5.3 → 5.9 µs, `set` 1.9 → 2.1,
  `get` 1.45 → 1.6); on sg4 it costs more still (`raft` 6.6, `set` 2.6, `get` 1.9 µs);
- command execution is only ~35–39% of the main thread; the rest is socket I/O and the RedisRaft
  replication loop (not profiled: `perf` is not installed). At 100 client threads the donors run
  2–3× as many AppendEntries rounds per second after the migration with a third to a half as many
  operations each (sg1: 19 → 8 ops per round).

## Files

- `figures/final/client_breakdown.png` — throughput and READ/UPDATE latency per run around the migration
- `figures/final/tput_all_70s.png`, `tput_<run>.png` — throughput timelines (kill = red line, migration = shaded)
- `figures/final/gantt_<run>.png` — per-phase Gantt (recipient rows stop at a recipient-leader kill)
- `figures/final/breakdown.json`, `breakdown.md` — all numbers above; `metrics_final.json`,
  `recovery_final.txt` — from `final_analysis.py`
- `logs/final_<run>/` — experiment output with the complete node logs

## Caveats

- One repetition per scenario; the numbers are single runs.
- Quorum reads off (the setting the linearizability check needs is `yes`; its cost is not measured here).
- Reads that hit a dead node count as YCSB READ errors; updates are retried by the client (0 errors).
