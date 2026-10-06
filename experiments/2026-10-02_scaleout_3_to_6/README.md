# Scale-out from 3 shardgroups to 6 with the AqRaft migration (2026-10-02)

**Question:** when the cluster doubles (3 new replicated groups on redis3/4/5, each donor
handing half its range to its own recipient), what happens to throughput and latency during
and after the migration, and does the migration stay correct under crashes — including a
whole-host crash that hits three groups at once?

**Setup:** aqueduct fork + RedisRaft, 6 shardgroups × 3 replicas, pairs sg1→sg4, sg2→sg5,
sg3→sg6, 2730 slots per donor, three migrations in parallel. Topology, procedure and
scenarios: `ansible/experiments/custom_scaleout_3to6/README.md`. Quick profile: 300k keys,
`workloada_lin_quick`, 2 YCSB clients. Commit on `aqueduct_broken`: _fill in when run_.

Companion to `2026-10-01_procs_3_vs_6`: that one measured what 6 masters buy on the same
3 hosts without Raft or migration (+88% throughput); this one gets from 3 groups to 6 online.

**Result:** on the final build every scenario passes twice (20/20): the history of 200 test keys
is linearizable, the migration is durable on every recipient leader, no node crashed, each group's
live replicas are identical and no migrated key is missing. HEALTHY migrates the 3 x 2730 slots in
about 15 s (6 rounds per donor, three pairs in parallel); in the first HEALTHY run total throughput
went from 99K to 127K ops/s (+28%; lincheck load, 32 threads per client, not a tuned measurement).

Runs on the controller: final build `/tmp/lincheck/scaleout3to6/final6/` (`summary.txt`, one
directory per run with history, checks and node logs). `logs/` and `figures/` here hold the six
first-day runs whose output was collected (HEALTHY, S1, S2, S3, S5, S7). What was fixed in
between: `ansible/experiments/custom_scaleout_3to6/README.md` and `ansible/lincheck/FIXES_LOG.md`.

| scenario | fault | first run (2026-10-02) | final build (2026-10-03, 2 runs each) |
|---|---|---|---|
| HEALTHY | none | PASS | PASS, PASS |
| S1 | recipient leader (sg4) mid-transfer | FAIL: an sg5 follower lacked one round | PASS, PASS |
| S2 | donor leader (sg1) mid-transfer | FAIL: 25 keys with stale reads | PASS, PASS |
| S3 | donor follower | PASS | PASS, PASS |
| S4 | recipient chain follower | FAIL: migration incomplete | PASS, PASS |
| S5 | donor leader after transfer | PASS | PASS, PASS |
| S6 | recipient host redis3 (sg4 leader + sg5, sg6 followers) | FAIL: migration incomplete | PASS, PASS |
| S7 | donor host redis0 (sg1 leader + sg2, sg3 followers) | FAIL: 35 violating keys | PASS, PASS |
| S8 | sg1 leader + sg4 leader | FAIL: assertion on the promoted sg4 leader | PASS, PASS |
| S9 | sg1 leader + sg5 leader | FAIL: migration incomplete, stale reads | PASS, PASS |

## Layout
- `run.sh [scenario ...]` — exact command (runs on the controller); copies each run into
  `logs/<scenario>/` (the collected run) and `logs/<scenario>/lincheck/` (history, checks, node logs).
- `figures.sh` — rebuilds `figures/` from `logs/`: migration window + total throughput/latency
  over both clients.
- `figures/` — generated figures (do not edit by hand).
