# Runbook: the two scale-out experiments (3 -> 4 and 3 -> 6) with failure scenarios

Last updated 2026-10-06. These two setups, each with its failure scenarios, are what we run from
now on. Keep this file current: when a command, default or caveat changes, change it here.
Detailed history of every fix is in `ansible/lincheck/FIXES_LOG.md` (entries 42-45 for 5-6 October).

Everything runs from redis0 (the control node). The repo is root-owned: edit with `sudo`.

## 0. Reproduce the campaigns of 5-6 October with one command

```bash
cd /users/entall/rd/ansible/campaign

# 30M dataset, both setups, as run on 2026-10-06 (13/13 pass). About 4.5 hours.
setsid nohup ./run_campaign.sh big both <name> > /tmp/<name>.log 2>&1 < /dev/null &

# quick profile, both setups, as run on 2026-10-05. About 1 h 15 min.
setsid nohup ./run_campaign.sh small both <name> > /tmp/<name>.log 2>&1 < /dev/null &

# one setup, chosen scenarios
./run_campaign.sh small 3to4 <name> HEALTHY S1 S5
./run_campaign.sh big   3to6 <name> S1
```

- Progress: `cut -c1-60 /tmp/lincheck/<name>/3to4/summary.txt` (and `3to6`), one line per finished run.
- Results: `experiments/2026-10-02_scaleout_3_to_6/data/<name>/<setup>/` (client data per scenario)
  and `.../data/<name>/figures/` (`<setup>_ycsb.png`, `<setup>_gantt_HEALTHY.png`,
  `<setup>_summary.txt` with result, migration window and per-second throughput per scenario).
- Redraw figures from saved data:
  `./make_figures.sh <3to4|3to6> <small|big> <data dir> <lincheck out root> <figure dir>`.
- Extra server settings for one campaign: `EXTRA_34="-e ..."` / `EXTRA_36="-e ..."` in the
  environment, e.g. `EXTRA_34="-e rdma_peer_probe_grace_ms=50"`.
- Before starting: section 2 (nothing else running, no `/tmp/cluster_in_use`). The server build must
  be deployed (`ansible/deploy_redis.sh`, section 7).

Saved reference runs (same layout, plus `lincheck_runs.tar.gz` with every server log):
`experiments/2026-10-02_scaleout_3_to_6/data/big_2026-10-06/` and `data/small_2026-10-05/`.

What `run_campaign.sh big` passes that the plain scripts do not: for 3 -> 4,
`-e dataset_snapshot=ws30m -e rdma_backpatch_pool_size=8 -e rdma_transfer_chunk_slots=57
-e ycsb_preconnect=10.10.1.4:8000,10.10.1.5:8000,10.10.1.6:8000` on top of the common flags, with
`PROFILE=full YCSB_THREADS=200`. For 3 -> 6, `PROFILE=full YCSB_THREADS=200` only (its `env.sh`
already sets 8 merge threads, 57-slot chunks and the pre-connects).

Scenario names (both setups, since 2026-10-05): S1 recipient leader, S2 donor leader, S3 donor
follower, S4 recipient follower, S5 both leaders (was S8). 3 -> 6 also runs S9 (leaders of two
different pairs); it is not drawn in the figure.

Server settings these runs rely on (defaults in `tasks/cluster/start_redisraft_instances.yml`):
`rdma_fwd_slot_gate=yes` (merge overlaps replication), `rdma_peer_probe_ms=300` (dead-node check),
`rdma_chain_warm_on_promote=yes`, `rdma_peer_probe_grace_ms=-1` (parallel follower check OFF: it
gives S4 a 1 s dip). 3 -> 6 driver: `scaleout_pairs.py` waits for the previous pair's last hop.

Reference results, 30M, 2026-10-06 (one run per scenario):

| | No fault | S1 | S2 | S3 | S4 | S5 |
|---|---|---|---|---|---|---|
| 3 -> 4 migration | 3.5 s | 6.6 s | 6.5 s | 3.6 s | 4.9 s | 7.1 s |
| 3 -> 4 seconds under 50% | 0 | 2 | 2 | 0 | 0 | 3 |
| 3 -> 6 migration | 11.9 s | 12.8 s | 12.8 s | 12.0 s | 11.6 s | 14.2 s |
| 3 -> 6 seconds under 50% | 0 | 1 | 2 | 0 | 0 | 1 |

Throughput: 3 -> 4 about 150k -> 170k ops/s; 3 -> 6 about 150k -> 267k ops/s.

## 1. The two setups at a glance

| | 3 -> 4 (single recipient) | 3 -> 6 (three pairs) |
|---|---|---|
| Donors | sg1, sg2, sg3 on redis0/1/2 | same |
| Recipients | sg4 on redis3 (leader), redis4, redis5 | sg4, sg5, sg6 on redis3/4/5 (leaders redis3, redis4, redis5) |
| What moves | 1365 slots from each donor into sg4 | 2730 slots from each donor to its own recipient (sg1->sg4, sg2->sg5, sg3->sg6) |
| Rounds | 1 per donor (`ROUNDS=N` for more) | 6 per pair, 455 slots each |
| Order | donors one after another (orchestrated by sg1's leader) | sequential, one pair at a time (`SCALEOUT_MODE`) |
| Inventory / topology | `inventory.ini`, `group_vars/all.yml` | `inventory_scale3to6.ini`, `experiments/custom_scaleout_3to6/topology.yml` |
| Entry point | `ansible/lincheck/run_all_lin.sh` | `ansible/experiments/custom_scaleout_3to6/run_all.sh` |
| Scenarios file | `ansible/crash/scenarios.env` | `ansible/experiments/custom_scaleout_3to6/scenarios.env` |
| Raft election / request timeout | 300 / 50 ms | 1000 / 100 ms |
| Data in `/tmp/experiments/` | `lin_healthy`, `crash_s1` ... | `scaleout3to6_lin_healthy`, `scaleout3to6_crash_s1` ... |

Both use three-replica Raft groups, quorum reads on, two YCSB clients (ycsb0, ycsb1) and the
linearizability probe client on ycsb0.

## 2. Before any run

```bash
# nothing of ours still running (another session may run nic_sampler.sh: leave it)
ps -eo pid,etimes,args | grep -E '[r]un_lin_scenario|[r]un_all_lin|[a]nsible-playbook|[c]rash_inject\.sh|[s]caleout_pairs\.py'
ls /tmp/cluster_in_use        # must not exist
```

Start long runs detached and poll the summary file; a foreground call is cut after 10 minutes:

```bash
OUT_ROOT=/tmp/lincheck/<name> setsid nohup ./run_all.sh > /tmp/<name>.log 2>&1 < /dev/null &
cut -c1-60 /tmp/lincheck/<name>/summary.txt      # one line per finished scenario
```

## 3. Running 3 -> 4

```bash
cd /users/entall/rd/ansible/lincheck

# quick profile: 300k keys, 90 s run, ~5 min per scenario
OUT_ROOT=/tmp/lincheck/<name> ./run_all_lin.sh HEALTHY S1 S2 S3 S4 S5

# 30M profile: snapshot ws30m, 3 min run, ~17 min per scenario (11 of them the post-check).
# EXTRA_ANSIBLE_ARGS replaces the script's defaults, so all of them are repeated here.
PROFILE=full YCSB_THREADS=200 OUT_ROOT=/tmp/lincheck/<name> \
  EXTRA_ANSIBLE_ARGS="-e dataset_snapshot=ws30m -e rdma_src_prereg_slots=1365 -e rdma_landing_prereg_pools=3 -e rdma_chain_ack_via_raft=yes -e rdma_backpatch_pool_size=16 -e rdma_transfer_chunk_slots=57" \
  ./run_all_lin.sh HEALTHY S1 S2 S3 S4 S5
```

- `PROFILE=full` defaults to 100 threads per client; the reference figures use 200.
- Scenarios: S1 recipient leader, S2 donor leader (during), S3 donor follower, S4 recipient
  follower, S5 both leaders (called S8 until 2026-10-05; the old S5, donor leader after its transfer, is now S5_DONOR_AFTER and not in the default list). (S6/S7 there need the
  five-replica inventory.)
- Reference result, 30M, 200 threads, fault-free (2026-10-04 campaign): 147k -> 171k ops/s,
  lowest second 130k, migration 3.9 s for 8.0 GiB. All seven scenarios pass. Report with downtime
  per scenario: `experiments/2026-10-02_scaleout_3_to_6/BIG_CAMPAIGN_2026-10-04.md`.

## 4. Running 3 -> 6

```bash
cd /users/entall/rd/ansible/experiments/custom_scaleout_3to6

OUT_ROOT=/tmp/lincheck/scaleout3to6/<name> ./run_all.sh          # HEALTHY S1 S2 S3 S4 S5 S9
OUT_ROOT=... ./run_all.sh S2 S4                                   # chosen scenarios
OUT_ROOT=... ./run_scenario.sh S6                                 # one scenario, no summary line
SCALEOUT_MODE=pipelined OUT_ROOT=... ./run_all.sh                 # next copy starts when the previous copy ends
```

- Defaults (in `env.sh`): quick profile, `workloada_lin_quick_uniform`, 400 threads per client,
  `ROUNDS=6`, sequential, one landing pool per round pre-registered, 57-slot chunks, donor
  followers warmed at prepare time.
- Scenarios: S1 recipient leader (pair A), S2 donor leader (pair A), S3 donor follower, S4
  recipient follower, S5 both leaders of pair A (called S8 until 2026-10-05), S9 donor leader of pair A + recipient leader of
  pair B. S6 (recipient host) and S7 (donor host) are defined but not in the
  default list.
- Reference result, quick profile, fault-free: ~155k -> ~305k ops/s, migration window ~12.1 s
  sequential, 17.2 GB moved (9.3 s, and 7.7 s pipelined, were measured with the follower early forward,
  removed on 2026-10-04; pipelined has not been re-measured since).
- 30M profile: `PROFILE=full YCSB_THREADS=200 OUT_ROOT=... ./run_all.sh` (~23 min per scenario).
  It restores snapshot `ws30m_3to6` (created 2026-10-05 with `dataset_create.yml`; the command is
  in that file's header). Reference, fault-free: 149k -> 266k ops/s, migration 12.4 s for 16 GiB,
  17 GB of memory left on the recipient hosts at the lowest point. Keep slots-per-round <= 510 and
  `ROUNDS` < 8 on the 62 GB hosts.
- `MERGE_THREADS` (default 8) sets the recipient leader's merge threads.

## 5. Reading the result

`$OUT_ROOT/summary.txt`, one line per run:
`<time> <scenario> q=yes rc=0 OVERALL: PASS | linearizable keys=200 VIOLATING keys=0 ... | migration=PASS crashes=PASS replicas=PASS donors=PASS bulk=PASS <dir>`

- `OVERALL` = linearizability check AND post-check. Details: `<dir>/postcheck.txt`,
  `<dir>/lincheck.txt`.
- `<dir>/logs/<host>/` holds the server logs, `<dir>/playbook.log` the driver's output,
  `<dir>/run.log` the kill time (`t_kill=`, written at the END of the run).
- Migration window: `/tmp/experiments/<run>/migration_window.txt` (line `FULL`).
- `/tmp/experiments/<run>` is overwritten by the next run of the same scenario. Save it first:
  `experiments/tools/scaleout/snap_runs.sh scaleout3to6_ <dir>` (or `lin_healthy` / `crash_s`).

## 6. Figures

```bash
E=/users/entall/rd/experiments
# 3 -> 6, all scenarios (panels for the directories present in <snap>)
python3 $E/2026-10-02_scaleout_3_to_6/plot_scaleout_ycsb.py <snap> /tmp/lincheck/scaleout3to6/<name> out.png
# 3 -> 4, all scenarios
python3 $E/2026-10-01_protocol_fixes_quick/tools/plot_all_throughput_30m.py <snap> out.png
# one run, whole duration, both clients
python3 $E/2026-10-01_protocol_fixes_quick/tools/plot_total_ycsb.py <snap>/<run> out.png "title"
# one 30M run in the style of the 3 -> 6 panels (<snap> has HEALTHY/, S1/ ...)
python3 $E/2026-10-02_scaleout_3_to_6/plot_single_30m.py <snap> <lincheck out root> out.png
```

Set `MPLCONFIGDIR` to a writable directory. Figures of record live in
`experiments/2026-10-02_scaleout_3_to_6/figures/`.

## 7. After changing code

| Changed | Deploy |
|---|---|
| `redis/src` (server) | `ansible/deploy_redis.sh` (rsync to redis1-5, kill instances, rebuild everywhere) |
| `redisraft/` (module) | `sudo ansible-playbook -i inventory_scale3to6.ini tasks/build/build_redisraft.yml` |
| `ycsb_client/` (YCSB + probe client) | `sudo ansible-playbook -i inventory_scale3to6.ini tasks/build/build_ycsb.yml` |
| `ansible/scaleout_pairs.py` (3 -> 6 driver) | nothing; it is read at the start of each run |

Keep a `*.bak_<what>` copy beside every source file before editing it, and add an entry to
`ansible/lincheck/FIXES_LOG.md`.

Acceptance after ANY server, module or client change, in this order:

1. 3 -> 4, 30M: `HEALTHY` and `S4`. Compare the fault-free throughput dip with the reference in
   section 3. The quick profile hides costs that grow with the data per slot: on 2026-10-04 two
   changes passed every quick run and failed only here (a main-thread checksum: 142k -> 18k
   during the migration; the follower early forward: S4 replicas diverged).
2. 3 -> 6 quick: the full default list.
3. 3 -> 4, 30M: the remaining scenarios.

## 8. Aborting a run

Kill by PID, never with `pkill -f <pattern>` from a shell whose own command line contains the
pattern (it kills the shell):

```bash
ps -eo pid,args | awk '/run_all_lin|run_lin_scenario|run_crash_scenario|crash_inject\.sh|ansible-playbook|scaleout_pairs\.py|LinHistoryClient|PS=1365; NR=/ && !/awk/'
sudo kill <pids>;  rm -f /tmp/cluster_in_use
```

A crash injector left running kills a node in the NEXT run. The 3 -> 4 migration loop
(`PS=1365; NR=`) left running re-drives migrations into the next run.

## 9. Things that look like bugs and are not (or are known)

- YCSB output files contain every status line twice. Count each second once; summing all lines
  doubles the throughput.
- redis1 has half the memory speed of the other hosts: a donor leader there copies ~15% slower.
- The experiment network drops RoCE packets now and then (no flow control on the switch). A
  transfer that suddenly takes seconds: compare `rx_discards_phy` and the hw_counters
  `out_of_sequence`, `local_ack_timeout_err` before and after
  (`experiments/tools/scaleout/nicsnap.sh`, `nicdiff.py`).
- The RDMA ack timeout cannot be made shorter than ~0.54 s per try on these NICs.
- 3 -> 6 quick profile: transfers take 0.4 s and the injector needs ~0.55 s from marker to kill,
  so a kill usually lands between the victim pair's rounds, not mid-copy. The mid-transfer
  recovery paths are exercised by the 3 -> 4 30M runs (rounds of 1.25 s), not by 3 -> 6 quick.
- Do not warm a donor (`RDMA MIGRATE-WARM`) under client load: 4 s with the group frozen.
- The workload files live on ycsb0/ycsb1 under `/users/entall/rd/workloads/`, not in the repo.

## 10. Analysis helpers (`experiments/tools/scaleout/`)

| Script | Use |
|---|---|
| `snap_runs.sh <prefix> <dir>` | save YCSB output + windows before the next run overwrites them |
| `tl.py <run dir> <seconds> '<regex>' [<seconds before>]` | merged timeline of all server logs around the kill |
| `recov2.py <out root> S1 S2 ...` | per transfer: wait, setup, copy, commit (3 -> 6) |
| `recov.py <out root> <snap>` | per scenario: elections, pauses, client dip (3 -> 6) |
| `tp.py <snap> <run dir> S2` | per-second client throughput around the kill (3 -> 6) |
| `phases.py <logs dir>`, `overlap.py <logs dir>` | fault-free phase totals; do copies overlap |
| `nicsnap.sh <file>`, `nicdiff.py <a> <b>` | NIC drop counters before / after a run |
| `dumpwatch.py <out dir> <victim ip> <port>` | thread dumps of the YCSB JVM 1.4-4.4 s after a kill |

## 11. Open items (2026-10-06)

- Client outage of 2-3 s when a leader is killed: the YCSB socket timeout is 10 s (`ycsb_timeout`),
  so client threads wait until the killed server's connections are reset (~2.8 s).
- 3 -> 4 S5: the round is re-sent at ~+2.9 s although the new donor leader is ready at +1.2 s; the
  delay is in the driver loop of `reshard_cluster_rdma_v2_orchestrated_nround.yml`, not yet traced.
- Parallel follower check (`rdma_peer_probe_grace_ms=50`) makes S1's round recover 0.8 s sooner but
  gives S4 a 1 s dip; cause unknown, so it is off.
- 3 -> 6 dips after a failover: S1 (30M only), S2 (two donor leaders on redis1, the slow-memory
  host), S5 and S9 (several transfers into one host). Fault-free 3 -> 6 steps down once (249k ->
  237k at +10 s).
- 0.2-0.25 s between "chain established" and the first replicated block in every round.
- One run per scenario; nothing of this week's work is committed in git.
