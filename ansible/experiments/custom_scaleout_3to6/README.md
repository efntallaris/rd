# custom_scaleout_3to6 — scale from 3 shardgroups to 6 with the AqRaft migration

The single-recipient experiments (`custom_reshard_v2_orch_raft_chunked`) move a quarter
of the data from three donors into one new group, sg4. This one doubles the cluster:
three new groups join, and every donor hands **half of its range to its own recipient**.

| pair | donor (leader, followers) | migrated slots | recipient (leader, followers) | RDMA port |
|---|---|---|---|---|
| A | sg1 redis0:8000 (redis1, redis2) | 0-2729 | sg4 redis3:8000 (redis4, redis5) | 17777 |
| B | sg2 redis1:8001 (redis2, redis0) | 5461-8190 | sg5 redis4:8001 (redis5, redis3) | 17778 |
| C | sg3 redis2:8002 (redis0, redis1) | 10922-13651 | sg6 redis5:8002 (redis3, redis4) | 17779 |

Before: 3 groups of ~5461 slots on redis0/1/2. After: 6 groups of ~2731 slots on
redis0-5. Every group is a 3-replica Raft group; every host leads one group and follows
two. The fault-tolerance protocol is unchanged (TXN_START / chain replication to the
recipient's followers / INDX_UPD / TXN_DONE, roll-forward on a leader change).

**Status (2026-10-03):** on the quick profile, HEALTHY and all nine crash scenarios pass,
two repetitions each (20/20; lincheck + postcheck). Getting there took fixes in the server,
the Raft module, the client and this harness: see "What had to be fixed" at the end.

## Files

| file | what |
|---|---|
| `topology.yml` | the 6 shardgroups (`-e @…`): `role`, `recipient`, `rdma_port` per group, slots per donor |
| `../../inventory_scale3to6.ini` | redis3/4/5 all in `redis_migrate` (monitored + logs collected) |
| `workload.yml` | the run: start, load (or restore), pre-warm, YCSB, migrate, collect |
| `../../tasks/cluster/scaleout_pairs_prepare.yml`, `scaleout_pairs_migrate.yml` | the two migration steps |
| `../../scaleout_pairs.py` | the driver those tasks call (`plan`, `prepare`, `migrate`) |
| `scenarios.env` | crash scenarios S1-S9 for this topology |
| `env.sh`, `run_scenario.sh`, `run_all.sh` | run through the lincheck + crash harness |
| `dataset_create.yml` | save a loaded 6-group dataset as a snapshot (for `PROFILE=full`) |

## The migration procedure

`workload.yml` does, in order:

1. **Start** all 18 instances (`start_redisraft_instances.yml`). Recipient groups get
   the recipient flags (landing pools, no Raft snapshots), donors the donor flags, and
   every instance of a pair `--rdma-migration-port <rdma_port>`.
2. **Form the groups** (`init_redisraft_shardgroups.yml`): INIT + JOIN for all six;
   only the donors are LINKed to each other. Each recipient is its own cluster,
   configured for exactly the slot range it will receive.
3. **Load** with YCSB through the donors (or restore a snapshot).
4. **Prepare** (`scaleout_pairs.py prepare`, before the workload starts): on every donor
   replica set the RDMA port and `rdma-writeflip-spec` to *its* recipient; `RDMA
   MIGRATE-WARM` on each donor leader; `RDMA CHAIN-WARM` on each recipient leader.
5. **Start the YCSB run**, wait `pre_reshard_pause` seconds.
6. **Migrate** (`scaleout_pairs.py migrate`):
   The 2730 slots of a donor go in `ROUNDS` rounds (default 6 × 455); per round:
   1. set `rdma-reshard-migrated` (the round's slot offset) on each donor leader;
   2. `RDMA MIGRATE <recipient leader> <port> <slots>` on each donor leader — all three
      at once (`scaleout_mode=parallel`, default) or one after the other (`sequential`);
   3. poll `RDMA MIGRATE-STATUS <id>` until every donor is DONE;
   4. for a pair that did not end DONE (a leader was killed): ask the recipient's
      current leader what is durable (`RDMA MGN-RESUME-STATUS`) and re-ship the rest
      from the donor's current leader (`RDMA MGN-RECOVER donor …`), until durable;
   Then, once after the last round:
   5. wait until the recipients' background merges have drained;
   6. `RAFT.SHARDGROUP NARROW` on each donor: it keeps the upper half, its recipient
      is announced for the lower half;
   7. remove dead nodes from their groups and push the final 6-group map to all six
      leaders (`RAFT.SHARDGROUP REPLACE`), so every node and client agrees.
7. **Wait for YCSB, collect** results into `/tmp/experiments/<experiment_name>/`.

There is no orchestrator node (`RDMA MIGRATE-ALL` sends every donor to one recipient,
so it is not used): the driver talks to whichever node currently leads each group.
EVICT stays off, as in the single-recipient runs — the donors keep the migrated keys
as a frozen copy, which `postcheck.py` compares against.

Print the plan without touching the cluster:
```bash
python3 ansible/scaleout_pairs.py plan /tmp/aq_scaleout_topology.json --rounds 1
```

## Scenarios

`./run_scenario.sh <name>`; the kill is `kill -9` of the instance(s), armed on a log marker.

| | fault | armed on | killed |
|---|---|---|---|
| HEALTHY | none | – | – |
| S1 | recipient leader, mid-transfer | 4th of 12 `DONE-SLOTS-CHUNK` on redis3 sg4 (round 2 of 6) | redis3 sg4 |
| S2 | donor leader, mid-transfer | `state=TRANSFER` on redis0 sg1 | redis0 sg1 |
| S3 | donor follower | 1st `DONE-SLOTS-CHUNK` on redis3 sg4 | redis1 sg1 |
| S4 | recipient chain follower, as the forward starts | `spawned pipelined forwarder` on redis3 sg4 | redis4 sg4 |
| S5 | donor leader + recipient leader of the same pair (called S8 until 2026-10-05; the earlier S5, donor leader after its last round, was removed 2026-10-04) | as S2 | redis0 sg1 + redis3 sg4 |
| S6 | **recipient host**: A's recipient leader + a chain follower of B and of C | as S1 | redis3 sg4, sg5, sg6 |
| S7 | **donor host**: A's donor leader + a follower of B and of C | as S2 | redis0 sg1, sg2, sg3 |
| S9 | donor leader of A + recipient leader of B | as S2 | redis0 sg1 + redis4 sg5 |

S5 was removed and S6/S7 (host crashes) are out of the default list since 2026-10-04
(`./run_scenario.sh S6` / `S7` still work). S1-S5 are the single-recipient scenarios applied to pair A (same markers), so
they compare directly with `ansible/crash/scenarios.env`. S6, S7 and S9 only exist with
this topology: one host failure hits three groups in different roles, and two pairs can
fail independently. (The 5-replica scenarios S6/S7 of `crash/scenarios.env` do not apply;
the numbers are reused here.) In every scenario each group keeps 2 of its 3 replicas.

## Run

On the control node (redis0). Prerequisites are those of `ansible/lincheck/HOWTO_LINCHECK.md`
(binaries built on all hosts, YCSB client built, workload file on the YCSB hosts, cluster free).

```bash
cd /users/entall/rd/ansible/experiments/custom_scaleout_3to6
./run_scenario.sh HEALTHY                      # one run, quick profile (300k keys), 6 rounds, ~6 min
./run_scenario.sh S6
./run_all.sh                                   # HEALTHY S1..S9; summary in /tmp/lincheck/scaleout3to6/summary.txt
SCALEOUT_MODE=sequential ./run_scenario.sh HEALTHY
YCSB_THREADS=200 WORKLOAD=workloada_lin_quick_60s ./run_scenario.sh HEALTHY   # figure-quality quick run
```

Each run ends with `OVERALL: PASS/FAIL` = lincheck (history of 200 test keys in the three
migrated ranges) + `postcheck.py` (migration durable on every recipient leader, no crash
signatures, each recipient group's replicas identical, donors agree, no migrated key lost).
Results: `$OUT_ROOT/<scenario>_q<yes|no>_<ts>/` and `/tmp/experiments/scaleout3to6_<lin_healthy|crash_sN>/`.

`PROFILE=full` needs a 6-group snapshot first (the `ws30m` snapshot has no sg5/sg6):
see the command at the top of `dataset_create.yml`, then `PROFILE=full ./run_scenario.sh HEALTHY`.

Options: everything `run_lin_scenario.sh` takes, plus `SCALEOUT_MODE`,
`SCALEOUT_EXTRA_ARGS` (more `-e` flags, e.g. `-e scaleout_warm_skip=sg1`), `DATASET_SNAPSHOT`.

**Aborting:** as in `HOWTO_LINCHECK.md`, and also kill the driver, which outlives a
killed `ansible-playbook` and would keep re-driving into the next cluster:
`ps -eo pid,args | awk '/scaleout_pairs\.py/ && !/awk/'`.

## What differs from the single-recipient setup

- **RDMA ports.** A chain follower listens on its `rdma-migration-port` + 1000. With one
  recipient group per host that default (17777) is fine; with three per host they would
  collide, so each group has its own `rdma_port` in `topology.yml`, set at start-up.
- **Sizes and rounds.** 2730 slots per donor instead of 1365. Donors:
  `rdma_src_prereg_slots=2730`, ~5.7 GB per instance, 17 GB per host. Recipients: every
  instance registers 8 chain pools, and the leader up to 8 landing pools, each sized for
  one round's slots, and a host runs three instances — about 34 × slots-per-round × 2 MiB
  per host. A single round of 2730 slots needs > 100 GB and took redis3/4/5 (62 GB) down
  on 2026-10-02. Hence `ROUNDS=6` (455 slots per round, ~32 GB per host). Keep
  slots-per-round ≤ 510 and `ROUNDS` < 8 (the pools are not reused within a migration).
  For a 30M-key run add the data: ~15M keys per recipient host after the migration.
- **`crash/verdict.sh`** only looks at redis3/sg4 (pair A). Its block in `playbook.log`
  is informative; the verdict of a run is `OVERALL`.
- **Figures.** `plot_phase_gantt.py` and `plot_full_run.py` read redis3/sg4 only; use
  `migration_window.py` and `plot_total_ycsb.py` (see `experiments/2026-10-02_scaleout_3_to_6/`).

## What had to be fixed

Details, one entry per fix, with the backup taken before each: `ansible/lincheck/FIXES_LOG.md`
(entries of 2026-10-02 and 2026-10-02/03). In short:

- **Harness / configuration.** 6 rounds (memory); each recipient configured for only its own
  range; Raft timing 1000 / 100 ms in `topology.yml` (see there); the driver treats a donor that
  answers nothing as down and re-drives a round that stays pending after its donor stopped.
- **Raft module.** A second NARROW to the same recipient group left the earlier round's slots
  pointing at freed memory (crash, lost ownership).
- **Client.** After losing a donor leader it accepted a recipient's ownership claim for slots of
  rounds not migrated yet (stale reads).
- **Server, chain replication.** Chain listener on a former leader; send-completion accounting
  shared between sessions (followers got empty blocks); failed RDMA connections reused forever,
  on the leader, on followers and on the donor link; the donor announcing a chunk whose writes
  had failed (keys lost silently); a live follower that was routed around never caught up; the
  accept loop exiting on a signal.
- **Server, merge.** Key-count rebuild racing deletes (assertion); the follower's background merge
  without the dict-resize hold (assertion); the promotion merge stalling the new leader's main
  thread; a leftover merge re-installing keys deleted after the session closed.

## Still open

- **RDMA transport errors are tolerated, not explained.** Between the recipient hosts, RDMA
  writes occasionally fail ("transport retry counter exceeded") or take seconds, and TCP
  heartbeats are delayed by hundreds of ms, while several transfers run at once. Every case seen
  now recovers, but the cause (three pairs sharing the NICs?) is not established.
  `SCALEOUT_MODE=sequential` has not been tried.
- **The leader's post-commit re-send** (catch-up when no follower can serve a lacking one) is in
  place but has not been observed to complete in a run.
- **Raft timing.** 1000 / 100 ms here against 300 / 50 ms elsewhere: failure detection takes
  about 1-2 s. With 300 ms the two-replica groups left by a host crash were unstable.
- **`PROFILE=full` (30M keys)** has not been run: it needs a 6-group snapshot
  (`dataset_create.yml`) and a memory check (~26 GB of pools per recipient host plus the data).
- Two repetitions per scenario is a small sample for races this rare; S1 and S9 each failed about
  one run in three before the last fixes.
