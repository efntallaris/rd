# Big-workload campaign, 4-5 October 2026: 3 -> 4 and 3 -> 6 on the 30M dataset

Both setups, fault-free plus every crash scenario, on the 30M-key dataset, on one build.
**Result: 14 of 14 runs pass** (linearizability on the 200 probe keys, and the post-run checks:
migration complete, no unexpected crash, replicas identical, donors consistent, no migrated key
missing).

- Load: YCSB workload A (50% reads, 50% updates, zipfian), 30M keys, 2 clients x 200 threads,
  3-minute run, migration starts about 60 s in. Plus the probe client on 200 keys.
- Build: as of FIXES_LOG entry 35. Settings: 16 merge threads, 57-slot chunks.
- Raw results: `/tmp/lincheck/big/3to4`, `/tmp/lincheck/big/3to6` (`summary.txt` in each).
- Figures: `figures/big/` next to this file.
- Each scenario was run once.

## 1. Fault-free migration

| | 3 -> 4 | 3 -> 6 |
|---|---|---|
| Slots moved | 3 x 1365 | 3 x 2730 |
| Keys moved | 7.5 million | 15 million |
| Data on the wire, donor -> recipient leader | 8.0 GiB | 16.0 GiB (17.2 GB) |
| Rounds | 1 per donor | 6 per pair (18 transfers of 455 slots) |
| Migration time | **3.9 s** | **12.4 s** |
| Average rate over the migration | 17.5 Gb/s | 11.1 Gb/s |
| Time spent copying | 3.2 s | 6.5 s |
| Throughput before -> after | 147k -> 171k ops/s | 149k -> 266k ops/s |
| Lowest second during the migration | 130k | 138k |

The copy itself runs at 22-23 Gb/s in both, which is the limit of the 25 Gb/s link at its current
packet size. What differs is the time between copies.

**3 -> 4 (3.9 s).** The three donors copy back to back (1.0-1.2 s each). After each copy the
recipient leader merges 2.5 million keys in 0.35 s and commits. Only the last donor's merge and
commit add to the total. Gantt: `figures/big/big_3to4_gantt_HEALTHY.png`.

**3 -> 6 (12.4 s).** Sequential mode: each of the 18 transfers waits for the previous one to
commit. Per transfer: setup 0.03 s, copy 0.36 s, wait for commit 0.28 s, driver 0.02 s. The commit
wait (5.0 s in total) is the follower copy finishing, the merge (0.14 s for 830k keys) and the two
Raft commits. Gantt: `figures/big/big_3to6_gantt_HEALTHY.png`.

Before this campaign the 3 -> 4 fault-free migration took 5.9 s on the same data. The merge after
each copy went from 1.4 s to 0.35 s (two global locks taken per merged key were removed from the
merge path, and it now uses 16 threads); smaller chunks removed another 0.2 s.

## 2. Results and downtime per scenario

"Downtime" below is read from the total YCSB throughput, per second: seconds under 10% of the
pre-crash level, seconds under 50%, and the first second back at 90%. Times are seconds after the
kill. Throughput figures: `figures/big/big_3to4_ycsb.png`, `figures/big/big_3to6_ycsb.png`.

### 3 -> 4

| Scenario | Result | Migration time | Under 10% | Under 50% | Back at 90% | Lowest second |
|---|---|---|---|---|---|---|
| No fault | pass | 3.9 s | 0 | 0 | - | 130k |
| S1 recipient leader | pass | 11.4 s | 1 s | 3 s | +4 s | 0 |
| S2 donor leader (during) | pass | 8.3 s | 0 | 0 | +2 s | 77k |
| S3 donor follower | pass | 4.1 s | 0 | 0 | - | 134k |
| S4 recipient follower | pass | 6.8 s | 0 | 0 | - | 154k |
| S5 donor leader (after) | pass | 5.8 s | 0 | 0 | +1 s | 61k |
| S8 both leaders | pass | 10.9 s | 2 s | 3 s | +4 s | 0 |

### 3 -> 6

| Scenario | Result | Migration time | Under 10% | Under 50% | Back at 90% | Lowest second |
|---|---|---|---|---|---|---|
| No fault | pass | 12.4 s | 0 | 0 | - | 138k |
| S1 recipient leader | pass | 14.4 s | 0 | 1 s | +2 s | 24k |
| S2 donor leader (during) | pass | 13.5 s | 1 s | 2 s | +2 s | 12k |
| S3 donor follower | pass | 12.4 s | 0 | 0 | +1 s | 118k |
| S4 recipient follower | pass | 12.5 s | 0 | 0 | - | 140k |
| S8 two leaders, one pair | pass | 18.4 s | 1 s | 1 s | +3 s | 1k |
| S9 two leaders, two pairs | pass | 18.4 s | 3 s | 4 s | +4 s | 0 |

Crashing a follower (S3, S4) causes no downtime in either setup. Every downtime comes from losing
a leader.

## 3. Why there is downtime, and where the time goes

A leader crash makes a slice of the key space unavailable. The YCSB clients are closed-loop
(400 threads, each waiting for its current operation), so within a fraction of a second most
threads are waiting on a key in that slice and total throughput falls towards zero, even though
most of the key space is still served. Downtime ends when that slice is served again.

### 3 -> 4, S1: recipient leader killed during the first donor's copy (about 3 s)

| Time after kill | Event |
|---|---|
| 0 | sg4's leader killed; sg1's round (1365 slots) is mid-copy |
| +0.34 s | New sg4 leader elected (300 ms election timeout) |
| +0.53 s | The donor's round fails (its chunk message to the dead leader is reset) |
| +0.74 s | The orchestration reports FAILED |
| +0.74 -> +2.76 s | **Nothing happens: the driver's recovery loop is in a fixed 2 s sleep** |
| +2.78 s | Driver asks the new leader what it holds (nothing), tells sg1 to recover |
| +2.80 s | sg1 re-sends the round; clients on those slots are served again as it lands |
| +3 to +4 s | Throughput back (136k, then 158k) |

Of about 2.8 s of unavailability: election 0.34 s, failure detection 0.4 s, **driver sleep 2.0 s**,
restart 0.1 s. The other two donors are then recovered the same way one after another (at +5.8 s
and +8.9 s), which is why the migration takes 11.4 s; that does not cause further downtime.

### 3 -> 4, S8: both leaders (about 3 s)

Same shape as S1. Elections at +0.40 s (sg4) and +0.72 s (sg1). The new donor leader starts
recovery by itself at +0.72 s but still has the dead recipient's address; the round is re-driven
by the driver at +2.80 s, after the same 2 s sleep. Throughput is back at +4 s.

### 3 -> 4, S2 and S5: donor leader (no downtime)

- S2: election at +0.50 s; the new donor leader finds the open round in its log and re-sends it
  at +0.52 s without waiting for the driver. Throughput dips to 77k for one second.
- S5: the round was already committed; only the election (0.57 s). One second at 61k.

### 3 -> 6, S1 and S2: one leader (1-2 s)

The 3 -> 6 setup uses a 1000 ms election timeout, and that is the downtime: the new leader is
elected at +1.16 s (S1) and +1.30 s (S2), and throughput is back at +2 s. In both runs the kill
landed between that pair's rounds, so no round had to be recovered.

### 3 -> 6, S8: both leaders of one pair (about 2 s, then two later dips)

| Time after kill | Event |
|---|---|
| 0, +0.35 s | sg4's leader, then sg1's leader killed; pair A's round is in setup |
| +1.31 s, +1.60 s | New leaders for sg4 and sg1 |
| +1.60 s | New donor leader's first recovery attempt goes to the dead recipient address and fails |
| +1.65 -> +2.45 s | Second attempt, to the new recipient leader: round re-sent and committed |
| +3 s | Throughput back (168k) |

Later, pair A's next round does not start until +6.7 s (4.2 s lost in the 3 -> 6 driver's recovery
loop, which also has fixed sleeps). That lengthens the migration (18.4 s) but is not downtime.
Throughput dips briefly at +12 s and +14 s while pair A's rounds run against the new recipient
leader.

### 3 -> 6, S9: leaders of two different pairs (about 3 s)

| Time after kill | Event |
|---|---|
| 0, +0.35 s | sg5's leader, then sg1's leader killed |
| +1.22 s | New sg5 leader |
| +1.97 s | New sg1 leader: two groups were leaderless until here |
| +2.50 -> +3.51 s | Pair B's round starts against the new sg5 leader; **its setup takes 1.0 s** instead of 0.03 s, with writes to its 455 slots already redirected |
| +4 s | Throughput back (172k) |

Elections account for 2.0 s and the slow setup for 1.0 s. The setup is slow because the donor's
pre-registered source memory belongs to its link to the old recipient leader; with the new
leader it registers again, every round (0.85-1.0 s each). That is also the cause of the repeated
dips to 120-130k every three seconds afterwards.

## 4. What would reduce the downtime

| Cause | Where | Cost today | Fix |
|---|---|---|---|
| Fixed 2 s sleep in the recovery loop | 3 -> 4 driver | 2.0 of ~2.8 s in S1 and S8 | Poll instead of sleeping |
| 1000 ms election timeout | 3 -> 6 | 1.2-2.0 s in every leader crash | Try 300 ms again (it was raised on 3 Oct when two-replica groups kept losing leaders; the build has changed a lot since) |
| Donor re-registers its source after a recipient failover, with the slots already redirected | server | 1.0 s in S9, plus later dips in S8 and S9 | Register for the new link before redirecting writes, or once for all remaining rounds |
| Fixed 3 s + 2 s sleeps in the recovery loop | 3 -> 6 driver | 4.2 s of migration time in S8 (not downtime) | Poll instead of sleeping |

None of these is done yet.

## 5. Caveats

- One run per scenario.
- 3 -> 6 crash timing: transfers take 0.4 s and the injector needs about 0.55 s from its marker to
  the kill, so in S1 and S2 the kill landed between the victim pair's rounds. The 3 -> 4 runs
  (1.0 s copies) do hit a round in flight.
- A slot is shipped as one 2 MiB block whatever it holds. With about 1830 keys of about 123
  bytes per slot, the blocks are roughly 10-15% used (estimated from the record shape; not
  measured on the 30M dataset).
- While taking stack samples with a debugger, the recipient leader was paused for longer than the
  election timeout; that run lost leadership repeatedly and then FAILED the linearizability check
  (5 keys). A leader that pauses and resumes is not one of the crash scenarios and has not been
  investigated.
- Nothing is committed in git.

## 6. Figures (`figures/big/`)

- `big_3to4_ycsb.png`, `big_3to6_ycsb.png`: throughput and latency, all scenarios.
- `big_3to4_gantt_<scenario>.png`, `big_3to6_gantt_<scenario>.png`: Gantt per scenario
  (donor setup and copy, replication to the follower, leader merge, commit, crashes, elections).
- Tools: `experiments/tools/scaleout/downtime.py` and `gantt_pairs.py`.
