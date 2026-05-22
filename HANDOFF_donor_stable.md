# Donor STABLE-at-DONE — handoff (2026-05-20)

## What was done

Implemented the "let MOVED handle routing after DONE" plan from
`/users/entall/.claude/plans/no-writes-should-land-resilient-fox.md`,
in the simplified **donor-only** form (recipient changes were tried and
reverted — see Crashes below).

**Change**: at the end of `migrationWorker` in
[redis/src/cluster_rdma.c](redis/src/cluster_rdma.c#L2868) (after the
BACKPATCH poll loop confirms `done = 1`), the donor flips each migrated
slot's `slot_mig_state[]` back to `STABLE` and clears `slot_peer_endpoint[]`.
Recipient still transitions to STABLE at RECV-FLIP (unchanged).

State walk per slot:

| Side | PREP | FLIP | TRANSFER | BACKPATCH | **DONE (new)** |
|---|---|---|---|---|---|
| Donor | STABLE → MIGRATING | MIGRATED | MIGRATED | MIGRATED | **STABLE** |
| Recipient | MIGRATING | **STABLE** (at FLIP RPC) | STABLE | STABLE | STABLE |

The donor's `cluster.c:1326` read-bypass returns `myself` only when
`slot_mig_state != STABLE`. Once donor goes STABLE post-DONE, the bypass
stops firing → reads return MOVED → JedisCluster refreshes → all traffic
for the migrated slots flows to the recipient.

## Test result

Ran `ansible/run_workloada_concurrent_with_mpstat.sh` (10M records, 20M
ops, 200 YCSB threads, 3 migrations × 1365 slots each, 10s dwells).

- **Mig #1 (redis0 → redis3):** OK, ~4s
- **Mig #2 (redis1 → redis3):** OK, ~4s
- **Mig #3 (redis2 → redis3):** FAILED — "recipient backpatch timed out
  after 60s of polling"

Logs preserved at `/tmp/preserved_logs_conc_20260520_195823/`.
Plots saved to [ansible/plots_20260520_donor_stable/](ansible/plots_20260520_donor_stable/):
`ycsb_timeseries.png`, `resources_redis{0,1,2,3}.png`. There's also a
combined view at
[ansible/cpu_net_tp_lat_donor_stable.png](ansible/cpu_net_tp_lat_donor_stable.png).

### What the plots show

**Routing did shift to redis3** (the goal of the plan):
- `resources_redis3.png`: recipient cluster-NIC climbs 0 → 3 MB/s after
  Mig #1, 3 → 8 MB/s after Mig #2.
- Pre-PR baseline (from the prior run): redis3 sat at ~5 MB/s the whole
  time, while donors hogged ~19 MB/s each.

**Mig #1 MOVED storm is visible** as the deep dip in `ycsb_timeseries.png`:
369K → 21K → **0 → 0** → 14K → 148K → 363K (6s deep dip). It's the
**first-ever** JedisCluster slot-map cache invalidation + first-ever
connection-pool init to redis3. Mig #2 and Mig #3 dips are shallow
(2-3s, ~277K trough) because the cache and pool are already warm.

**Throughput went up modestly: 370K → 395K (+7%).** See "Throughput
analysis" section below for why this is expected, not a regression.

## Plot reading

### `ycsb_timeseries.png` — YCSB throughput + latency

- **M1** (4.6s band, t≈11-15s): throughput plunges to 100K then to 0
  (the MOVED storm). Latency spikes proportionally.
- **M2** (4.9s band, t≈31-35s): small dip from ~400K → ~300K, fully
  recovered by t=35s.
- **M3 not drawn** because the script only adds bands for migrations
  with a `RDMA MIGRATE worker: DONE n_slots=` log line — redis2's
  Mig #3 timed out before reaching DONE.
- Total migration time on the band: **25.1s** (spans both serial
  migrations, including the inter-migration dwell).

### Per-host cluster-NIC traffic

- `resources_redis0.png` (donor #1): steady ~18 MB/s before, dips to 0
  during M1 band (donor busy with RDMA transfer + YCSB clients in
  MOVED storm), recovers to ~18 MB/s after. **No drop after migration**
  — redis0 still owns slots 1365..4095, so most of its YCSB load
  remains.
- `resources_redis1.png` (donor #2): same shape, dip during M2 transfer
  (t≈31-35), recovers to ~17 MB/s.
- `resources_redis2.png` (donor #3): ~17-18 MB/s; dips during transfer
  windows for the other migrations (clients refreshing). Mig #3 band
  missing because it never finished.
- `resources_redis3.png` (recipient): **the key change** — climbs from
  ~0 MB/s → ~3 MB/s after M1 → ~8 MB/s after M2. Pre-PR baseline had
  redis3 stuck near ~5 MB/s the whole time while donors hogged ~19 MB/s
  each. Now traffic visibly migrates onto redis3, though still
  under-loaded relative to donors at t=45s (8 vs 17-18 MB/s).

Caveats:
- Plot window stops at `PLOT_X_MAX=45`, so post-M2 steady-state is
  truncated.
- CPU panel hard to read (everyone ≤10%) — workload is network-bound
  with this load, not CPU-bound. Use the network panel as the load
  proxy.
- Disk panel empty — no iostat capture was preserved.
- Mig #3 stall means we never saw the full 3-migration shift; expected
  redis3 plateau is ~12-15 MB/s if all three migrations had completed.

## Why does only Mig #1 have a deep throughput dip?

Three things stack on Mig #1 that are already absorbed by Mig #2:

1. **JedisCluster slot-map cache invalidation is one-time.** Jedis
   2.9.0's `JedisClusterInfoCache.renewSlotCache()` takes a single
   write-lock and runs `CLUSTER SLOTS`. With 200 client threads, the
   first time any thread sees MOVED, all 200 race on that lock; one
   wins, 199 block. After that refresh, the slot map for **all 16384
   slots** is fresh — including slots that haven't migrated yet. Mig
   #2 only invalidates 1365 of 16384 slots; refresh is cheap.

2. **Connection pool to redis3 didn't exist before Mig #1.** Pre-M1,
   no client thread had opened a Jedis connection to redis3 (it owned
   no slots). On the first MOVED burst, each thread must instantiate
   a connection (TCP + AUTH + CLIENT SETNAME). 200 threads × first
   connect = synchronized burst. By Mig #2 the pool is warm.

3. **Donor read-bypass hid all reads until DONE-STABLE.** During PREP
   → TRANSFER → BACKPATCH, `cluster.c:1326` returns `myself` for reads
   on non-STABLE slots, so clients only see MOVED on writes. On a
   50/50 read/update workload, the client doesn't aggressively refresh
   until the donor transitions to STABLE at DONE. Mig #1's MOVED storm
   thus peaks right at DONE (t=15s, 0 ops/sec).

Mig #2's STABLE-flip causes the same trigger, but the cache is warm,
connections exist, and the topology delta is small. Dip is shallow.

Per-second numbers:
- Mig #1 dip: 369K → 21K → **0 → 0** → 14K → 148K → 363K (6s deep)
- Mig #2 dip: 374K → 277K → 349K → 386K (1-2s shallow)
- Mig #3 dip: 394K → 375K → 391K → **329K → 340K → 314K** (small,
  then YCSB output stops because backpatch stalled — but the dip
  itself is small)

## Throughput analysis — why no scaling after each migration

Steady-state throughput by phase (from YCSB log):

| Phase | ops/sec | Active redis nodes |
|---|---|---|
| Pre-migration (t=14-22s) | ~370K | 3 |
| Mig #1 dip (t=23-25s) | 21K → 0 | — |
| Post-Mig-1 (t=27-30s) | ~378K | 3 (redis0 STABLE, traffic shifted) |
| Mig #2 dip (t=43s) | 277K | — |
| Post-Mig-2 (t=46-54s) | ~395K | 4 |
| Steady state w/ M1+M2 done (t=55-63s) | ~393K | 4 |

Total: **370K → 395K, +7%**.

### The real bottleneck picture (two stacked caps)

There are **two ceilings** at play. Throughput is bounded by the
smaller of them.

**Cap 1: per-redis single-thread capacity.** Redis executes commands
on one main thread. mpstat shows ~5% **system-wide** CPU, but on a
32-core cloudlab box, 100% of *one* core ≈ 3% system CPU — so 5%
system is **consistent with the redis main thread being saturated**
plus background-thread overhead. We cannot conclude from `mpstat all`
that redis is idle. Pre-migration we see 3 nodes × R ≈ 370K, so
R ≈ 120-130K ops/sec per redis main thread on this hardware.

**Cap 2: YCSB closed-loop.** 200 threads × ~500µs/op ≈ 400K ceiling.
Each thread blocks waiting for reply, then issues the next.

### Per-phase predictions vs observed

Each donor keeps serving its remaining slots (it owned 4096 of 16384;
after one migration it owns 4096 - 1365 = 2731, still ~67% of original).
If donors stay saturated at R while serving fewer slots, total = sum of
saturated nodes + partial recipient.

| Phase | Per-node load model | Predicted | Observed |
|---|---|---|---|
| Pre-migration | 3 × R | ~375K | 370K |
| Post-Mig-1 | 3 × R + 0.25R (redis3 partial) | ~470K | 378K |
| Post-Mig-2 | 3 × R + 0.5R | ~440K | 395K |
| Post-Mig-3 | 4 × R | ~500K | (didn't run) |

The per-node-saturation model predicts +25% by Post-Mig-2; we see +7%.
**A second cap is binding** before we hit `4R`. Likely candidates:

1. **YCSB closed loop hits its 400K cap** as soon as we'd otherwise
   exceed it. Adding redis cores beyond that doesn't help because no
   YCSB thread is waiting on a slow server.

2. **JedisCluster sync semantics.** Jedis 2.9.0 is one in-flight
   request per connection. If the connection pool size doesn't scale
   with the 4th node, effective parallelism is unchanged.

3. **Per-command client overhead** — CRC16 + slot-table lookup + Java
   serialization happens once per op regardless of cluster size.

We can't distinguish 1/2/3 without per-core CPU sampling on each redis
host (e.g., `pidstat -p $(pidof redis-server) 1` or `top -H`) or a
different driver. **The architecture IS rebalancing traffic** (resources
plots confirm redis3 climbs 0 → 8 MB/s) — but a client-side cap is
preventing the full 4-node speedup from materializing in the YCSB
number.

### To see real scaling

To distinguish the redis-thread cap from the YCSB-client cap:
- **Run `pidstat -p <redis-pid> -u 1`** on each host during the test
  to see per-process CPU. If donors are at 100% (one-core), Cap 1 is
  binding. If donors are at <50%, Cap 2 (client) is binding.
- **Crank YCSB threads to 600+** — moves the closed-loop ceiling from
  400K → 1.2M. If redis3 then absorbs its full share and total scales
  toward 4R ≈ 500K, the client-thread cap was indeed binding.
- **Heavier per-op work** (LRANGE, bigger payloads, HMSET) — drives
  per-op latency up so per-thread throughput drops, but raises the
  per-op work the redis main thread must do, exposing per-node
  saturation more clearly.
- **Open-loop client** (memtier with `--rate-limit`) — bypass the
  closed-loop cap entirely; throughput is then bounded only by the
  redis main-thread cap.

The +7% post-M2 means the load *did* spread (we'd otherwise see no
change), but the spread didn't translate into proportional throughput
because of a client-side cap we haven't fully characterized.

## Mig #3 stall — root cause hypothesis

Backpatch worker on redis3 is NOT crashed — gdb shows it actively in
`dictFindLinkInternal` / `dictSdsCompareKV` 4 minutes after dispatch.
Just very slow.

Mig #1 backpatch: 832K applied keys in **0.7s**.
Mig #2 backpatch: 829K applied keys in **0.9s**.
Mig #3 backpatch: 0 progress reported in 60s before donor timed out.

The plan worked *too well*. With donors going STABLE after Mig #1 and #2,
JedisCluster routes **all** traffic for migrated slots to redis3. By
Mig #3 the recipient's main thread is handling 2-3x its prior steady-
state load. The lock-free backpatch worker now contends with that
traffic for cache lines, hash buckets, and dict rehash steps.

Specifically: `rdmaBackpatchSlot` at
[redis/src/cluster_rdma.c:565](redis/src/cluster_rdma.c#L565) does
`kvstoreDictExpand(slot, total_entries * 4)` to pre-size, then calls
`lookupKeyWrite` for each entry to skip duplicates. With main thread
hammering the same dicts, the lookup-skip path is slow.

## Two paths forward

1. **Backpatch fast path** — small change. In `rdmaBackpatchSlot`, skip
   `lookupKeyWrite` entirely and let `dbAdd` either succeed or no-op
   (it already has skip-if-present semantics). Costs an SDS dup that
   may get dropped, but eliminates the per-key probe against an
   actively-being-written dict.

2. **Throttle recipient during backpatch** — bigger change. While
   `recipient_backpatch_in_progress > 0`, have the recipient *refuse*
   to acknowledge its STABLE state to clients for migrating slots
   (keep the donor's bypass alive longer). This defers the MOVED storm
   until backpatch is genuinely complete.

Option 1 is the smaller change and addresses the actual bottleneck.
Recommend starting there.

## Files touched

- `redis/src/cluster_rdma.c`:
  - `migrationWorker` — added per-slot STABLE flip after BACKPATCH poll
    succeeds (donor side).
  - `rdmaReshardRecvFlipCommand` — unchanged (recipient still STABLE at FLIP).
  - BACKPATCH_DONE sites (~889, ~968) — unchanged (just Fenwick rebuild +
    counter decrement, no state transition added).

## Crashes during experimentation (for context)

Earlier attempt also deferred the recipient's MIGRATING→STABLE from
RECV-FLIP to BACKPATCH_DONE (as the plan originally specified). That
caused crashes:
- "malformed entry" in dict + segfault in `dictSetKeyAtLink+0x152`,
  ~20s after Mig #1 backpatch done.
- Reverted to donor-only STABLE. Recipient stays with the old
  STABLE-at-FLIP behavior.

The recipient-side change probably needs a different locking approach
(it broke the invariant that the recipient owns the slot the moment
RECV-FLIP runs).

## How to resume

```bash
# 1. Pull preserved logs (already on the dev box at /tmp/...)
ls /tmp/preserved_logs_conc_20260520_195823/

# 2. View the plots
xdg-open ansible/plots_20260520_donor_stable/ycsb_timeseries.png
xdg-open ansible/plots_20260520_donor_stable/resources_redis3.png

# 3. Re-run after Option-1 fix
bash ansible/run_workloada_concurrent_with_mpstat.sh
```

## Open questions / things to verify

- [ ] Does Option-1 (skip `lookupKeyWrite` in `rdmaBackpatchSlot`)
      eliminate the Mig #3 stall?
- [ ] After full 3-migration completion, does throughput hit ~400K and
      stay there (i.e., redis3 absorbing its share)?
- [ ] Data integrity check after STABLE flip: `workloadreadonly +
      dataintegrity=true` post-migration. Skipped this time because
      the run failed before reaching it.
