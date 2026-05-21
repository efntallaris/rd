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

**Throughput went up modestly: 370K → 395K (+7%).** Bigger gains would
require the redis servers to actually be the bottleneck — they're at
~5% CPU. The YCSB workload is **closed-loop with 200 threads × ~500µs
latency = ~400K ops/sec ceiling**. Adding a 4th node trims latency
slightly but doesn't lift the closed-loop cap. To prove cluster scaling,
crank YCSB threads to 600+ or use an open-loop client.

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
