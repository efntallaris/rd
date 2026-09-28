# Throughput rise after migration (2026-07-12)

**Question:** once sg4 takes its share of slots and ownership is consistent, does aggregate
throughput actually rise?

**Result:** 3 shard groups ≈153K ops/s → 4 shard groups ≈197K ops/s, **+29%**, all four leaders
balanced near 100% CPU (3M keys, 50/50, 200 client threads). The "rise3m" campaign in commit
`d9beb430` shows +23–29% across HEALTHY/S1/S3/S4.

Root cause of the earlier flat traces: sg4 advertised the full keyspace (0–16383), so the
cluster-wide slot map was inconsistent and clients under-routed to it. Fix: ownership reconcile
across all shard groups (`ansible/reconcile_ownership.py`) + clients trust only donor handover
reports.

- `figures/throughput_increase.png` — the +29% result with the migration window and leader CPU.
- `figures/throughput_debug.png`, `figures/ownership_bug.png` — the inconsistent-ownership trace.
- `logs/` — empty: the source runs were not kept. Re-run a HEALTHY reshard at 3M and fetch it
  with `experiments/tools/fetch_logs.sh`, then `experiments/tools/make_figures.sh`.
