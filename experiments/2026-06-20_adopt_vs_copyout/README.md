# Production index-update measurement — adopt-in-place vs copy-out (30M, 6-redis/2-client)

Config: n_rounds=2, rdma_chain_pipeline=yes, rdma_chain_xsession=yes, rdma_async_apply=yes,
rdma_transfer_chunk_slots=171 (=> 4 chunks/session, 171 slots/chunk). adopt via
`-e rdma_adopt_in_place=yes`. Donor EVICT disabled.

## INDEX-UPDATE per donor-session (first DONE-SLOTS-CHUNK seq=0 -> backpatch-merge batch DONE)
| session | prod_adopt_a (wa 50/50) | prod_adopt_b (wb 95/5) | copy-out baseline (wa) |
|---------|-------------------------|------------------------|------------------------|
| 1       | 0.60 s                  | 0.50 s                 | 0.50 s                 |
| 2       | 0.40 s                  | 0.40 s                 | 0.40 s                 |
| 3       | 0.40 s                  | 0.40 s                 | 1.00 s                 |
| 4       | 0.30 s                  | 0.30 s                 | 10.70 s                |
| 5       | 0.40 s                  | 0.40 s                 | 16.70 s                |
| 6       | 0.40 s                  | 0.40 s                 | 16.70 s                |

## Per-chunk PERCHUNK-BACKPATCH cadence
adopt: ~126 ms/chunk (uniform). copy-out: ~126 ms nominal but with multi-second stalls when the merge drain blocks the next chunk.

## Health
- Both adopt runs: load [INSERT] Operations = 30,000,000; zero YCSB read misses; failed=0; no crashes.
- Aggregate throughput: wa ~156k ops/s (2x ~78k), wb ~262k ops/s (2x ~131k).

## Transferred volume (leader -> F1 chain write, summed over sessions)
| run                | blocks (2 MiB) | size    |
|--------------------|----------------|---------|
| prod_adopt_a       | 4,098          | 8.00 GiB |
| prod_adopt_b       | 4,098          | 8.00 GiB |
| copy-out baseline  | 4,098          | 8.00 GiB |

Identical across runs (same 30M dataset): 8,594,128,896 bytes over 6 donor sessions.

## Takeaway
adopt-in-place holds INDEX-UPDATE flat at ~0.4 s/session (~126 ms/chunk) regardless of merge load
(it indexes kvobjs in place instead of memcpy-ing them out), where copy-out scales with keys copied
and degrades to ~16.7 s/session at round 2.

Files: prod_adopt_a_gantt.png, prod_adopt_a_ycsb.png, prod_adopt_b_gantt.png, prod_adopt_b_ycsb.png,
copyout_baseline_gantt.png.
