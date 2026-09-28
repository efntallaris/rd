# Copy-out removed — adopt-in-place is now the only path (30M, 6-redis/2-client)

Regression check after deleting the copy-out merge path and the
`cluster-rdma-adopt-in-place` config flag: adopt-in-place is now unconditional
(`redis/src/cluster_rdma.c`, `config.c`, `server.h`). This run carries NO
`-e rdma_adopt_in_place=...` flag — the flag no longer exists.

Config: n_rounds=2, rdma_chain_pipeline=yes, rdma_chain_xsession=yes, rdma_async_apply=yes,
rdma_transfer_chunk_slots=171 (=> 4 chunks/session). Workload: workloada_prod_30m_run10min
(wa 50/50). experiment_name=prod_adopt_noflag_a.

## INDEX-UPDATE per donor-session (first DONE-SLOTS-CHUNK seq=0 -> backpatch-merge batch DONE)
| session | copy-out removed (this run) | prior adopt-in-place (`prod_adopt_a`) |
|---------|-----------------------------|----------------------------------------|
| 1       | 0.56 s                      | 0.60 s                                 |
| 2       | 0.38 s                      | 0.40 s                                 |
| 3       | 0.38 s                      | 0.40 s                                 |
| 4       | 0.38 s                      | 0.30 s                                 |
| 5       | 0.38 s                      | 0.40 s                                 |
| 6       | 0.38 s                      | 0.40 s                                 |

Flat at ~0.38 s/session (~150 ms/chunk) — unchanged from the flagged adopt-in-place
run. Migration window = 3.76 s.

## Transferred volume (leader -> F1 chain write, summed over sessions)
4,098 blocks (2 MiB) = 8.00 GiB (8,594,128,896 bytes) over 6 donor sessions.

## Health
- Load: [INSERT] Operations = 30,000,000; failed=0 on all 9 hosts.
- Aggregate throughput: ~159k ops/s (2x ~79.5k). READ 339 us, UPDATE 897 us avg.
- write-MOVED storm: 728 total, all to sg4 nodes (10.10.1.4/.5/.6), zero to donors
  (10.10.1.1/.2/.3). NOTE: redis5 (.6) is now an sg4 follower in place of the old
  ycsb1 (.7), so redirects target .4/.5/.6.
- One stray [READ] Return=ERROR=1 (one read of ~millions; prior run logged zero).

## n_rounds=1 variant (prod_adopt_noflag_a_1round)
Same config/workload but `-e n_rounds=1`: all slots move in ONE wave, so the
migration is 3 donor-sessions (one per donor) instead of 6. Each session now
carries its donor's full share, so per-session INDEX-UPDATE is ~2x the 2-round
value — but total data moved is unchanged.

| session | n_rounds=1 INDEX-UPDATE |
|---------|-------------------------|
| 1       | 1.08 s                  |
| 2       | 0.89 s                  |
| 3       | 0.88 s                  |

- Migration window = 3.33 s (vs 3.76 s for 2 rounds).
- Transferred: 4,097 blocks = 8.00 GiB (8,592,031,744 bytes) over 3 sessions.
- Load 30,000,000; failed=0; ~154k ops/s aggregate (2x ~76.8k); READ 349 us,
  UPDATE 930 us; write-MOVED 495 total, all to sg4 (.4/.5/.6), zero to donors.

Files: prod_adopt_noflag_a_1round_gantt.png, prod_adopt_noflag_a_1round_ycsb.png.

## Takeaway
Removing the copy-out path did not regress migration: with adopt-in-place as the
sole merge path, INDEX-UPDATE stays flat per session and post-migration throughput
recovers cleanly after the FLIP — at both n_rounds=2 (~0.38 s/session x 6) and
n_rounds=1 (~0.9 s/session x 3). Total data moved (~8 GiB) is identical; rounds
just trade session count against per-session size.

Files: prod_adopt_noflag_a_gantt.png, prod_adopt_noflag_a_ycsb.png.
