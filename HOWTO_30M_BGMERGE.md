# HOWTO — AqRaft 30M reshard, background-merge + tight-chain setup (validated 2026-07-05)

Run the 30M-key reshard with the current best configuration. Everything below is
**compiled into the deployed binary** — the run command just selects the workload
shape. Branch `aqueduct_broken`. Run everything as **root** (`sudo`) from
`/users/entall/rd` on the controller (redis0).

**Baked-in binary features (all default-on / already fixed — no flags needed):**
- **Background merge** (`--rdma-merge-background`, compiled-in default **yes**):
  recipient's shadow→live merge runs on the backpatch pool workers, off the main
  event loop (main-thread merge cost ~17% → ~0.1%). Recovery t=54s → t=33s vs the
  legacy main-thread merge.
- **3 backpatch pool workers** (`rdma-backpatch-pool-size`, default **3**): parallel
  scan-blocks + add-keys-to-index threads.
- **Dict-resize FORBID during backpatch** (crash fix): prevents the concurrent
  rehash-step corruption (`dict.c:548`) that bigger chunks exposed. Makes bg-merge
  robust across chunk sizes.
- **Chain-capture decouple** (chain fix): snapshots are captured on the main thread
  at chunk-arrival, so each donor session's chain forward overlaps its own backpatch
  and the three forwards run back-to-back (no idle-wire gap on the last session).

**Workload shape (this HOWTO):** `n_rounds=1` (all QP connect + memory registration
happens BEFORE the flip, outside the migration window) + `rdma_transfer_chunk_slots=342`
(= **4 chunks / donor session**, 1365 slots/donor ÷ 342). This gives the tightest
client-visible window (COLD-EXCLUDED ~**3.45s**).

---

## 1. Run it

```bash
cd /users/entall/rd/ansible
sudo ansible-playbook -i inventory.ini \
  experiments/custom_reshard_v2_orch_raft_chunked/workload_nround.yml \
  -e redis_variant=custom -e pre_reshard_pause=20 -e n_rounds=1 \
  -e rdma_migration_peer_stagger_ms=0 \
  -e ycsb_slotpoll_ms=100 -e rdma_chain_pipeline=yes -e rdma_chain_xsession=yes \
  -e rdma_async_apply=yes -e rdma_transfer_chunk_slots=342 \
  -e '{"rdma_follower_proxy": "no"}' \
  -e redis_workload=workloada_prod_30m_run10min \
  -e experiment_name=bgm_30m_4ck
```

`rdma_merge_background` and `rdma_backpatch_pool_size` are **NOT passed** — they take
the compiled-in defaults (on, 3). Total wall time ~35 min (~22 min load + 20s pause +
migration + 10-min run + collect). Clean run = `failed=0` on every host.

### Toggling the knobs (A/B)
- Legacy main-thread merge baseline:      `-e rdma_merge_background=no`
- Different worker count (e.g. 4):        `-e rdma_backpatch_pool_size=4`
- 8 chunks/session instead of 4:          `-e rdma_transfer_chunk_slots=171`
- Two rounds (connect re-enters window):  `-e n_rounds=2`

---

## 2. Expected results (validated 2026-07-05, bgm_30m_4ck_capfix)

> NOTE: the ycsb node reuses one output file across runs, so it may contain a
> PRIOR run's block first. Always read the LAST test block:
> `s=$(grep -nE "Command line:" $f | tail -1 | cut -d: -f1); sed -n "${s},\$p" $f | grep ...`

```bash
B=/tmp/experiments/bgm_30m_4ck
grep -E "^\[INSERT\], Operations" $B/ycsb/ycsb0/tmp/ycsb_output_load_ycsb0   # 30000000
for c in ycsb0 ycsb1; do f=$B/ycsb/$c/tmp/ycsb_output_$c
  s=$(grep -nE "Command line:" "$f" | tail -1 | cut -d: -f1)
  echo "== $c =="; sed -n "${s},\$p" "$f" | grep -E "^\[OVERALL\], Throughput|^\[UPDATE\], AverageLatency|Return=ERROR"
done
```

- **Load:** `[INSERT], Operations, 30000000`, `Return=OK, 30000000`.
- **Throughput:** ~**77k/client**, ~**154k aggregate** (ycsb0 + ycsb1).
- **UPDATE latency** ~**925µs**, READ ~**350µs** (sub-ms, no collapse).
- **Errors:** ~1 READ error/client (baseline noise); **no UPDATE errors**.
  A ~4k-ops/s + thousands-of-UPDATE-errors result means the recipient CRASHED — see §5.
- **Migration window:** COLD-EXCLUDED ~**3.45s** (client-visible); FULL ~11.3s
  (the extra is sg1's full-range connect+register, **pre-flip / outside the cold
  window**, not client-visible).
- **Key integrity:** sg4 recipient DBSIZE == migrated slot keyspace, exact:
  ```bash
  sudo ssh redis3 "/users/entall/rd/redis/src/redis-cli -p 8000 DBSIZE"   # ~7,492,752
  # == bg-merge 'moved' (~7.44M) + 'skipped' (~50k, post-FLIP client don't-clobber)
  ```

### Verify the setup + fixes took effect (recipient sg4 = redis3, LIVE before teardown)
```bash
sudo ssh redis3 'L=/tmp/redis_logs/redis3_sg4.log
  grep -E "pool workers started|bgMergeInit|AqRaft bg-merge" $L | tail -3
  echo "--- crash (want 0) ---";    grep -icE "ASSERTION FAILED|dict.c:548|REDIS BUG" $L
  echo "--- restarts (want 1) ---"; grep -icE "Redis is starting" $L
  echo "--- chain back-to-back (3 forwards ~1.0s each, small gaps) ---"
  grep -E "forward FIRST-POST|pipelined forward complete" $L | grep -oE "[0-9:.]+ .*(FIRST-POST|forward complete)"'
```
Expect: `dispatcher + 3 pool workers started`, `bgMergeInit ... (rdma-merge-background on)`,
`AqRaft bg-merge: session mig_id=1 merge_done ... moved=~7.44M`, crash=0, restarts=1,
and the 3 chain FIRST-POST → complete pairs running **back-to-back** (each ~1.0s, the
last session's forward starting BEFORE its backpatch finishes = overlap restored).

### Exact byte-integrity (optional, read-only)
```bash
# add to the §1 command:  -e redis_workload=workloada_bgm_ro -e '{"rdma_reshard_debug_bytes":"yes"}'
```
Then confirm MISMATCH=0 and the recipient serves all reads (0 read errors).

---

## 3. Chunk metrics (keys, chunks, MB, transfer rate)

The migration moves ~25% of the keyspace to sg4 as fixed **2 MiB blocks** (one block per
slot; a slot needing >2 MiB gets extra blocks). The transfer is chunked: each donor ships
its owned range in `ceil(reshard_slots_per_source / rdma_transfer_chunk_slots)` chunks.

**For this HOWTO's config (30M keys, `n_rounds=1`, `rdma_transfer_chunk_slots=342`):**

| Metric | Value | How it's derived |
|---|---|---|
| Total keys in cluster | 30,000,000 | `recordcount` |
| Slots (fixed) | 16,384 | Redis cluster slots |
| **Keys per slot** | **~1,830** | 30,000,000 ÷ 16,384 |
| Donors (source shard groups) | 3 (sg1/sg2/sg3) | topology |
| Slots migrated per donor | 1,365 | `reshard_slots_per_source` = 25% range ÷ 3 |
| **Chunks per donor session** | **4** | ceil(1365 ÷ 342) → 342+342+342+339 |
| **Total chunks** | **12** | 1 round × 3 donors × 4 |
| Slots per chunk | 342 (last 339) | `rdma_transfer_chunk_slots` |
| **MB per chunk (wire)** | **684 MiB** | 342 slots × 2 MiB (last chunk 678 MiB) |
| **Keys per chunk** | **~626,000** | 342 slots × ~1,830 keys/slot |
| Keys per donor session | ~2.50M | 1,365 slots × ~1,830 |
| **Total keys migrated** | **~7.49M** | 4,095 slots × ~1,830 (= 25% of 30M) |
| **Total data on wire** | **8.00 GiB** | 4,097 blocks × 2 MiB |
| Inter-chunk cadence | ~252 ms | measured (DONE-SLOTS-CHUNK timestamps) |
| **Transfer rate per chunk / stream** | **~22.7 Gbit/s** | 684 MiB ÷ 0.252 s = 2.64 GiB/s per donor |
| Aggregate transfer rate | ~11–13 Gbit/s | 8 GiB over the transfer span (donors staggered) |

**How the knobs scale these:**
- `rdma_transfer_chunk_slots=N` → chunks/session = ceil(1365 ÷ N); MB/chunk = N × 2 MiB;
  keys/chunk ≈ N × 1,830. (171 → 8 chunks × 342 MiB × ~313k keys; 342 → 4 chunks × 684 MiB
  × ~626k keys.) **Per-stream Gbit/s is unchanged** — bigger chunks arrive proportionally
  less often. Chunk size is a pipeline-granularity knob, not a bandwidth knob.
- `n_rounds=R` → total chunks = R × 3 × ceil((1365/R) ÷ N); each round migrates 1365/R
  slots/donor.
- Different `recordcount` → keys/slot = recordcount ÷ 16,384; keys/chunk scales with it
  (block count/chunk can rise if a slot exceeds 2 MiB and needs a 2nd block).

**Extract the ACTUAL per-run numbers from the recipient (redis3), LIVE before teardown:**
```bash
sudo ssh redis3 'L=/tmp/redis_logs/redis3_sg4.log
  echo "chunk sizes (n_slots per chunk):"
  grep -aoE "seq=[0-9]+ n_slots=[0-9]+" $L | sort | uniq -c
  echo "total chunks:"; grep -c "DONE-SLOTS-CHUNK" $L
  echo "keys the workers scanned+indexed (moved+skipped == DBSIZE):"
  grep "AqRaft bg-merge" $L | tail -1 | grep -oE "moved=[0-9]+ skipped=[0-9]+"
  echo "per-chunk cadence (one donor session, ms apart):"
  grep "DONE-SLOTS-CHUNK.*08000" $L | grep -oE "[0-9:.]+ .*seq=[0-9]+"'
# keys/chunk = (moved+skipped) / total_chunks ;  MB/chunk = slots_per_chunk × 2 MiB
# Gbit/s per stream = (MB/chunk × 8 / 1024) / (cadence_seconds)
```

> The bg-merge `moved + skipped` total (7,492,752) equals the recipient DBSIZE exactly,
> and ÷ 12 chunks = ~624k keys/chunk — the direct validation of the ~626k estimate.
> `moved` = keys the workers adopted into the live index; `skipped` = post-FLIP client
> writes the merge correctly did NOT clobber (live won).

---

## 4. Plots

```bash
cd /users/entall/rd
OUT=/tmp/plots_bgm; mkdir -p $OUT
python3 plot_phase_gantt.py /tmp/experiments/bgm_30m_4ck $OUT/gantt.png
# ycsb timeseries: extract the LAST run block first (stale prior-run guard), then plot
```
The gantt shows CONNECT + REGISTER **before** the "migration time" arrow (pre-flip),
INDEX-UPDATE uniform ~750ms/session, and CHAIN-REPLICATION 1.1/2.1/3.1 **back-to-back**
(no gap). Reference: `ansible/bgm_30m_4ck_capfix_gantt.png`.

---

## 5. Rebuild + redeploy (only if you change source)

```bash
cd /users/entall/rd
for h in redis1 redis2 redis3 redis4 redis5; do
  sudo rsync -e "ssh -o StrictHostKeyChecking=accept-new" -a --delete redis/src/ "$h":/users/entall/rd/redis/src/
done
cd ansible && sudo ansible-playbook -i inventory.ini tasks/build/build_redis_custom.yml
```
(YCSB jar + workloads: see `HOWTO_30M.md` §3 / `aqraft-30m-run-prereqs` — ycsb1's jar
may need copying from ycsb0; the prod workload is deployed manually.)

---

## 6. Config summary + failure modes

| flag | default | effect |
|---|---|---|
| `rdma-merge-background` | **yes** | shadow→live merge on pool workers (off main thread) |
| `rdma-backpatch-pool-size` | **3** | parallel scan+index worker threads |
| `n_rounds` | 1 (this HOWTO) | single round → connect/register outside the window |
| `rdma_transfer_chunk_slots` | 342 (this HOWTO) | 4 chunks/donor session |

| symptom | cause / fix |
|---|---|
| ~4k ops/s + thousands of UPDATE errors, `redis3:8000` down, NARROW barrier "Connection refused" | recipient crashed (`dict.c:548`). The FORBID crash fix must be in the deployed binary — rebuild (§4). |
| `1 pool workers started` (wanted 3) | ansible template default not updated — pass `-e rdma_backpatch_pool_size=3` or fix `start_redisraft_instances.yml`. |
| last chain forward idle ~1s / 3.1 detached in gantt | chain-capture fix missing from binary — rebuild (§4). |
| post-mig collapse ~5k ops/s, `writeRedirect` ~tens of millions | client poller no-downgrade fix / jar not rebuilt — see `HOWTO_30M.md` §1. |

See memory `aqraft-background-merge.md` for the full fix history.
