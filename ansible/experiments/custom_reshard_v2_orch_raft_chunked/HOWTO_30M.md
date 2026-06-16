# HOWTO — AqRaft 30M-key reshard run (scaling-correct)

How to load **30,000,000** KV pairs and run the perchunk RDMA migration so the
post-migration throughput **scales** (~104k → ~124k ops/s) instead of collapsing
to ~5k. Branch: `aqueduct_broken`. Run everything as **root** (`sudo`) from
`/users/entall/rd` on the controller (redis0).

> The 30M run only scales with the **client poller no-downgrade fix** + a couple
> of orchestration fixes below. Without them, 10M scales but 30M collapses to ~5k
> (a write-MOVED storm). All fixes are already in the tree — this doc is the
> recipe + why each piece matters.

---

## 0. Why 30M is different from 10M

At 30M each donor holds ~10M keys and a much larger raft log; the migration moves
~25% of slots to sg4. The collapse at 30M is **NOT** a server bottleneck (donor
& sg4 leaders each sustain ~50k SET/s). It was a **YCSB-client write-MOVED storm**:
for migrated slots the background CLUSTER-SLOTS poller kept overwriting the
client's write target (`SHARED_PEER`) from the **sg4 leader** back to a **stale
donor** (a donor's post-NARROW CLUSTER SLOTS still lists itself as owner of the
migrated slot). Every write then routed to the donor and `-MOVED`-stormed to sg4
(~33M redirects → ~5k ops/s). Reads stayed fast (served locally), writes hit ~18ms.

**Fix:** the poller never *downgrades* a recipient `SHARED_PEER` to a donor. Storm
33M → ~900, throughput 5k → ~124k. See `RedisClient.java` (`curIsRecipient` /
"never DOWNGRADE").

---

## 1. Required fixes (verify they're present)

```bash
cd /users/entall/rd
# (a) Client poller no-downgrade scaling fix  -> expect >= 3
sudo grep -c "curIsRecipient" ycsb_client/redis/src/main/java/site/ycsb/db/RedisClient.java
# (b) Load phase not capped by maxexecutiontime -> expect 1
grep -c "maxexecutiontime=0" ansible/tasks/ycsb/load_ycsb.yml
```

- **(a) Poller no-downgrade** (`RedisClient.java`): the scaling fix. Also present:
  retry budget 200→1500 + survive-on-exhaust (worker threads ride out a 30M-scale
  slot flip instead of dying), and a write-leader-pin on `-MOVED`.
- **(b) Load not time-capped** (`load_ycsb.yml` adds `-p maxexecutiontime=0`):
  the workload sets `maxexecutiontime=600`, which otherwise **truncates the LOAD**
  to ~13.8M of 30M. The override makes the load always insert the full `recordcount`.

---

## 2. Workload file

`workloads/workloada_prod_30m_run10min` (already deployed to ycsb0/ycsb1):

```
recordcount=30000000        # 30M KV pairs (YCSB default value = 10 fields x 100B = ~1KB)
operationcount=200000000    # high so the timed run never exhausts its op budget
maxexecutiontime=600        # 10-minute RUN (the LOAD ignores this via the (b) fix)
requestdistribution=zipfian
readproportion=0.5 / updateproportion=0.5
```

To recreate / re-deploy it:
```bash
# edit ycsb_client/workloads/workloada_prod_30m_run10min, then:
sudo scp ycsb_client/workloads/workloada_prod_30m_run10min ycsb0:/users/entall/rd/workloads/
sudo scp ycsb_client/workloads/workloada_prod_30m_run10min ycsb1:/users/entall/rd/workloads/
```

---

## 3. Build & deploy (only if you changed source)

```bash
cd /users/entall/rd
# redis (only if redis/src changed): push to every peer, then build everywhere
for h in redis1 redis2 redis3 redis4 ycsb1; do
  sudo rsync -e "ssh -o StrictHostKeyChecking=accept-new" -a redis/src/ "$h":/users/entall/rd/redis/src/
done
cd ansible && sudo ansible-playbook -i inventory.ini tasks/build/build_redis_custom.yml

# YCSB client (only if RedisClient.java changed): rsync + maven build to ycsb0
sudo ansible-playbook -i inventory.ini tasks/build/build_ycsb.yml
# verify the scaling fix landed in the jar:
sudo ssh ycsb0 "unzip -p /users/entall/ycsb_client/redis-binding/lib/redis-binding-0.18.0-SNAPSHOT.jar \
  site/ycsb/db/RedisClient.class | strings | grep -c DONOR_HOSTS"   # expect >= 1
```

---

## 4. Run the 30M experiment

```bash
cd /users/entall/rd/ansible
sudo ansible-playbook -i inventory.ini \
  experiments/custom_reshard_v2_orch_raft_chunked/workload_nround.yml \
  -e redis_variant=custom -e pre_reshard_pause=20 -e n_rounds=2 \
  -e rdma_backpatch_pool_size=4 -e rdma_migration_peer_stagger_ms=0 \
  -e ycsb_slotpoll_ms=100 -e rdma_chain_pipeline=yes -e rdma_chain_xsession=yes \
  -e rdma_async_apply=yes -e rdma_transfer_chunk_slots=171 \
  -e '{"rdma_follower_proxy": "no"}' \
  -e redis_workload=workloada_prod_30m_run10min \
  -e experiment_name=perchunk_30m_workloada
```

Total wall time ~35 min: ~22 min load (30M inserts) + 20s pause + ~12s migration +
10-min timed run + collect. A clean run ends `failed=0` on every host.

### Flags that matter at 30M
| flag | why |
|---|---|
| `-e '{"rdma_follower_proxy": "no"}'` | JSON form forces a real boolean. With the client fix this isn't strictly required, but keeps follower writes honest (`-MOVED` to leader instead of silent slow proxy). **Gotcha:** `-e key=no` passes the string `"no"` which Jinja treats as truthy — always use the JSON `-e '{...}'` form for booleans. |
| `rdma_async_apply=yes` | commit migration before the merge drains (keeps the window short). |
| `rdma_transfer_chunk_slots=171` | 4 chunks/donor-session (clean per-chunk Gantt markers); migration window is the same with the default 32. |
| snapshots **off** (default) | do NOT enable raft snapshots at 30M — the ~45s RDB fork stalls trigger leader elections that break the migration's NARROW. Leave `rdma_disable_raft_snapshots` at its default (true). |

---

## 5. Expected results (success criteria)

```bash
B=/tmp/experiments/perchunk_30m_workloada
YO=$B/ycsb/ycsb0/tmp/ycsb_output_ycsb0
grep -E "^\[INSERT\], Operations" $B/ycsb/ycsb0/tmp/ycsb_output_load_ycsb0   # 30000000
grep -E "^\[OVERALL\], Throughput|^\[UPDATE\], AverageLatency" "$YO"
# write-MOVED storm (should be ~hundreds, NOT tens of millions):
grep "AQRAFT-INSTR" "$YO" | tail -1 | grep -oE "writeRedirect:.*"
```

- **Load:** `[INSERT], Operations, 30000000`, `Return=OK, 30000000`.
- **Post-migration throughput:** ~**124k ops/s** (rises from ~104k pre-migration).
- **UPDATE latency:** ~**590µs** (NOT ~7–18ms).
- **Write-MOVED storm:** ~**900 total** (NOT ~33M). All redirects to sg4 nodes
  (10.10.1.4/.5/.7), zero to donor hosts (10.10.1.1/.2/.3).
- **Throughput shape:** flat ~124k for the full run, except a brief ~6s stall at
  the **NARROW** hand-off (~t=56s) when donors surrender slots and the client
  re-resolves — expected, self-corrects.

---

## 6. Plots

```bash
cd /users/entall/rd
OUT=/tmp/plots_30m; mkdir -p $OUT
python3 plot_ycsb_timeseries.py /tmp/experiments/perchunk_30m_workloada --output $OUT/ycsb_30m.png
python3 plot_phase_gantt.py     /tmp/experiments/perchunk_30m_workloada $OUT/gantt_30m.png
```

---

## 7. Verify byte-integrity (optional)

Add `-e '{"rdma_reshard_debug_bytes": "yes"}'` to log per-slot CRCs. With a
**write** workload the recipient legitimately has **≥** the donor snapshot (client
writes land post-FLIP), so per-slot `RCV bytes >= SRC bytes` (never fewer) is the
correctness check — not exact CRC equality. For an exact byte-for-byte check use a
**read-only** workload (`readproportion=1.0`): then `SRC2==RCV` matches 4095/4095,
MISMATCH=0.

---

## 8. Tunable knobs added this work

- `--cluster-rdma-merge-keys-per-tick` (default 512): keys copied out of the
  landing pool per `mergeBackpatchTick` (main thread). Higher drains the per-key
  copy-out faster but lengthens main-thread stalls. NOTE: raising it does **not**
  reliably make every session's `applied` non-zero — the recipient `pool-free`
  drain (copy-out fully completing for every session) is still WIP, which is why
  sg4 carries elevated post-migration RSS.

---

## 9. Common failure modes

| symptom | cause / fix |
|---|---|
| Post-mig throughput ~5k, writes ~18ms, `writeRedirect` ~tens of millions | client poller fix missing or jar not rebuilt → §1(a), §3 |
| `[INSERT], Operations` < 30000000 | load truncated by `maxexecutiontime` → §1(b) |
| Migration NARROW fails / leader election mid-run | raft snapshots were enabled at 30M → leave snapshots off (§4) |
| `follower-proxy` flag ignored | used `-e key=no` (string, truthy); use JSON `-e '{"rdma_follower_proxy":"no"}'` |
| Run dies early / worker threads throw `exhausted retries` | client missing the 1500-retry + survive-on-exhaust patch → §1(a) |
