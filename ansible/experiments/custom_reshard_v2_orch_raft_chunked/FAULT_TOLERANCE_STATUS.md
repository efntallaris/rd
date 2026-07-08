# AqRaft Fault Tolerance — Status, Roadmap & How to Run (updated 2026-07-08)

Fault-tolerance work layered on the validated 30M background-merge reshard
(`HOWTO_30M_BGMERGE.md`). Design + per-scenario semantics: `CRASH_SCENARIOS.md`.
Run guide: `HOWTO_CRASH.md`. Harness: `ansible/crash/`. Branch `aqueduct_broken`.

---

## 1. What has been DONE

### 1a. Design (CRASH_SCENARIOS.md — complete)
Four crash scenarios with user-defined required behavior, all **roll-forward, never roll-back**:

| # | scenario | required behavior | status today |
|---|----------|-------------------|--------------|
| S1 | recipient **leader** (redis3) | on election: detect active migration → execute committed-but-unexecuted `INDX_UPD`s (third watermark `mgn_executed_idx`) → pull missing blocks from a surviving peer (`CHAIN-STATUS`) → re-form chain → donor hand-off | **designed, not implemented** |
| S2 | donor **leader** (redis0/1/2) | on election: new donor leader RPCs the recipient for session status → resumes from remaining slots | **designed, not implemented** |
| S3 | donor **follower** | majority (2/3) holds → migration completes unaffected | **PASSES today (validated control)** |
| S4 | recipient **follower** (redis4/5) | predecessor detects death → **leader re-forms the chain** around the dead member → real majority ack → only then `INDX_UPD` | **implemented, verification in progress** |

**Core invariant (drives everything):** `INDX_UPD` committed ⟺ the session's data is
physically on a **majority** of the recipient group. Never fire it on a timeout or as a
fallback — wait/repair as long as needed; with no live majority, fail loudly.

### 1b. Key empirical findings (from real 30M runs)
- **The clean baseline violated the invariant**: every session fired
  `MGN_INDX_UPD anyway` on the 5s timeout — the tail's CHAIN-ACK arrives *after* 5s
  (follower merge of the 2.86 GB pool). Durability was faked on every validated run.
- **Two premature-fire paths**: 5s timeout (`anyway`, baseline) and synchronous
  forward-failure (`immediately as fallback`, fires when a chain follower dies).
- S4 baseline: killing redis4 mid-chain is **client-invisible** (exact DBSIZE, 0 UPDATE
  errors) but silently degrades durability — the killed follower (and downstream tail)
  miss the bytes. Gantt signature: migration 3.45s→6.08s, tail-hop row empty.

### 1c. Code landed (redis/src, deployed to all hosts)
- **Part A — real-ack gating** (commit `4eee2f4b`, **VERIFIED on a clean 30M run**):
  `chainPendingTick` never fires `INDX_UPD` on a deadline; waits for the genuine
  CHAIN-ACK. Verified: `firing anyway` 3→**0**, `chain-ack observed` 0→**3**, DBSIZE
  exact, throughput ~79k/client. Cost: +~0.9s migration-completion (honest durability).
- **Part B — chain re-form on dead member** (implemented, in verification):
  - `rdmaLeaderChainDropDeadHead()` (`cluster_rdma_chain.c`): drop dead head peers[0],
    promote the next live follower (leader already holds a QP + PREP'd pool to every
    follower — no new RDMA handshake needed).
  - Both forward-failure sites (`cluster_rdma.c`, pipelined + per-slot) now: re-form →
    re-forward to the survivor → await its REAL ack; if no live follower remains →
    leave batch un-finalized → donor poll fails the migration **loudly** (never fake).
  - Fixes found via testing: idempotent re-form for sibling batches sharing one session
    (`n_peers==1` → re-forward to survivor, don't refuse); NULL-peer crash guard
    (`sdsdup(NULL)` segfault when the dead follower died mid-establish).
- **Verification state of Part B:** the mechanism was OBSERVED WORKING in run v1
  (`RE-FORM — dropped dead head redis4 → promoted redis5 → re-forwarded → chain-ack
  observed count=1, real live-majority durability`) but a full clean S4 PASS is still
  pending — runs v2/v4 had harness timing bugs (kill landed outside the forward window /
  phantom kill on a stale pid+log; both injector bugs now fixed).

### 1d. Harness + docs (ansible/crash/, committed)
`crash_inject.sh` (marker-armed killer; fresh-log gate + alive-pid check),
`scenarios.env` (S1–S4 arm/target config), `run_crash_scenario.sh` (one-command:
arm → base 30M reshard → verdict), `verdict.sh` (crash sigs, leader changes, degrade
counts, DBSIZE, YCSB errors — reads collected logs, fetches sg4 follower logs the base
collection skips). Baselines recorded: S3 PASS, S4 gap. Figures in `figures/`.

---

## 2. What NEEDS TO BE DONE

1. **Finish S4 verification (next step):** re-run `run_crash_scenario.sh S4` with the
   fixed injector; expect `RE-FORM dropped dead head` >0, `chain-ack observed` >0,
   `immediately as fallback` =0, migration DONE, DBSIZE 7,492,752, no crash sigs.
   Then commit figures + mark S4 done.
2. **S1/S2 baseline runs** (harness ready): confirm the documented gaps — S1: promoted
   follower does NOT resume the migration; S2: donor FAILED + recipient batch stuck.
3. **Phase B #1 — recipient-leader recovery** (largest feature):
   third Raft watermark `mgn_executed_idx` (per-session, advances on merge-fully-applied);
   `RDMA CHAIN-STATUS <sess>` per-slot RPC; recipient become-leader hook (execute pending
   `INDX_UPD`s, gap-pull blocks from a surviving peer via the chain-forward path pointed
   inward); re-form as head (#4 primitive); donor hand-off to the new leader.
4. **Phase B #2 — donor-leader resume:** donor become-leader hook (scan mgn-log for
   `TXN_START` without `TXN_DONE`); session-keyed status RPC to the recipient; recipient
   batch adoption keyed on replicated `sess` (not the old node id); resume-from-TRANSFER
   for remaining slots; backstop abort.
5. **Wiring:** redisraft state-change callback (`raft.c:1313`) → cluster_rdma
   become-leader notifications (used by both #1 and #2).
6. **Re-run all four scenarios post-fix** — same commands, verdicts flip gap→PASS.
   Optional: RESTART=yes variant (relaunch the killed node, verify Raft rejoin).

---

## 3. HOW TO RUN the fault-tolerance experiments

Everything as **root** from the controller (redis0), repo `/users/entall/rd`.

### 3a. One-command scenario run (~35 min each)
```bash
cd /users/entall/rd/ansible/crash
./run_crash_scenario.sh S3   # donor follower  (control — must PASS)
./run_crash_scenario.sh S4   # recipient follower (chain re-form test)
./run_crash_scenario.sh S1   # recipient leader (gap until #1 lands)
./run_crash_scenario.sh S2   # donor leader     (gap until #2 lands)
```
Each: arms the killer (fires on the Nth marker in redis3's log, then `kill -9`s the
target's pidfile after verifying the pid is alive) → runs the exact HOWTO_30M_BGMERGE §1
reshard (`experiment_name=crash_<id>`) → prints the verdict. Injection record:
`/tmp/crash_inject/<label>.txt` (`result=KILLED|NO_MARKER|STALE_PID`).

### 3b. Read the verdict
```bash
./verdict.sh S4 crash_s4     # re-print anytime (read-only)
```
Key rows: crash signatures =0 · sg4 "Node is now a leader" · chain degrade
(`firing MGN_INDX_UPD` immediate/timeout — **must be 0 with Parts A+B**) ·
`chain-ack observed` >0 (real durability) · DBSIZE ==7,492,752 · YCSB UPDATE-err ≈0.
Full log greps: collected logs in `/tmp/experiments/crash_<id>/logs/...`;
sg4 followers (redis4/5) are fetched by the wrapper (base collection skips them).

### 3c. After changing C source (rebuild + redeploy)
```bash
cd /users/entall/rd
for h in redis1 redis2 redis3 redis4 redis5; do
  sudo rsync -e "ssh -o StrictHostKeyChecking=accept-new" -a --delete redis/src/ "$h":/users/entall/rd/redis/src/
done
cd ansible && sudo ansible-playbook -i inventory.ini tasks/build/build_redis_custom.yml
# then re-run the scenario (3a)
```

### 3d. Clean (no-crash) invariant check — Part A regression test
```bash
# run the base HOWTO_30M_BGMERGE §1 command with -e experiment_name=partA_clean, then:
R3=/tmp/experiments/partA_clean/logs/redis3/tmp/redis_logs/redis3_sg4.log
sudo grep -c 'firing MGN_INDX_UPD' $R3    # want 0  (no faked durability)
sudo grep -c 'chain-ack observed'  $R3    # want 3  (one real ack per session)
```

### 3e. Figures
```bash
cd /users/entall/rd
python3 plot_phase_gantt.py      /tmp/experiments/crash_s4 /tmp/plots/crash_s4_gantt.png
python3 plot_ycsb_timeseries.py  /tmp/experiments/crash_s4 --output /tmp/plots/crash_s4_ycsb.png
# reference figures: ansible/experiments/custom_reshard_v2_orch_raft_chunked/figures/
```
