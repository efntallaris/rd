# Crash campaign — auto-collected results

Generated 2026-09-29 10:55 UTC by `collect.sh` from `logs/`. Counts are grep hits
summed over the recipient (sg4) and sg1 logs of each run.

| run | reshard rc | kill | crash sigs | leader elections (incl. startup) | faked INDX_UPD | chain-ack observed | INDX_UPD applied | RE-FORM | resume-plan | RESUME-STATUS | recipient DBSIZE |
|---|---|---|---|---|---|---|---|---|---|---|---|
| HEALTHY | see run.log | — | 0 | 4 | 0 | 3 | 9 | 0 | 0 | 0 | ? |
| S1 | rc=2, play failed | KILLED | 0 | 3 | 0 | 2 | 5 | 3 | 0 | 1 | ? |
| S2 | rc=0 | KILLED | 0 | 4 | 0 | 0 | 2 | 0 | 1 | 0 | ? |
| S3 | rc=0 | KILLED | 0 | 4 | 0 | 3 | 9 | 0 | 0 | 0 | ? |
| S4 | rc=0 | KILLED | 0 | 4 | 0 | 3 | 6 | 9 | 0 | 0 | ? |
| S5 | rc=0 | KILLED | 0 | 4 | 0 | 0 | 4 | 0 | 0 | 0 | ? |

## metrics.json (analyze_crash.py)

```json
{
  "HEALTHY": {
    "name": "Healthy (no crash)",
    "target": "reference \u2014 clean reshard",
    "color": "#3b5bdb",
    "pre": 300145.67741935485,
    "plateau": 373565.17982905987,
    "rise": 24.461289278247843,
    "t2f": 27,
    "migwin": 12.6,
    "upd_err": null,
    "crash": 0,
    "reform": 0,
    "elections": 0,
    "dbsize": null,
    "end": 600
  },
  "S1": {
    "name": "Recipient LEADER crash",
    "target": "kill sg4 leader redis3 mid-transfer",
    "color": "#d6336c",
    "pre": 301526.8387096774,
    "plateau": 375018.53112033196,
    "rise": 24.3731843988241,
    "t2f": 118,
    "migwin": null,
    "upd_err": null,
    "crash": 0,
    "reform": 3,
    "elections": 1,
    "dbsize": 897307,
    "end": 440
  },
  "S2": {
    "name": "Donor LEADER crash",
    "target": "kill sg1 leader redis0 mid-transfer",
    "color": "#7048e8",
    "pre": 302805.4503225807,
    "plateau": 351249.1241595442,
    "rise": 15.998283315361771,
    "t2f": 44,
    "migwin": 23.41,
    "upd_err": 0,
    "crash": 0,
    "reform": 0,
    "elections": 0,
    "dbsize": null,
    "end": 600
  },
  "S3": {
    "name": "Donor FOLLOWER crash",
    "target": "kill sg1 follower redis1 mid-transfer",
    "color": "#e8890c",
    "pre": 303371.3548387097,
    "plateau": 369575.396011396,
    "rise": 21.822772689888392,
    "t2f": 58,
    "migwin": 14.54,
    "upd_err": 0,
    "crash": 0,
    "reform": 0,
    "elections": 0,
    "dbsize": null,
    "end": 600
  },
  "S4": {
    "name": "Recipient FOLLOWER crash",
    "target": "kill sg4 chain follower redis4",
    "color": "#2f9e44",
    "pre": 299193.1612903226,
    "plateau": 362788.3378347578,
    "rise": 21.25555820533127,
    "t2f": 20,
    "migwin": 22.34,
    "upd_err": 0,
    "crash": 0,
    "reform": 9,
    "elections": 0,
    "dbsize": null,
    "end": 600
  },
  "S5": {
    "name": "Donor LEADER crash AFTER xfer",
    "target": "kill sg1 leader redis0 after its DONE",
    "color": "#0ca678",
    "pre": 310024.81741935486,
    "plateau": 376089.5021082621,
    "rise": 21.309482653301547,
    "t2f": 70,
    "migwin": 12.14,
    "upd_err": 0,
    "crash": 0,
    "reform": 0,
    "elections": 0,
    "dbsize": null,
    "end": 600
  }
}```

## Verdicts (tail of each run log)

### HEALTHY

```

TASK [Clean up monitoring logs] ************************************************
changed: [redis0]
changed: [redis1]
changed: [redis2]
changed: [redis3]

TASK [Clean up Redis logs] *****************************************************
changed: [redis0]
changed: [redis1]
changed: [redis2]
changed: [redis3]

PLAY [Report cold-excluded migration window] ***********************************

TASK [Compute migration window] ************************************************
ok: [localhost]

TASK [Print migration window] **************************************************
ok: [localhost] => {
    "msg": [
        "==================== MIGRATION WINDOW ====================",
        "  COLD-EXCLUDED  (sg1 FLIPPING -> last donor DONE) :   4.46s   [18:16:42.772 -> 18:16:47.237]",
        "  FULL           (first TXN_START -> last TXN_DONE):  12.60s   [18:16:34.643 -> 18:16:47.238]",
        "  cold tax (sg1 PREP+REGISTERING, excluded)       :   8.13s",
        "========================================================="
    ]
}

PLAY RECAP *********************************************************************
localhost                  : ok=2    changed=0    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
redis0                     : ok=66   changed=27   unreachable=0    failed=0    skipped=1    rescued=0    ignored=0   
redis1                     : ok=30   changed=19   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
redis2                     : ok=30   changed=19   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
redis3                     : ok=30   changed=19   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
redis4                     : ok=18   changed=10   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
redis5                     : ok=18   changed=10   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
ycsb0                      : ok=22   changed=12   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
ycsb1                      : ok=20   changed=11   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   

```

### S1

```
redis5                     : ok=18   changed=10   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
ycsb0                      : ok=14   changed=8    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
ycsb1                      : ok=12   changed=7    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   

[run] playbook exited rc=2
[run] collecting verdict...

------------------------------------------------------------
 VERDICT — S1   exp=crash_s1
------------------------------------------------------------
[crash signatures] (want 0)
   redis3_sg4: 0   [/]
   redis4_sg4: 0   [redis4]
   redis5_sg4: 0   [redis5]
   redis0_sg1: 0   [tmp]
   redis1_sg1: 0   [tmp]
[sg4 leader changes] 'Node is now a leader'  (S1 expects >=1 on redis4/5)
   redis3: 1
   redis4: 0
   redis5: 1
[chain degrade] 'firing MGN_INDX_UPD (anyway|immediately)' on redis3  (S4 baseline: >0)
   redis3 total:      0
   redis3 immediate:  0
   redis3 5s-timeout: 0
[migration] markers on recipient (redis3)
   TXN_DONE:  2   INDX_UPD applied: 1
   bg-merge:  moved=2478793 skipped=24757
[recipient DBSIZE] redis3:8000 (expect ~7,492,752)
   (unreachable)
[ycsb] last-block throughput + error lines (per client)
   ycsb0: (no output at /tmp/experiments/crash_s1/ycsb/ycsb0/tmp/ycsb_output_ycsb0)
   ycsb1: (no output at /tmp/experiments/crash_s1/ycsb/ycsb1/tmp/ycsb_output_ycsb1)
[injection] result=KILLED label=S1-recipient-leader target=redis3 pid=102254 t_arm=1790643151.597769771 t_kill=1790643152.160396302
------------------------------------------------------------
 BASELINE EXPECTATION: recipient down; a follower may log 'now a leader'
 but migration NOT resumed -> UPDATE errors + short DBSIZE. (Gap until Phase-B #1.)
------------------------------------------------------------

[run] injection record: /tmp/crash_inject/S1-recipient-leader.txt
[run] done (S1).
```

### S2

```
redis5                     : ok=18   changed=10   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
ycsb0                      : ok=22   changed=12   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
ycsb1                      : ok=20   changed=11   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   

[run] playbook exited rc=0
[run] collecting verdict...

------------------------------------------------------------
 VERDICT — S2   exp=crash_s2
------------------------------------------------------------
[crash signatures] (want 0)
   redis3_sg4: 0   [/]
   redis4_sg4: 0   [redis4]
   redis5_sg4: 0   [redis5]
   redis0_sg1: 0   [redis0]
   redis1_sg1: 0   [redis1]
[sg4 leader changes] 'Node is now a leader'  (S1 expects >=1 on redis4/5)
   redis3: 1
   redis4: 0
   redis5: 0
[chain degrade] 'firing MGN_INDX_UPD (anyway|immediately)' on redis3  (S4 baseline: >0)
   redis3 total:      0
   redis3 immediate:  0
   redis3 5s-timeout: 0
[migration] markers on recipient (redis3)
   TXN_DONE:  0   INDX_UPD applied: 0
   bg-merge:  n/a
[recipient DBSIZE] redis3:8000 (expect ~7,492,752)
   4752463
[ycsb] last-block throughput + error lines (per client)
   ycsb0: [OVERALL], Throughput(ops/sec), 81402.82823300427  | UPDATE-err=0 READ-err=1
   ycsb1: [OVERALL], Throughput(ops/sec), 80733.2593470208  | UPDATE-err=0 READ-err=1
[injection] result=KILLED label=S2-donor-leader target=redis0 pid=96390 t_arm=1790645079.253766340 t_kill=1790645079.802135539
------------------------------------------------------------
 BASELINE EXPECTATION: donor RDMA_MIG_FAILED; recipient batch stuck;
 orchestration FAILED. (Gap until Phase-B #2.)
------------------------------------------------------------

[run] injection record: /tmp/crash_inject/S2-donor-leader.txt
[run] done (S2).
```

### S3

```
redis5                     : ok=18   changed=10   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
ycsb0                      : ok=22   changed=12   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
ycsb1                      : ok=20   changed=11   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   

[run] playbook exited rc=0
[run] collecting verdict...

------------------------------------------------------------
 VERDICT — S3   exp=crash_s3
------------------------------------------------------------
[crash signatures] (want 0)
   redis3_sg4: 0   [/]
   redis4_sg4: 0   [redis4]
   redis5_sg4: 0   [redis5]
   redis0_sg1: 0   [redis0]
   redis1_sg1: 0   [redis1]
[sg4 leader changes] 'Node is now a leader'  (S1 expects >=1 on redis4/5)
   redis3: 1
   redis4: 0
   redis5: 0
[chain degrade] 'firing MGN_INDX_UPD (anyway|immediately)' on redis3  (S4 baseline: >0)
   redis3 total:      0
   redis3 immediate:  0
   redis3 5s-timeout: 0
[migration] markers on recipient (redis3)
   TXN_DONE:  6   INDX_UPD applied: 3
   bg-merge:  moved=7459141 skipped=33611
[recipient DBSIZE] redis3:8000 (expect ~7,492,752)
   7492752
[ycsb] last-block throughput + error lines (per client)
   ycsb0: [OVERALL], Throughput(ops/sec), 85426.71201375744  | UPDATE-err=0 READ-err=1
   ycsb1: [OVERALL], Throughput(ops/sec), 84841.16600237279  | UPDATE-err=0 READ-err=1
[injection] result=KILLED label=S3-donor-follower target=redis1 pid=87477 t_arm=1790647194.042809511 t_kill=1790647194.647614556
------------------------------------------------------------
 CONTROL — should PASS TODAY: crash-sig=0, DBSIZE ~7.49M, UPDATE-err ~0,
 no sg4 leader change, degrade=0.
------------------------------------------------------------

[run] injection record: /tmp/crash_inject/S3-donor-follower.txt
[run] done (S3).
```

### S4

```
redis5                     : ok=18   changed=10   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
ycsb0                      : ok=22   changed=12   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
ycsb1                      : ok=20   changed=11   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   

[run] playbook exited rc=0
[run] collecting verdict...

------------------------------------------------------------
 VERDICT — S4   exp=crash_s4
------------------------------------------------------------
[crash signatures] (want 0)
   redis3_sg4: 0   [/]
   redis4_sg4: 0   [redis4]
   redis5_sg4: 0   [redis5]
   redis0_sg1: 0   [redis0]
   redis1_sg1: 0   [redis1]
[sg4 leader changes] 'Node is now a leader'  (S1 expects >=1 on redis4/5)
   redis3: 1
   redis4: 0
   redis5: 0
[chain degrade] 'firing MGN_INDX_UPD (anyway|immediately)' on redis3  (S4 baseline: >0)
   redis3 total:      0
   redis3 immediate:  0
   redis3 5s-timeout: 0
[migration] markers on recipient (redis3)
   TXN_DONE:  6   INDX_UPD applied: 3
   bg-merge:  moved=7468334 skipped=24418
[recipient DBSIZE] redis3:8000 (expect ~7,492,752)
   7492752
[ycsb] last-block throughput + error lines (per client)
   ycsb0: [OVERALL], Throughput(ops/sec), 84459.05212638315  | UPDATE-err=0 READ-err=0
   ycsb1: [OVERALL], Throughput(ops/sec), 84201.13314447593  | UPDATE-err=0 READ-err=0
[injection] result=KILLED label=S4-recipient-follower target=redis4 pid=70573 t_arm=1790649292.046191509 t_kill=1790649292.639294935
------------------------------------------------------------
 BASELINE EXPECTATION: migration completes but 'firing MGN_INDX_UPD anyway' > 0
 (chain degraded to Raft-only; killed follower missing bytes). (Gap until Phase-B #4.)
------------------------------------------------------------

[run] injection record: /tmp/crash_inject/S4-recipient-follower.txt
[run] done (S4).
```

### S5

```
ycsb0                      : ok=22   changed=12   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
ycsb1                      : ok=20   changed=11   unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   

[run] playbook exited rc=0
[run] collecting verdict...

------------------------------------------------------------
 VERDICT — S5   exp=crash_s5
------------------------------------------------------------
[crash signatures] (want 0)
   redis3_sg4: 0   [/]
   redis4_sg4: 0   [redis4]
   redis5_sg4: 0   [redis5]
   redis0_sg1: 0   [redis0]
   redis1_sg1: 0   [redis1]
[sg4 leader changes] 'Node is now a leader'  (S1 expects >=1 on redis4/5)
   redis3: 1
   redis4: 0
   redis5: 0
[chain degrade] 'firing MGN_INDX_UPD (anyway|immediately)' on redis3  (S4 baseline: >0)
   redis3 total:      0
   redis3 immediate:  0
   redis3 5s-timeout: 0
[migration] markers on recipient (redis3)
   TXN_DONE:  2   INDX_UPD applied: 0
   bg-merge:  n/a
[recipient DBSIZE] redis3:8000 (expect ~7,492,752)
   6181600
[ycsb] last-block throughput + error lines (per client)
   ycsb0: [OVERALL], Throughput(ops/sec), 87679.62391430419  | UPDATE-err=0 READ-err=1
   ycsb1: [OVERALL], Throughput(ops/sec), 87065.51711496738  | UPDATE-err=0 READ-err=1
[injection] result=KILLED label=S5-donor-after-transfer target=redis0 pid=159098 t_arm=1790651441.569902700 t_kill=1790651442.118371166
------------------------------------------------------------
 EXPECTATION: sg1 data already on recipient; killing sg1 leader is a
 NON-EVENT -> reshard still completes via a new sg1 leader (NARROW/reconcile),
 crash-sig=0, UPDATE-err ~0, integrity intact. (Debug 500k: DBSIZE << 7.49M.)
------------------------------------------------------------

[run] injection record: /tmp/crash_inject/S5-donor-after-transfer.txt
[run] done (S5).
```

## Figures

- campaign_heal_full_run.pdf
- campaign_heal_full_run.png
- campaign_heal_gantt.png
- campaign_HEALTHY.png
- campaign_heal_window.txt
- campaign_heal_ycsb.png
- campaign_S1.png
- campaign_S2.png
- campaign_S3.png
- campaign_S4.png
- campaign_S5.png
- crash_artifact.html
- crash_s1_full_run.pdf
- crash_s1_full_run.png
- crash_s1_gantt.png
- crash_s1_window.txt
- crash_s1_ycsb.png
- crash_s2_full_run.pdf
- crash_s2_full_run.png
- crash_s2_window.txt
- crash_s2_ycsb.png
- crash_s3_full_run.pdf
- crash_s3_full_run.png
- crash_s3_gantt.png
- crash_s3_window.txt
- crash_s3_ycsb.png
- crash_s4_full_run.pdf
- crash_s4_full_run.png
- crash_s4_gantt.png
- crash_s4_window.txt
- crash_s4_ycsb.png
- crash_s5_full_run.pdf
- crash_s5_full_run.png
- crash_s5_gantt.png
- crash_s5_window.txt
- crash_s5_ycsb.png
- metrics.json

## Resume / recovery lines

### S1

```
78504:M 28 Sep 2026 18:52:34.003 * <raft> AqRaft become-leader: in-flight migration sess=700000000000005461 detected on promotion (START seen, no DONE) — driving roll-forward recovery
78504:M 28 Sep 2026 18:52:38.213 * AqRaft MGN-RESUME-STATUS: range=5461-6825 durable=0 pending=0 missing=1365 (from 0 batches)
78504:M 28 Sep 2026 18:52:56.153 # CHAIN: sess=800000000000005461 pipelined forward failed (chain not ready for pipelined forward sess=700000000000000000) — attempting chain RE-FORM (never faking durability)
78504:M 28 Sep 2026 18:52:56.154 # CHAIN: sess=700000000000000000 RE-FORM — dropped dead head redis3:8000, promoted redis4:8000 to direct successor (leader will re-forward straight to it)
78504:M 28 Sep 2026 18:52:57.273 * CHAIN: sess=800000000000005461 RE-FORMED + re-forwarded to surviving follower — MGN_INDX_UPD deferred (awaiting REAL CHAIN-ACK from new tail)
78504:M 28 Sep 2026 18:52:34.003 * <raft> AqRaft become-leader: in-flight migration sess=700000000000005461 detected on promotion (START seen, no DONE) — driving roll-forward recovery
78504:M 28 Sep 2026 18:52:38.213 * AqRaft MGN-RESUME-STATUS: range=5461-6825 durable=0 pending=0 missing=1365 (from 0 batches)
78504:M 28 Sep 2026 18:52:56.153 # CHAIN: sess=800000000000005461 pipelined forward failed (chain not ready for pipelined forward sess=700000000000000000) — attempting chain RE-FORM (never faking durability)
78504:M 28 Sep 2026 18:52:56.154 # CHAIN: sess=700000000000000000 RE-FORM — dropped dead head redis3:8000, promoted redis4:8000 to direct successor (leader will re-forward straight to it)
78504:M 28 Sep 2026 18:52:57.273 * CHAIN: sess=800000000000005461 RE-FORMED + re-forwarded to surviving follower — MGN_INDX_UPD deferred (awaiting REAL CHAIN-ACK from new tail)
```

### S2

```
80144:M 28 Sep 2026 19:24:41.098 * <raft> AqRaft become-leader: in-flight migration sess=1 detected on promotion (START seen, no DONE) — driving roll-forward recovery
80144:M 28 Sep 2026 19:24:41.155 * AqRaft S2 resume-plan: id=800000000000000001 sess=1 range=0-1364 — recipient reports durable=0 pending=0 missing=1365; skipping 0 already-landed slots, re-shipping 1365 (waited 1ms)
```

### S4

```
150464:M 28 Sep 2026 20:34:54.318 # CHAIN: sess=1 pipelined forward failed (CHAIN-FORWARDED to F1 failed: Connection reset by peer) — attempting chain RE-FORM (never faking durability)
150464:M 28 Sep 2026 20:34:54.318 # CHAIN: sess=700000000000000000 RE-FORM — dropped dead head redis4:8000, promoted redis5:8000 to direct successor (leader will re-forward straight to it)
150464:M 28 Sep 2026 20:34:55.501 * CHAIN: sess=1 RE-FORMED + re-forwarded to surviving follower — MGN_INDX_UPD deferred (awaiting REAL CHAIN-ACK from new tail)
150464:M 28 Sep 2026 20:35:03.873 # CHAIN: sess=1 pipelined forward failed (chain not ready for pipelined forward sess=700000000000000001) — attempting chain RE-FORM (never faking durability)
150464:M 28 Sep 2026 20:35:03.873 # CHAIN: sess=700000000000000001 RE-FORM — dropped dead head redis4:8000, promoted redis5:8000 to direct successor (leader will re-forward straight to it)
150464:M 28 Sep 2026 20:35:04.881 # CHAIN: sess=1 pipelined forward failed (chain not ready for pipelined forward sess=700000000000000002) — attempting chain RE-FORM (never faking durability)
150464:M 28 Sep 2026 20:35:04.881 # CHAIN: sess=700000000000000002 RE-FORM — dropped dead head redis4:8000, promoted redis5:8000 to direct successor (leader will re-forward straight to it)
150464:M 28 Sep 2026 20:35:04.990 * CHAIN: sess=1 RE-FORMED + re-forwarded to surviving follower — MGN_INDX_UPD deferred (awaiting REAL CHAIN-ACK from new tail)
150464:M 28 Sep 2026 20:35:06.006 * CHAIN: sess=1 RE-FORMED + re-forwarded to surviving follower — MGN_INDX_UPD deferred (awaiting REAL CHAIN-ACK from new tail)
150464:M 28 Sep 2026 20:34:54.318 # CHAIN: sess=1 pipelined forward failed (CHAIN-FORWARDED to F1 failed: Connection reset by peer) — attempting chain RE-FORM (never faking durability)
150464:M 28 Sep 2026 20:34:54.318 # CHAIN: sess=700000000000000000 RE-FORM — dropped dead head redis4:8000, promoted redis5:8000 to direct successor (leader will re-forward straight to it)
150464:M 28 Sep 2026 20:34:55.501 * CHAIN: sess=1 RE-FORMED + re-forwarded to surviving follower — MGN_INDX_UPD deferred (awaiting REAL CHAIN-ACK from new tail)
150464:M 28 Sep 2026 20:35:03.873 # CHAIN: sess=1 pipelined forward failed (chain not ready for pipelined forward sess=700000000000000001) — attempting chain RE-FORM (never faking durability)
150464:M 28 Sep 2026 20:35:03.873 # CHAIN: sess=700000000000000001 RE-FORM — dropped dead head redis4:8000, promoted redis5:8000 to direct successor (leader will re-forward straight to it)
150464:M 28 Sep 2026 20:35:04.881 # CHAIN: sess=1 pipelined forward failed (chain not ready for pipelined forward sess=700000000000000002) — attempting chain RE-FORM (never faking durability)
150464:M 28 Sep 2026 20:35:04.881 # CHAIN: sess=700000000000000002 RE-FORM — dropped dead head redis4:8000, promoted redis5:8000 to direct successor (leader will re-forward straight to it)
150464:M 28 Sep 2026 20:35:04.990 * CHAIN: sess=1 RE-FORMED + re-forwarded to surviving follower — MGN_INDX_UPD deferred (awaiting REAL CHAIN-ACK from new tail)
150464:M 28 Sep 2026 20:35:06.006 * CHAIN: sess=1 RE-FORMED + re-forwarded to surviving follower — MGN_INDX_UPD deferred (awaiting REAL CHAIN-ACK from new tail)
```
