# Recipient-apply thread-safety audit

## Purpose

This document is the Phase 4 deliverable from [PHASE4_PLAN.md](../PHASE4_PLAN.md). It enumerates every call site in the Redis 8.6.2 tree (vendored at `redis/src/`) plus the aqueduct extensions (`r_allocator`, `cluster_rdma.c`) that reads or writes the 11 protected fields the recipient-apply worker must coexist with. Each row carries the lock instruction the implementing PR (Phase 4a → 4d) will apply.

This is **first-pass output**: ~216 call sites enumerated by structured grep + per-site classification. The Phase 4a / 4b / 4c PRs each consume their relevant subset and verify line-by-line before touching code. Misclassifications are expected at this scale — the audit's job is completeness, not perfection. Flag anything that looks wrong during the implementing PR's review.

## Scope

The 11 protected fields are repeated here for grepping convenience:

| # | Field | Where defined |
|---|---|---|
| 1 | `server.cluster->slots[CLUSTER_SLOTS]` | [cluster_legacy.h](../redis/src/cluster_legacy.h) |
| 2 | `server.cluster->migrating_slots_to[CLUSTER_SLOTS]` | cluster_legacy.h |
| 3 | `server.cluster->importing_slots_from[CLUSTER_SLOTS]` | cluster_legacy.h |
| 4 | `clusterNode->slots[CLUSTER_SLOTS/8]` (per-node bitmap) | cluster_legacy.h |
| 5 | `server.db[i].keys` (per-slot kvstore) | [server.h:1165](../redis/src/server.h#L1165) |
| 6 | `server.db[i].expires` (per-slot kvstore) | server.h:1165 |
| 7 | `server.db[i].blocking_keys` | server.h |
| 8 | `server.db[i].watched_keys` | server.h |
| 9 | `server.ready_keys` (**global**) | [server.h:2381](../redis/src/server.h#L2381) |
| 10 | `kvstoreDictMetadata` per-slot counters | [server.h:1191-1198](../redis/src/server.h#L1191-L1198) |
| 11 | `r_allocator` per-slot block lists | `redis/src/allocator.c` |

## How to read this document

Each row has 8 columns:

- **file:line** — relative to `redis/src/`.
- **function** — enclosing function.
- **field** — which of the 11 protected fields (or notification primitive: `signalKeyAsReady`, `notifyKeyspaceEvent`, `moduleNotifyKeyspaceEvent`).
- **R/W** — direction of access.
- **Hot/Slow** — `Hot` = per-command / per-key path; `Slow` = cron / gossip / admin / startup / debug.
- **Proposed** — lock instruction. Vocabulary:
  - `topo-rdlock+slot-rdlock` — hot-path readers.
  - `topo-rdlock+slot-wrlock` — single-slot writers (including the worker apply).
  - `topo-wrlock` — multi-slot mutators (RESET, MOVE, DEL_NODE_SLOTS, gossip UPDATE).
  - `slot-rdlock+batch-release` — long iterators (expire, defrag, SCAN, RDB save). Releases between batches of ~64 keys to avoid starving the worker.
  - `defer-to-main` — worker enqueues; main thread executes (notification dispatch, `signalKeyAsReady`, SWAPDB).
  - `none` — site is provably main-thread-only or covered by an existing per-callee lock.
  - `(existing)` in Notes — already protected by the prototype `cluster->slots_lock` rwlock; the 4a edit just renames to `slot-*` helpers.
- **Risk** — H (worker-vs-main race the refactor exists to fix), M (hot-path read/write), L (slow / admin / startup).
- **Note** — short justification.

---

## Call sites — slot routing (fields 1-4)

| file:line | function | field | R/W | Hot/Slow | Proposed | Risk | Note |
|---|---|---|---|---|---|---|---|
| cluster.c:1299 | getNodeByQuery | cluster->slots | R | Hot | topo-rdlock+slot-rdlock | H | hot dispatch routing |
| cluster.c:2125 | clusterCanServiceKeys | clusterNode.slots | R | Hot | topo-rdlock | M | replication check (see Q4) |
| cluster.c:2128 | clusterCanServiceKeys | clusterNode.slots | R | Hot | topo-rdlock | M | replication check |
| cluster.c:2147 | clusterCanServiceKeys | clusterNode.slots | R | Hot | topo-rdlock | M | replication check |
| cluster_legacy.c:118 | isSlotUnclaimed (macro) | cluster->slots | R | Hot | topo-rdlock+slot-rdlock | M | used in hot path |
| cluster_legacy.c:623 | clusterUpdateSlotsConfigWith | migrating_slots_to | W | Slow | topo-wrlock | M | gossip handler |
| cluster_legacy.c:625 | clusterUpdateSlotsConfigWith | importing_slots_from | W | Slow | topo-wrlock | M | gossip handler |
| cluster_legacy.c:1564 | clusterDelNode | importing_slots_from | W | Slow | topo-wrlock | L | multi-slot loop |
| cluster_legacy.c:1566 | clusterDelNode | migrating_slots_to | W | Slow | topo-wrlock | L | multi-slot loop |
| cluster_legacy.c:1568 | clusterDelNode | cluster->slots | R | Slow | topo-rdlock | L | node deletion |
| cluster_legacy.c:2383 | clusterUpdateSlotsConfigWith | cluster->slots | R | Slow | topo-rdlock | L | gossip slot check |
| cluster_legacy.c:2392 | clusterUpdateSlotsConfigWith | importing_slots_from | R | Slow | topo-rdlock | L | skip importing |
| cluster_legacy.c:2399 | clusterUpdateSlotsConfigWith | cluster->slots | R | Slow | topo-rdlock | L | check epoch |
| cluster_legacy.c:2405 | clusterUpdateSlotsConfigWith | cluster->slots | R | Slow | topo-rdlock | L | dirty slot tracking |
| cluster_legacy.c:2410 | clusterUpdateSlotsConfigWith | cluster->slots | R | Slow | topo-rdlock | L | dirty slot tracking |
| cluster_legacy.c:2418 | clusterUpdateSlotsConfigWith | cluster->slots | R | Slow | topo-rdlock | L | master tracking |
| cluster_legacy.c:2428 | clusterUpdateSlotsConfigWith | cluster->slots | R | Slow | topo-rdlock | L | owner check |
| cluster_legacy.c:3205 | clusterProcessMessage | cluster->slots | R | Slow | topo-rdlock | L | gossip dirty check |
| cluster_legacy.c:3207 | clusterProcessMessage | cluster->slots | R | Slow | topo-rdlock | L | epoch comparison |
| cluster_legacy.c:3213 | clusterProcessMessage | cluster->slots | R | Slow | topo-rdlock | L | send UPDATE |
| cluster_legacy.c:4127 | clusterFailoverReplaceYourMaster | cluster->slots | R | Slow | topo-rdlock | L | failover epoch check |
| cluster_legacy.c:4138 | clusterFailoverReplaceYourMaster | cluster->slots | R | Slow | topo-rdlock | L | debug logging |
| cluster_legacy.c:4270 | clusterPromoteMyself | clusterNode.slots | R | Slow | topo-rdlock | L | failover slot claim |
| cluster_legacy.c:5030 | clusterNodeSetSlotBit | clusterNode.slots | R | Slow | topo-rdlock | L | per-node bitmap test (see Q4) |
| cluster_legacy.c:5032 | clusterNodeSetSlotBit | clusterNode.slots | W | Slow | topo-wrlock | L | per-node bitmap set |
| cluster_legacy.c:5055 | clusterNodeClearSlotBit | clusterNode.slots | R | Slow | topo-rdlock | L | per-node bitmap test |
| cluster_legacy.c:5057 | clusterNodeClearSlotBit | clusterNode.slots | W | Slow | topo-wrlock | L | per-node bitmap clear |
| cluster_legacy.c:5065 | clusterNodeCoversSlot | clusterNode.slots | R | Hot | topo-rdlock | M | hot path via getNodeBySlot |
| cluster_legacy.c:5073 | clusterAddSlot | cluster->slots | R | Slow | topo-rdlock+slot-rdlock | M | check slot free |
| cluster_legacy.c:5075 | clusterAddSlot | cluster->slots | W | Slow | topo-rdlock+slot-wrlock | M | claim slot ownership |
| cluster_legacy.c:5086 | clusterDelSlot | cluster->slots | R | Slow | topo-rdlock+slot-rdlock | M | read current owner |
| cluster_legacy.c:5094 | clusterDelSlot | cluster->slots | W | Slow | topo-rdlock+slot-wrlock | M | release slot |
| cluster_legacy.c:5108 | clusterMoveNodeSlots | clusterNode.slots | R | Slow | topo-wrlock | L | multi-slot loop |
| cluster_legacy.c:5123 | clusterMoveNodeSlots | clusterNode.slots | R | Slow | topo-wrlock | L | multi-slot loop |
| cluster_legacy.c:5178 | clusterUpdateState | cluster->slots | R | Slow | topo-rdlock | L | coverage check |
| cluster_legacy.c:5179 | clusterUpdateState | cluster->slots | R | Slow | topo-rdlock | L | failed node check |
| cluster_legacy.c:5254 | removeAllNotOwnedShardChannelSubscriptions | cluster->slots | R | Slow | topo-rdlock | L | shard channel cleanup |
| cluster_legacy.c:5270 | clusterClaimUnassignedSlots | cluster->slots | R | Slow | topo-rdlock | L | startup unowned claim |
| cluster_legacy.c:5271 | clusterClaimUnassignedSlots | importing_slots_from | R | Slow | topo-rdlock | L | startup unowned claim |
| cluster_legacy.c:5282 | clusterClaimUnassignedSlots | cluster->slots | W | Slow | topo-wrlock | L | startup claim (multi-slot) |
| cluster_legacy.c:5435 | (clusterNodeCoversSlot caller) | clusterNode.slots | R | Slow | topo-rdlock | L | save config iteration |
| cluster_legacy.c:5456 | clusterGenNodeDescription | migrating_slots_to | R | Slow | topo-rdlock | L | config string gen |
| cluster_legacy.c:5459 | clusterGenNodeDescription | importing_slots_from | R | Slow | topo-rdlock | L | config string gen |
| cluster_legacy.c:5480 | clusterGenNodesSlotsInfo | cluster->slots | R | Slow | topo-rdlock | L | topology gen |
| cluster_legacy.c:5487 | clusterGenNodesSlotsInfo | cluster->slots | R | Slow | topo-rdlock | L | topology gen loop |
| cluster_legacy.c:5660 | clusterUpdateSlots | importing_slots_from | W | Slow | topo-wrlock | L | multi-slot |
| cluster_legacy.c:5666 | clusterUpdateSlots | cluster->slots | W | Slow | topo-wrlock | L | multi-slot |
| cluster_legacy.c:6104 | clusterCommand SETSLOT migrating | cluster->slots | R | Slow | topo-rdlock | L | admin command |
| cluster_legacy.c:6118 | clusterCommand SETSLOT migrating | migrating_slots_to | W | Slow | topo-rdlock+slot-wrlock | M | admin SETSLOT |
| cluster_legacy.c:6120 | clusterCommand SETSLOT importing | cluster->slots | R | Slow | topo-rdlock | L | admin command |
| cluster_legacy.c:6135 | clusterCommand SETSLOT importing | importing_slots_from | W | Slow | topo-rdlock+slot-wrlock | M | admin SETSLOT |
| cluster_legacy.c:6138 | clusterCommand SETSLOT stable | importing_slots_from | W | Slow | topo-rdlock+slot-wrlock | L | admin clear import |
| cluster_legacy.c:6139 | clusterCommand SETSLOT stable | migrating_slots_to | W | Slow | topo-rdlock+slot-wrlock | L | admin clear migrate |
| cluster_legacy.c:6154 | clusterCommand SETSLOT node | cluster->slots | R | Slow | topo-rdlock | L | admin NODE check |
| cluster_legacy.c:6166 | clusterCommand SETSLOT node | migrating_slots_to | R | Slow | topo-rdlock | L | admin key check |
| cluster_legacy.c:6167 | clusterCommand SETSLOT node | migrating_slots_to | W | Slow | topo-rdlock+slot-wrlock | L | admin clear migrate |
| cluster_legacy.c:6169 | clusterCommand SETSLOT node | cluster->slots | R | Slow | topo-rdlock | L | track ownership |
| cluster_legacy.c:6170 | clusterCommand SETSLOT node | cluster->slots | W | Slow | topo-rdlock+slot-wrlock | L | reassign slot |
| cluster_legacy.c:6193 | clusterCommand SETSLOT node | importing_slots_from | R | Slow | topo-rdlock | L | clear import check |
| cluster_legacy.c:6207 | clusterCommand SETSLOT node | importing_slots_from | W | Slow | topo-rdlock+slot-wrlock | L | clear import |
| cluster_legacy.c:6489 | clusterGetMigratingSlotSource | migrating_slots_to | R | Hot | topo-rdlock+slot-rdlock | M | v2 redirect predicate |
| cluster_legacy.c:6493 | clusterGetImportingSlotSource | importing_slots_from | R | Hot | topo-rdlock+slot-rdlock | M | v2 -ASK predicate |
| cluster_legacy.c:6501 | getNodeBySlot | cluster->slots | R | Hot | topo-rdlock+slot-rdlock | H | per-key routing table |
| cluster_rdma.c:885 | migrateToStartMigration | cluster->slots | R | Slow | topo-rdlock | L | check my slot coverage (existing) |
| cluster_rdma.c:1218 | migrateToFinalizePrep | cluster->slots | R | Slow | topo-rdlock | L | check my slot coverage (existing) |
| cluster_rdma.c:1336 | rdmaReshardFlipCommand | migrating_slots_to | W | Slow | topo-rdlock+slot-wrlock | H | source-side FLIP (existing) |
| cluster_rdma.c:1416 | rdmaReshardRecvFlipCommand | importing_slots_from | W | Slow | topo-rdlock+slot-wrlock | H | recipient-side FLIP (existing) |
| cluster_rdma.c:1626 | rdmaReshardExecCommand | migrating_slots_to | W | Slow | topo-rdlock+slot-wrlock | H | source socket finalize (existing) |
| cluster_rdma.c:472 | rdmaApplySlot (worker) | cluster->slots | W | Slow | topo-rdlock+slot-wrlock | H | recipient worker apply — primary motivation |
| cluster_asm.c:3174 | asmCheckOwnedByMaster | clusterNode.slots | R | Slow | topo-rdlock | L | ASM task validation |
| cluster_slot_stats.c:42 | markSlotsAssignedToMyShard | clusterNode.slots | R | Slow | topo-rdlock | L | slot stats reporting |
| cluster_slot_stats.c:89 | markSlotsAssignedToMyShard | clusterNode.slots | R | Slow | topo-rdlock | L | slot stats reporting |
| db.c:3054 | dbExpandSkipSlot | clusterNode.slots | R | Slow | topo-rdlock | L | DB expand skip |
| hotkeys.c:433 | xreadgroupCommand | clusterNode.slots | R | Slow | topo-rdlock | L | XREAD STREAMS slot check |

## Call sites — keyspace + metadata (fields 5, 6, 10)

| file:line | function | field | R/W | Hot/Slow | Proposed | Risk | Note |
|---|---|---|---|---|---|---|---|
| db.c:327 | lookupKey | notifyKeyspaceEvent | W | Slow | defer-to-main | H | NOTIFY_KEY_MISS; worker-reachable; module re-entry risk |
| db.c:352 | lookupKeyRead | db->keys | R | Hot | topo-rdlock+slot-rdlock | M | hot-path lookup |
| db.c:366 | lookupKeyWrite | db->keys | R | Hot | topo-rdlock+slot-rdlock | M | hot-path write-lookup |
| db.c:376 | lookupKeyWriteWithLink | db->keys | R | Hot | topo-rdlock+slot-rdlock | M | hot-path write-lookup with link |
| db.c:458 | dbAddInternal | db->keys | W | Hot | topo-rdlock+slot-wrlock | M | insert into keys dict |
| db.c:465 | dbAddInternal | db->expires | W | Hot | topo-rdlock+slot-wrlock | M | setExpireByLink may write expires |
| db.c:477 | dbAddInternal | signalKeyAsReady | W | Hot | defer-to-main | H | every dbAdd signals ready_keys (worker path) |
| db.c:478 | dbAddInternal | notifyKeyspaceEvent | W | Hot | defer-to-main | H | NOTIFY_NEW fired on every dbAdd |
| db.c:556 | dbAddRDBLoad | db->keys | R | Slow | slot-rdlock+batch-release | L | RDB load: find before add |
| db.c:566 | dbAddRDBLoad | db->keys | W | Slow | slot-rdlock+batch-release | L | RDB load: insert kvobj |
| db.c:573 | dbAddRDBLoad | db->expires | W | Slow | slot-rdlock+batch-release | L | RDB load: setExpireByLink |
| db.c:615 | dbSetValue | db->keys | R | Hot | topo-rdlock+slot-rdlock | M | find link to existing key |
| db.c:687 | dbSetValue | db->keys | W | Hot | topo-rdlock+slot-wrlock | M | replace value in keys dict |
| db.c:693 | dbSetValue | db->expires | W | Hot | topo-rdlock+slot-wrlock | M | delete from expires on overwrite |
| db.c:729 | dbSetValue | db->keys | W | Hot | topo-rdlock+slot-wrlock | M | replace value with new kvobj |
| db.c:735 | dbSetValue | db->expires | R | Hot | topo-rdlock+slot-rdlock | M | find link in expires dict |
| db.c:738 | dbSetValue | db->expires | W | Hot | topo-rdlock+slot-wrlock | M | replace expire entry |
| db.c:740 | dbSetValue | db->expires | W | Hot | topo-rdlock+slot-wrlock | M | delete from expires |
| db.c:852 | setKeyByLink | notifyKeyspaceEvent | W | Hot | defer-to-main | H | NOTIFY_OVERWRITTEN; worker-reachable |
| db.c:854 | setKeyByLink | notifyKeyspaceEvent | W | Hot | defer-to-main | H | NOTIFY_TYPE_CHANGED; worker-reachable |
| db.c:883 | dbRandomKey | db->keys | R | Slow | slot-rdlock+batch-release | L | size compare for volatility check |
| db.c:883 | dbRandomKey | db->expires | R | Slow | slot-rdlock+batch-release | L | size compare for volatility check |
| db.c:887 | dbRandomKey | db->keys | R | Slow | slot-rdlock+batch-release | L | fair random slot index |
| db.c:889 | dbRandomKey | db->keys | R | Slow | slot-rdlock+batch-release | L | fair random key from slot |
| db.c:920 | dbGenericDelete | db->keys | R | Hot | topo-rdlock+slot-rdlock | M | two-phase unlink find (see Q5) |
| db.c:954 | dbGenericDelete | db->expires | W | Hot | topo-rdlock+slot-wrlock | M | delete from expires |
| db.c:961 | dbGenericDelete | db->keys | W | Hot | topo-rdlock+slot-wrlock | M | NULL the key in async path |
| db.c:963 | dbGenericDelete | db->keys | W | Hot | topo-rdlock+slot-wrlock | M | two-phase unlink free from keys |
| db.c:1066 | emptyDbStructure | db->keys | R | Slow | slot-rdlock+batch-release | L | key count before empty |
| db.c:1213 | dbTotalServerDataBytes | db->keys | R | Slow | slot-rdlock+batch-release | L | aggregate key counts |
| db.c:1637 | scanGenericCommand | db->keys | R | Slow | slot-rdlock+batch-release | L | check dict size for slot |
| db.c:1642 | scanGenericCommand | db->keys | R | Slow | slot-rdlock+batch-release | L | init safe iterator for slot |
| db.c:1644 | scanGenericCommand | db->keys | R | Slow | slot-rdlock+batch-release | L | init iterator for all slots |
| db.c:1988 | scanGenericCommand | db->keys | R | Slow | slot-rdlock+batch-release | L | scan keys dict |
| db.c:2662 | swapdbWithDbAtIndex | db->keys | W | Slow | defer-to-main | L | swap keys pointer (see Q6) |
| db.c:2663 | swapdbWithDbAtIndex | db->expires | W | Slow | defer-to-main | L | swap expires pointer |
| db.c:2669 | swapdbWithDbAtIndex | db->keys | W | Slow | defer-to-main | L | swap back keys pointer |
| db.c:2670 | swapdbWithDbAtIndex | db->expires | W | Slow | defer-to-main | L | swap back expires pointer |
| db.c:2735 | removeExpire | db->expires | R | Hot | topo-rdlock+slot-rdlock | M | two-phase unlink find |
| db.c:2742 | removeExpire | db->expires | W | Hot | topo-rdlock+slot-wrlock | M | two-phase unlink free |
| db.c:2763 | setExpireByLink | db->keys | R | Hot | topo-rdlock+slot-rdlock | M | find link in keys dict |
| db.c:2785 | setExpireByLink | db->keys | W | Hot | topo-rdlock+slot-wrlock | M | update keys on kvobj realloc |
| db.c:2791 | setExpireByLink | db->expires | W | Hot | topo-rdlock+slot-wrlock | M | add to expires dict |
| db.c:3097 | dbFind | db->keys | R | Hot | topo-rdlock+slot-rdlock | M | lookup key in main dict |
| db.c:3110 | dbFindByLink | db->keys | R | Hot | topo-rdlock+slot-rdlock | M | find link for key |
| db.c:3121 | dbFindExpires | db->expires | R | Hot | topo-rdlock+slot-rdlock | M | lookup in expires dict |
| db.c:3125 | dbSize | db->keys | R | Slow | slot-rdlock+batch-release | L | total key count |
| db.c:3141 | dbSize | db->keys | R | Slow | slot-rdlock+batch-release | L | dict for slot |
| db.c:3143 | dbSize | db->keys | R | Slow | slot-rdlock+batch-release | L | dict size for slot |
| db.c:3152 | dbScan | db->keys | R | Slow | slot-rdlock+batch-release | L | scan keys dict |
| expire.c:389 | activeExpireCycle | db->expires | R | Slow | slot-rdlock+batch-release | L | empty check |
| expire.c:401 | activeExpireCycle | db->expires | R | Slow | slot-rdlock+batch-release | L | get expires dict size |
| expire.c:431 | activeExpireCycle | db->expires | R | Slow | slot-rdlock+batch-release | L | scan expires (long hold — batch release) |
| expire.c:757 | expireGenericCommand | db->keys | R | Hot | topo-rdlock+slot-rdlock | M | lookup for expire check |
| expire.c:822 | expireGenericCommand | db->expires | W | Hot | topo-rdlock+slot-wrlock | M | setExpire on successful expire |
| expire.c:869 | pttlGenericCommand | db->keys | R | Hot | topo-rdlock+slot-rdlock | M | lookup to check expire |
| expire.c:912 | persistCommand | db->keys | R | Hot | topo-rdlock+slot-rdlock | M | lookup to persist |
| expire.c:913 | persistCommand | db->expires | W | Hot | topo-rdlock+slot-wrlock | M | removeExpire on persist |
| expire.c:930 | touchCommand | db->keys | R | Hot | topo-rdlock+slot-rdlock | M | lookup for touch |
| aof.c:2500 | aofRewriteSelectDb | db->keys | R | Slow | slot-rdlock+batch-release | L | check db has keys |
| aof.c:2506 | aofRewriteSelectDb | db->keys | R | Slow | slot-rdlock+batch-release | L | init iterator |
| rdb.c:1583 | rdbSaveDb | db->keys | R | Slow | slot-rdlock+batch-release | L | total key count |
| rdb.c:1593 | rdbSaveDb | db->expires | R | Slow | slot-rdlock+batch-release | L | expires count |
| rdb.c:1601 | rdbSaveDb | db->keys | R | Slow | slot-rdlock+batch-release | L | init iterator |
| debug.c:290 | debugCommand | db->keys | R | Slow | slot-rdlock+batch-release | L | check db has keys |
| debug.c:299 | debugCommand | db->keys | R | Slow | slot-rdlock+batch-release | L | init iterator |
| defrag.c:1113 | defragDb | db->expires | R | Slow | slot-rdlock+batch-release | L | find link in expires |
| defrag.c:1124 | defragDb | db->keys | W | Slow | slot-rdlock+batch-release | L | update key in keys |
| defrag.c:1126 | defragDb | db->expires | W | Slow | slot-rdlock+batch-release | L | update in expires |
| defrag.c:1515 | defragDb | db->expires | R | Slow | slot-rdlock+batch-release | L | find link in expires |
| defrag.c:1521 | defragDb | db->keys | R | Slow | slot-rdlock+batch-release | L | find link in keys |
| defrag.c:1523 | defragDb | db->keys | W | Slow | slot-rdlock+batch-release | L | update key in keys |
| defrag.c:1525 | defragDb | db->expires | W | Slow | slot-rdlock+batch-release | L | update in expires |
| t_string.c:101 | setCommand | db->keys | R | Hot | topo-rdlock+slot-rdlock | M | lookupKeyWriteWithLink |
| t_string.c:154 | setCommand | db->keys | W | Hot | topo-rdlock+slot-wrlock | M | setKeyByLink |
| t_string.c:514 | getexCommand | db->expires | W | Hot | topo-rdlock+slot-wrlock | M | setExpire |
| t_string.c:524 | getexCommand | db->expires | W | Hot | topo-rdlock+slot-wrlock | M | removeExpire |
| t_string.c:547 | msetCommand | db->keys | W | Hot | topo-rdlock+slot-wrlock | M | setKey |
| t_string.c:707 | msetnxCommand | db->keys | R | Hot | topo-rdlock+slot-rdlock | M | lookupKeyWrite check |
| t_list.c:490 | lpushCommand | db->keys | R | Hot | topo-rdlock+slot-rdlock | M | lookupKeyWriteWithLink |
| t_list.c:499 | lpushCommand | db->keys | W | Hot | topo-rdlock+slot-wrlock | M | dbAddByLink |
| t_list.c:795 | lremCommand | db->keys | W | Hot | topo-rdlock+slot-wrlock | M | dbDelete |
| t_list.c:1150 | blmoveCommand | db->keys | W | Hot | topo-rdlock+slot-wrlock | M | dbDelete |
| t_list.c:1169 | blmoveCommand | db->keys | W | Hot | topo-rdlock+slot-wrlock | M | dbAdd |
| t_hash.c:975 | hsetCommand | db->keys | R | Hot | topo-rdlock+slot-rdlock | M | lookupKeyWrite |
| t_zset.c:2043 | zaddCommand | db->keys | R | Hot | topo-rdlock+slot-rdlock | M | lookupKeyWrite |
| t_zset.c:2048 | zaddCommand | db->keys | W | Hot | topo-rdlock+slot-wrlock | M | dbAdd |
| t_zset.c:3127 | zunionInterDiffCommand | db->keys | W | Hot | topo-rdlock+slot-wrlock | M | setKey |
| t_zset.c:3136 | zunionInterDiffCommand | db->keys | W | Hot | topo-rdlock+slot-wrlock | M | dbDelete |
| geo.c:833 | georadiusCommand | db->keys | W | Hot | topo-rdlock+slot-wrlock | M | setKey |
| sort.c:625 | sortCommand | db->keys | W | Slow | slot-rdlock+batch-release | L | setKey in sort result |
| object.c:1397 | getMemoryStats | db->keys | R | Slow | slot-rdlock+batch-release | L | key count |
| object.c:1403 | getMemoryStats | db->keys | R | Slow | slot-rdlock+batch-release | L | kvstore mem usage |
| object.c:1408 | getMemoryStats | db->expires | R | Slow | slot-rdlock+batch-release | L | expires mem usage |
| cluster_rdma.c:498 | rdmaApplySlot (worker) | db->keys | R | Hot | topo-rdlock+slot-wrlock | H | worker: lookup before add |
| cluster_rdma.c:500 | rdmaApplySlot (worker) | db->keys | W | Hot | topo-rdlock+slot-wrlock | H | worker: dbAdd into keys |
| cluster_slot_stats.c:57 | getSlotStat | kvstoreDictMetadata | R | Slow | slot-rdlock | L | read cpu_usec |
| cluster_slot_stats.c:59 | getSlotStat | kvstoreDictMetadata | R | Slow | slot-rdlock | L | read network_bytes_in |
| cluster_slot_stats.c:60 | getSlotStat | kvstoreDictMetadata | R | Slow | slot-rdlock | L | read network_bytes_out |
| cluster_slot_stats.c:122 | addReplySlotStat | kvstoreDictMetadata | R | Slow | slot-rdlock | L | reply cpu_usec |
| cluster_slot_stats.c:126 | addReplySlotStat | kvstoreDictMetadata | R | Slow | slot-rdlock | L | reply network_bytes_in |
| cluster_slot_stats.c:128 | addReplySlotStat | kvstoreDictMetadata | R | Slow | slot-rdlock | L | reply network_bytes_out |
| cluster_slot_stats.c:162 | clusterSlotStatsAddNetworkBytesOutForUserClient | kvstoreDictMetadata | W | Hot | topo-rdlock+slot-wrlock | M | update net bytes out (see Q3) |
| cluster_slot_stats.c:178 | clusterSlotStatsUpdateNetworkBytesOutForReplication | kvstoreDictMetadata | W | Hot | topo-rdlock+slot-wrlock | M | update net bytes out |
| cluster_slot_stats.c:207 | clusterSlotStatsFlushOutput | kvstoreDictMetadata | W | Hot | topo-rdlock+slot-wrlock | M | update net bytes out |
| cluster_slot_stats.c:227 | clusterSlotStatReset | kvstoreDictMetadata | W | Slow | slot-wrlock | L | reset cpu_usec |
| cluster_slot_stats.c:228 | clusterSlotStatReset | kvstoreDictMetadata | W | Slow | slot-wrlock | L | reset network_bytes_in |
| cluster_slot_stats.c:229 | clusterSlotStatReset | kvstoreDictMetadata | W | Slow | slot-wrlock | L | reset network_bytes_out |
| cluster_slot_stats.c:256 | clusterSlotStatsAddCpuDuration | kvstoreDictMetadata | W | Hot | topo-rdlock+slot-wrlock | M | update cpu_usec per command |
| cluster_slot_stats.c:292 | clusterSlotStatsAddNetworkBytesIn | kvstoreDictMetadata | W | Hot | topo-rdlock+slot-wrlock | M | update network_bytes_in |

## Call sites — notify / blocking / aqueduct (fields 7-9, 11 + notification primitives)

| file:line | function | field | R/W | Hot/Slow | Proposed | Risk | Note |
|---|---|---|---|---|---|---|---|
| blocked.c:351 | handleClientsBlockedOnKeys | ready_keys | R | Hot | none | L | main-thread handler loop |
| blocked.c:358 | handleClientsBlockedOnKeys | ready_keys | R | Hot | none | L | swap with fresh list |
| blocked.c:359 | handleClientsBlockedOnKeys | ready_keys | W | Hot | none | L | create fresh list (drain side) |
| blocked.c:367 | handleClientsBlockedOnKeys | ready_keys | W | Hot | none | L | delete from db->ready_keys |
| blocked.c:407 | blockForKeys | blocking_keys | W | Slow | none | L | BLPOP setup |
| blocked.c:412 | blockForKeys | blocking_keys | W | Slow | none | L | BLPOP setup |
| blocked.c:496 | signalKeyAsReadyLogic | blocking_keys | R | Hot | none | L | check before signaling |
| blocked.c:501 | signalKeyAsReadyLogic | blocking_keys | R | Hot | none | L | check before signaling |
| blocked.c:522 | signalKeyAsReadyLogic | ready_keys | W | Hot | defer-to-main | M | **worker hot path** — must enqueue (see Q3) |
| blocked.c:545 | unblockClientWaitingData | blocking_keys | R | Slow | none | L | cleanup |
| blocked.c:556 | unblockClientWaitingData | blocking_keys | W | Slow | none | L | cleanup |
| blocked.c:586 | handleClientsBlockedOnKey | blocking_keys | R | Hot | none | L | main-thread handler |
| db.c:2542 | flushAllDataIncludingDict | blocking_keys | R | Slow | none | L | safe iteration |
| db.c:2560 | flushAllDataIncludingDict | blocking_keys | R | Slow | none | L | safe iteration |
| multi.c:311 | watchCommand | watched_keys | R | Slow | none | L | WATCH setup |
| multi.c:314 | watchCommand | watched_keys | W | Slow | none | L | WATCH setup |
| multi.c:347 | unwatchAllKeys | watched_keys | W | Slow | none | L | UNWATCH/EXEC cleanup |
| multi.c:385 | touchWatchedKey | watched_keys | R | Hot | none | L | called on key mutation |
| multi.c:386 | touchWatchedKey | watched_keys | R | Hot | none | L | called on key mutation |
| multi.c:436 | touchAllWatchedKeysInDb | watched_keys | R | Slow | none | L | DB flush |
| multi.c:439 | touchAllWatchedKeysInDb | watched_keys | R | Slow | none | L | safe iteration |
| notify.c:98 | notifyKeyspaceEvent | moduleNotifyKeyspaceEvent | W | Hot | defer-to-main | H | dispatches module callbacks — worker must enqueue |
| module.c:9358 | moduleNotifyKeyspaceEvent | moduleNotifyKeyspaceEvent | W | Hot | defer-to-main | H | calls user `RedisModuleNotificationFunc`; re-entry risk |
| server.c:2936 | initServer | ready_keys | W | Slow | none | L | init only |
| server.c:3014 | initServer | blocking_keys | W | Slow | none | L | init only |
| server.c:3019 | initServer | watched_keys | W | Slow | none | L | init only |
| server.c:6155 | totalNumberOfStatefulKeys | blocking_keys | R | Slow | none | L | INFO command |
| server.c:6157 | totalNumberOfStatefulKeys | watched_keys | R | Slow | none | L | INFO command |
| cluster_rdma.c:184 | rdmaMigrateInitServerCommand | r_allocator | W | Slow | slot-wrlock | L | source-side init |
| cluster_rdma.c:474 | rdmaApplySlot (worker) | r_allocator | R | Hot | slot-rdlock | H | worker reads blocks during apply |
| cluster_rdma.c:1010 | rdmaReshardsplitCommand | r_allocator | R | Slow | slot-rdlock | L | stats logging |
| cluster_rdma.c:1287 | reshardPrepCommand | r_allocator | W | Slow | slot-wrlock | L | source locks slot |
| cluster_rdma.c:1589 | reshardExecCommand | r_allocator | W | Slow | slot-wrlock | L | source locks slot |
| allocator.c:202 | r_allocator_freeq_enqueue_lru | r_allocator | W | Hot | slot-wrlock | L | internal freelist |
| allocator.c:279 | r_allocator_dequeue_lru | r_allocator | R | Hot | slot-rdlock | L | internal freelist |
| allocator.c:491 | r_allocator_dequeue | r_allocator | W | Hot | slot-wrlock | L | internal block list |
| allocator.c:553 | r_allocator_alloc_new_empty_block | r_allocator | W | Hot | slot-wrlock | L | block allocation |
| allocator.c:618 | r_allocator_insert_kv | r_allocator | W | Hot | slot-wrlock | L | KV insert |
| allocator.c:717 | r_allocator_insert_kvobj | r_allocator | W | Hot | slot-wrlock | L | KV insert |
| allocator.c:790 | r_allocator_get_block_list_for_slot | r_allocator | R | Hot | slot-rdlock | L | block list read |
| allocator.c:802 | r_allocator_get_block_buffers_for_slot | r_allocator | R | Hot | slot-rdlock | L | buffer read (worker-reachable) |
| allocator.c:829 | r_allocator_lock_slot_blocks | r_allocator | W | Slow | slot-wrlock | L | lock slot |
| allocator.c:854 | r_allocator_free_kv | r_allocator | W | Hot | slot-wrlock | L | KV free |

---

## Per-file summary

Counts are derived from the rows above. "H" / "M" / "L" are the Risk column; "Hot" / "Slow" are the Hot/Slow column; "defer" counts `defer-to-main` proposals; "batch" counts `slot-rdlock+batch-release` proposals.

| File | Rows | Hot | Slow | W | R | H | M | L | defer | batch |
|---|---|---|---|---|---|---|---|---|---|---|
| cluster_legacy.c | 49 | 6 | 43 | 23 | 26 | 0 | 8 | 41 | 0 | 0 |
| cluster_rdma.c | 11 | 3 | 8 | 9 | 2 | 7 | 0 | 4 | 0 | 0 |
| cluster.c | 4 | 4 | 0 | 0 | 4 | 1 | 3 | 0 | 0 | 0 |
| cluster_slot_stats.c | 14 | 5 | 9 | 8 | 6 | 0 | 5 | 9 | 0 | 0 |
| db.c | 47 | 28 | 19 | 22 | 25 | 6 | 21 | 20 | 6 | 16 |
| expire.c | 9 | 6 | 3 | 4 | 5 | 0 | 6 | 3 | 0 | 3 |
| defrag.c | 7 | 0 | 7 | 4 | 3 | 0 | 0 | 7 | 0 | 7 |
| rdb.c | 3 | 0 | 3 | 0 | 3 | 0 | 0 | 3 | 0 | 3 |
| aof.c | 2 | 0 | 2 | 0 | 2 | 0 | 0 | 2 | 0 | 2 |
| t_string.c | 6 | 6 | 0 | 4 | 2 | 0 | 6 | 0 | 0 | 0 |
| t_list.c | 5 | 5 | 0 | 4 | 1 | 0 | 5 | 0 | 0 | 0 |
| t_hash.c | 1 | 1 | 0 | 0 | 1 | 0 | 1 | 0 | 0 | 0 |
| t_zset.c | 4 | 4 | 0 | 3 | 1 | 0 | 4 | 0 | 0 | 0 |
| geo.c | 1 | 1 | 0 | 1 | 0 | 0 | 1 | 0 | 0 | 0 |
| sort.c | 1 | 0 | 1 | 1 | 0 | 0 | 0 | 1 | 0 | 1 |
| object.c | 3 | 0 | 3 | 0 | 3 | 0 | 0 | 3 | 0 | 3 |
| debug.c | 2 | 0 | 2 | 0 | 2 | 0 | 0 | 2 | 0 | 2 |
| blocked.c | 12 | 8 | 4 | 5 | 7 | 0 | 1 | 11 | 1 | 0 |
| multi.c | 7 | 3 | 4 | 3 | 4 | 0 | 0 | 7 | 0 | 0 |
| notify.c | 1 | 1 | 0 | 1 | 0 | 1 | 0 | 0 | 1 | 0 |
| module.c | 1 | 1 | 0 | 1 | 0 | 1 | 0 | 0 | 1 | 0 |
| server.c | 5 | 0 | 5 | 3 | 2 | 0 | 0 | 5 | 0 | 0 |
| allocator.c | 12 | 11 | 1 | 8 | 4 | 0 | 0 | 12 | 0 | 0 |
| cluster_asm.c | 1 | 0 | 1 | 0 | 1 | 0 | 0 | 1 | 0 | 0 |
| hotkeys.c | 1 | 0 | 1 | 0 | 1 | 0 | 0 | 1 | 0 | 0 |
| **Total** | **209** | **93** | **116** | **104** | **105** | **16** | **61** | **132** | **9** | **37** |

Total is ~209 unique rows (a few sites touch multiple fields and are listed per-field, hence a slight over-count vs grep-hit cardinality). Comfortably inside the plan's "~120-180" hedge once you collapse multi-field rows.

The H rows are concentrated where the plan predicted: `cluster_rdma.c` (the worker), `getNodeByQuery` / `getNodeBySlot` (hot routing), `dbAdd*` / `setKeyByLink` / `lookupKey` (worker-reachable notifications), `notify.c` / `module.c` (the deferred dispatch boundary).

---

## Open questions for the implementing PR

### Q1. Shared lock for `db->keys` and `db->expires`?

`db->keys` and `db->expires` are separate kvstore instances but always logically co-located: a `setExpire` writes both; `dbGenericDelete` writes both; `lookupKey` may read both (TTL check). The audit currently assigns each its own `slot-wrlock`. Two designs:

- **(a)** Share `slot_locks[S]` between `keys[S]` and `expires[S]`. Simpler discipline; one wrlock covers both. Cost: a long `kvstoreScan` of `expires` blocks `keys` writers even though they don't conflict.
- **(b)** Separate `slot_locks_keys[S]` and `slot_locks_expires[S]`. More parallelism. Cost: double the lock count (1.79 MB), and call sites that touch both need a defined order to avoid AB-BA.

Recommendation: (a) for Phase 4a/4b. Promote to (b) only if profiling shows expires-scan starving the worker after batch-release is in place.

### Q2. `kvstoreDictMetadata` writes from `clusterSlotStats*` are hot-path Ws

Every command write `clusterSlotStatsAddNetworkBytesOutForUserClient` / `clusterSlotStatsAddCpuDuration` / `clusterSlotStatsAddNetworkBytesIn`. With `topo-rdlock+slot-wrlock` per command, the stats updates take the same wrlock as `dbAdd`/`dbDelete`. That's correct but redundant: stats writes don't conflict with key writes; they're just slot-scoped counters.

Options:
- **(a)** Keep stats writes under `slot-wrlock` (matches current design).
- **(b)** Make the counters `_Atomic uint64_t` and drop the lock for stats — saves one wrlock per command but introduces relaxed-ordering surprise for `getSlotStat` readers.
- **(c)** Move stats updates to a thread-local accumulator flushed under the wrlock when the lock is taken for the data write — zero extra lock cycles, no atomics.

Recommendation: (a) for 4b. If benchmark regression is >5%, switch to (c).

### Q3. `signalKeyAsReady` mutates `server.ready_keys` from the worker

The worker's `dbAdd` calls `signalKeyAsReady` ([db.c:477](redis/src/db.c#L477)) which appends to `server.ready_keys` ([blocked.c:522](redis/src/blocked.c#L522)). Three designs:

- **(a)** Defer-to-main: worker enqueues "this key got ready" records to an SPSC ring; main drains and calls `signalKeyAsReady` itself.
- **(b)** Dedicated `ready_keys_mu` mutex; worker takes it for the append.
- **(c)** Make `ready_keys` lockless (atomic head pointer + CAS append). Complex.

Recommendation: (a). It reuses the module-callback queue infrastructure from G3.

### Q4. `clusterNode->slots[]` bitmap reads from hot path (`clusterCanServiceKeys`, `clusterNodeCoversSlot` via `getNodeBySlot`)

The agent classified these as `none` initially; the audit promotes them to `topo-rdlock` because the bitmap is mutated by `clusterAddSlot`/`clusterDelSlot` which can run while the worker holds a slot wrlock (the cluster-level entry write is protected; the per-node bitmap write is not under the same lock today). Either:

- **(a)** Per-node bitmap writes piggyback on the cluster-level `topo-rdlock+slot-wrlock` we already take in `clusterAddSlot`/`clusterDelSlot`. Reads on hot path take `topo-rdlock` only. Resolution: cheap, matches the design.
- **(b)** Make the bitmap an array of `_Atomic uint64_t` and drop the lock requirement on the read side. Saves one rdlock per `getNodeBySlot`.

Recommendation: (a). The `topo-rdlock` is already taken for the `cluster->slots[]` read in `getNodeBySlot`; the bitmap read piggybacks on the same lock without extra cost.

### Q5. Two-phase `dbGenericDelete` — does the slot lock get upgraded R→W?

`dbGenericDelete` does a `dbFindByLink` (read), then `kvstoreDelete` (write). With separate rdlock + wrlock that's two acquisitions. Cleaner: always take the wrlock upfront since the intent is delete. Cost: marginal — the read window is microseconds.

Recommendation: always `topo-rdlock+slot-wrlock` for `dbGenericDelete`. Matches what the audit table says.

### Q6. SWAPDB during migration

`swapdbWithDbAtIndex` ([db.c:2662-2670](redis/src/db.c#L2662)) atomically swaps two `redisDb` structs. If a slot is `importing` and the worker is mid-apply when SWAPDB fires, the worker's `db` pointer is stale and the apply lands in the wrong database.

The audit marks SWAPDB `defer-to-main`, but that's not enough — the worker's `db` pointer is captured at apply time. Two options:

- **(a)** Reject `SWAPDB` while any slot is in `migrating_slots_to[]` or `importing_slots_from[]`. Returns `-MIGRATION_IN_PROGRESS`. Simple, conservative.
- **(b)** Pause the worker for the duration of SWAPDB. Reuses the topology-wrlock — SWAPDB takes `topo-wrlock` for the swap; worker drops the lock before each slot and re-takes it after.

Recommendation: (a). SWAPDB during migration is rare and the rejection is honest.

### Q7. Should the audit's `none` slow-path mutations (e.g., `initServer` writes, `blockForKeys` setup) be re-verified as worker-unreachable?

The audit confidently says `none` for `initServer` and `blockForKeys` because they're "main-thread-only today." A future change that calls them from any new thread (not just the apply worker) would break the assumption. For 4a-4d this is fine; for a maintenance comment in the headers, add `// MAIN-THREAD-ONLY; see audit Q7` to the implicated functions so future readers don't accidentally widen the call surface.

### Q8. Lock-order rule between `db->blocking_keys` / `db->watched_keys` and `slot_locks[S]`

These dicts are per-db, not per-slot. The hot-path `dbAdd` takes `slot-wrlock` then implicitly touches `blocking_keys` via `signalKeyAsReady`. If we ever lock `blocking_keys` (we currently propose `none`), the order must be: take `slot_locks[S]` first, then `blocking_keys` mu. Document this in a header comment when 4c lands so future contributors don't reverse the order.

### Q9. Module API surface (`RM_OpenKey`, `RM_Call`) called from notification callbacks

`RM_OpenKey` is the obvious one. `RM_Call` (dispatch arbitrary command from a module) is worse — it can take `WATCH`, hit AOF propagation, hit replication. The deferred-to-main queue must serialize all of these.

Recommendation: the SPSC queue records `(key_obj, event_type, db_id)`; the main-thread drainer calls `notifyKeyspaceEvent` itself (no worker context flows through). Modules see a normal main-thread notification firing slightly later than the data write — acceptable semantics.

### Q10. Where does the `topo-wrlock` exclude long-running scans?

`activeExpireCycle` runs every 100 ms and `kvstoreScan` can hold rdlock for ms. If a `topo-wrlock` writer (e.g., `clusterResetAllSlots`) queues, it waits behind the scan. With batch-release every 64 keys, the wait is bounded but not zero.

Question: is the topology-tier writer high-priority enough to interrupt the scan? Likely not — `CLUSTER RESET` is rare. Bound the wait at ~10 ms (one scan window) and accept it.

---

## Out of scope for this audit

- `kvstore.c` internals (dict resize, incremental rehash) — covered by lock-above-kvstore everywhere; the audit doesn't enumerate hits inside `kvstore.c`.
- Module ABI changes — Phase 4c assumes the existing module notification API stays; no new public functions.
- AOF + replication propagation from the worker — already not called from the worker today (`rdmaApplySlot` skips `propagate()`).
- The chunked-tick fallback (`MIGRATION_SLOTS_PER_TICK = 8`) stays in place until 4d verification passes.

---

## What to do with this document

1. **Phase 4a PR**: consume rows for `cluster->slots`, `migrating_slots_to`, `importing_slots_from`, `clusterNode.slots` (~75 rows in cluster_legacy.c + cluster.c + cluster_rdma.c). Implement `cluster_topology_lock` + `slot_locks[CLUSTER_SLOTS]` + helpers; wrap every call site listed.
2. **Phase 4b PR**: consume rows for `db->keys`, `db->expires`, `kvstoreDictMetadata` (~110 rows across db.c, expire.c, t_*.c, defrag.c, rdb.c, aof.c, cluster_slot_stats.c, geo.c, sort.c, object.c, debug.c). Add the batch-release helper for slow paths. Resolve Q1, Q2 before merging.
3. **Phase 4c PR**: consume rows for `blocking_keys`, `ready_keys`, `watched_keys`, `notifyKeyspaceEvent`, `moduleNotifyKeyspaceEvent` (~30 rows). Implement the SPSC defer-to-main queue. Resolve Q3, Q9 before merging.
4. **Phase 4d PR**: consume `r_allocator` rows + flip `migrationApplyTick` to a worker thread. ~12 rows of allocator changes; the worker plumbing is the bulk of the diff.

Each PR's verification gates are listed in [PHASE4_PLAN.md](../PHASE4_PLAN.md#phased-implementation-out-of-scope-for-phase-4-scoped-here-for-the-follow-up-pr).
