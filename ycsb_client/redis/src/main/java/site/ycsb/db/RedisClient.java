/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB — Aqueduct double-read variant.
 *
 * Bypasses Jedis's automatic MOVED/ASK handling by maintaining its own
 * slot->endpoint cache and slot-state cache, both kept fresh by metadata
 * piggybacked on every GET reply (when server.slot_meta_reply is on).
 *
 * Read path (workload-c hot path):
 *   - STABLE slot:    1 RTT to the owner.
 *   - MIGRATING slot: 1 RTT to the owner; if the value comes back nil and
 *                     the slot still says non-STABLE, 1 RTT to the peer.
 *                     This is the "double read" — keeps clients off the
 *                     -MOVED/-ASK redirect ping-pong that otherwise stalls
 *                     YCSB throughput for ~5 s per migration.
 *
 * Write path (workload-load): SET/DEL still go through JedisCluster, which
 * handles MOVED on its own. Workload c has no writes after the load phase,
 * and the load happens before any migration, so this is safe for the
 * experiment scope.
 */

package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import redis.clients.jedis.BasicCommands;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.JedisClusterCRC16;
import redis.clients.util.SafeEncoder;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class RedisClient extends DB {

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String CLUSTER_PROPERTY = "redis.cluster";
  public static final String TIMEOUT_PROPERTY = "redis.timeout";

  public static final String INDEX_KEY = "_indices";

  // ---- aqueduct slot-state ----

  /** Per-slot migration state — mirrors the server-side slotMigState enum. */
  private static final int SLOT_STABLE    = 0;
  private static final int SLOT_MIGRATING = 1;
  private static final int SLOT_MIGRATED  = 2;

  // ---- AqRaft instrumentation (TEMPORARY): diagnose why post-migration
  // throughput stays halved despite Patch 30. Counts read-path routing so we
  // can see if the Patch 30 collapse ever fires and what the donor returns for
  // a migrated slot. Logged to stderr (lands in ycsb_output) every ~3s. ----
  /* Shared (cross-thread) recipient-LEADER hint per slot, learned from the
   * migration-window slot-meta peer (which is the recipient leader). The
   * recipient leader is a cluster-wide fact, so sharing it lets any thread's
   * WRITE route to the leader even before that thread has READ the slot — the
   * per-thread slotCache[].peer would otherwise be null and the write would
   * fall back to the donor's round-robin -MOVED target (a follower ~2/3 of the
   * time). Best-effort: a null/stale read just falls back, which stays correct. */
  private static final HostAndPort[] SHARED_PEER = new HostAndPort[16384];

  /* AqRaft proactive collapse (option #2): a SINGLE shared background daemon
   * polls CLUSTER SLOTS and, the moment a slot's owner changes from its
   * bootstrap (donor) owner to the recipient (i.e. NARROW landed), flips
   * SHARED_COLLAPSED[slot]=true and records the new owner in SHARED_PEER[slot].
   * The read/write paths then route straight to the recipient WITHOUT each
   * thread having to discover the flip via a per-slot donor -MOVED round-trip
   * — which is the MOVED storm behind the deep mid-migration throughput dip.
   * Correctness is unchanged: the flag only flips AFTER CLUSTER SLOTS shows the
   * recipient as the authoritative owner (post-NARROW), exactly the condition
   * under which the lazy donor-MOVED collapse already routes to the recipient. */
  private static final HostAndPort[] SHARED_BOOT_OWNER = new HostAndPort[16384];
  private static final boolean[] SHARED_COLLAPSED = new boolean[16384];
  private static final java.util.concurrent.atomic.AtomicLong AQDBG = new java.util.concurrent.atomic.AtomicLong();
  /* Hosts of the DONOR cluster (captured at bootstrap). The migration recipient
   * (sg4) lives on different hosts, so a non-donor SHARED_PEER == the recipient. */
  private static final java.util.Set<String> DONOR_HOSTS =
      java.util.concurrent.ConcurrentHashMap.newKeySet();
  // AqRaft: hosts that recently failed to connect (e.g. a crash-killed leader).
  // A dead host must never be (re)pinned as a slot's write target, or ~1/N of
  // writes stall on it forever after a leader crash. Cleared on reconnect.
  private static final java.util.Set<String> DEAD_HOSTS =
      java.util.concurrent.ConcurrentHashMap.newKeySet();

  /* AqRaft follower-crash fix. A killed node's port DROPs packets in this env
   * (cloudlab), so a control-plane probe to it costs a full socket timeout, not
   * a fast connection-refused. When a follower dies, every worker re-resolving
   * the migrated slots hits that dead endpoint in refreshSlotOwner at the same
   * time and they all block together — observed as a ~6 s CLIENT-WIDE throughput
   * stall AFTER the migration (the reshard itself is uninterrupted; the recipient
   * leader is idle-alive with quorum intact). Two-part fix: (a) probe with a
   * SHORT connect timeout so the first hit fails in ms not seconds; (b) remember
   * the dead endpoint for a short TTL so concurrent/subsequent re-resolves skip
   * it. Keyed by host:PORT — one host can run a DEAD sg1 replica and a LIVE sg2
   * leader, so a host-level skip would wrongly drop a live master. */
  private static final java.util.concurrent.ConcurrentHashMap<HostAndPort, Long> DEAD_UNTIL =
      new java.util.concurrent.ConcurrentHashMap<>();
  private static final int PROBE_CONNECT_TIMEOUT_MS = 400;
  private static final long DEAD_TTL_MS = 2000;   // re-probe after 2 s in case it recovered
  private static boolean endpointDead(HostAndPort hp) {
    Long until = DEAD_UNTIL.get(hp);
    return until != null && until > System.currentTimeMillis();
  }
  private static void markEndpointDead(HostAndPort hp) {
    DEAD_UNTIL.put(hp, System.currentTimeMillis() + DEAD_TTL_MS);
  }
  /* Control-plane (CLUSTER SLOTS) probe: short connect timeout, normal socket timeout. */
  private static Jedis probeJedis(HostAndPort hp, Integer soTimeoutMs) {
    int so = (soTimeoutMs != null && soTimeoutMs > 0) ? soTimeoutMs : 2000;
    return new Jedis(hp.getHost(), hp.getPort(), PROBE_CONNECT_TIMEOUT_MS, so);
  }
  private static final java.util.concurrent.atomic.AtomicBoolean SLOT_POLLER_STARTED =
      new java.util.concurrent.atomic.AtomicBoolean(false);
  /* All master endpoints the client knows (populated at bootstrap). The poller
   * polls EVERY one of them and UNIONs the migrated ranges, because after a
   * cross-shardgroup migration NO single node has the correct full slot map:
   * each donor only learns about ITS OWN migration to sg4 (e.g. redis0 knows
   * the slots it gave up, but still reports the other donors as owning their
   * already-migrated slots). Polling one seed therefore mis-routes ~2/3 of the
   * migrated slots to the wrong (donor) node forever -> -MOVED bounce, read
   * failures, donors never offloaded, no scaling. */
  private static final java.util.Set<HostAndPort> POLL_TARGETS =
      java.util.concurrent.ConcurrentHashMap.newKeySet();

  /** Start the shared CLUSTER SLOTS poller once. pollMs<=0 disables it (baseline). */
  @SuppressWarnings("unchecked")
  private static void startSlotPollerOnce(final HostAndPort seed, final Integer timeoutMs,
                                          final int pollMs) {
    if (pollMs <= 0) return;
    if (!SLOT_POLLER_STARTED.compareAndSet(false, true)) return;
    POLL_TARGETS.add(seed);
    Thread t = new Thread(() -> {
      while (true) {
        try {
          Thread.sleep(pollMs);
          /* AqRaft S1 fix: also poll the RECIPIENT leaders. sg4 owns no slots at
           * bootstrap (migration hasn't happened), so its nodes are never in the
           * bootstrap slot map / POLL_TARGETS. The recipient leader is learned via
           * the read path into SHARED_PEER; add those here so the poller queries
           * them and (below) learns their REPLICAS — otherwise a recipient-leader
           * crash (S1) leaves the client with no endpoint for the new sg4 leader. */
          for (int s = 0; s < SHARED_PEER.length; s++) {
            HostAndPort peer = SHARED_PEER[s];
            if (peer != null) { POLL_TARGETS.add(peer); }
          }
          /* Poll every known master and UNION sg4-owned ranges. We only ever
           * SET collapsed (never unset): a node's stale view of another donor's
           * slots reports the BOOT owner (== SHARED_BOOT_OWNER) and is a no-op,
           * while the donor's own view reports sg4 (!= boot) and flips it. */
          for (HostAndPort target : POLL_TARGETS) {
            if (endpointDead(target)) continue;   // skip a known-dead endpoint this tick
            Jedis j = probeJedis(target, timeoutMs);  // short connect timeout
            try {
              j.connect();
              j.getClient().cluster(new byte[][]{SafeEncoder.encode("SLOTS")});
              Object reply = j.getClient().getOne();
              if (!(reply instanceof List)) { continue; }
              for (Object rangeObj : (List<Object>) reply) {
                List<Object> range = (List<Object>) rangeObj;
                int startSlot = (int) (long) (Long) range.get(0);
                int endSlot   = (int) (long) (Long) range.get(1);
                List<Object> masterInfo = (List<Object>) range.get(2);
                String mh = SafeEncoder.encode((byte[]) masterInfo.get(0));
                long   mp = (Long) masterInfo.get(1);
                HostAndPort owner = new HostAndPort(mh, (int) mp);
                // AqRaft S1 fix: learn this range's REPLICAS as poll targets, so a
                // promoted replica is discoverable after a leader crash (especially
                // the sg4 recipient group, whose replicas were never in the
                // bootstrap slot map). The promoted replica reports ITSELF as master
                // in its own CLUSTER SLOTS, letting the union below re-resolve
                // ownership to the new leader.
                for (int ri = 3; ri < range.size(); ri++) {
                  try {
                    List<Object> rep = (List<Object>) range.get(ri);
                    POLL_TARGETS.add(new HostAndPort(
                        SafeEncoder.encode((byte[]) rep.get(0)),
                        (int) (long) (Long) rep.get(1)));
                  } catch (Exception ignore) { /* skip malformed replica */ }
                }
                /* AqRaft ownership fix: ONLY trust a collapse report from a DONOR
                 * host. During migration the recipient sg4 advertises its full
                 * configured range (0-16383); polling sg4 would collapse EVERY slot
                 * onto it and route the whole keyspace to one leader (throughput
                 * collapse). Donors correctly announce ONLY the slots they narrowed
                 * to sg4, so a donor-only union yields exactly the migrated ranges.
                 * sg4's replicas stay in POLL_TARGETS for leader discovery but must
                 * not drive collapse. */
                boolean targetIsDonor = DONOR_HOSTS.contains(target.getHost());
                /* AqRaft dead-leader failover (S1 recipient-leader crash): when we
                 * poll a RECIPIENT node reporting ITSELF as owner of this range (a
                 * promoted sg4 leader), fail over any slot whose CURRENT owner is a
                 * now-DEAD recipient leader to this live node IMMEDIATELY — instead
                 * of waiting ~tens of seconds for the donor's post-reconcile CLUSTER
                 * SLOTS to propagate (clients otherwise keep writing to the dead
                 * leader). Safe: only overrides a slot whose current owner is a dead
                 * RECIPIENT; donor slots have live owners, and sg4's full-range
                 * self-advertisement can't collapse the keyspace here because a
                 * non-migrated slot's owner is never dead. */
                if (!targetIsDonor && owner.equals(target)
                    && !DEAD_HOSTS.contains(owner.getHost()) && !endpointDead(owner)) {
                  for (int fs = startSlot; fs <= endSlot && fs < 16384; fs++) {
                    HostAndPort fcur = SHARED_PEER[fs];
                    if (fcur != null && !fcur.equals(owner) && endpointDead(fcur)
                        && !DONOR_HOSTS.contains(fcur.getHost())) {
                      SHARED_PEER[fs] = owner;
                      SHARED_COLLAPSED[fs] = true;
                    }
                  }
                }
                for (int s = startSlot; s <= endSlot && s < 16384; s++) {
                  HostAndPort boot = SHARED_BOOT_OWNER[s];
                  /* A donor reporting ANOTHER DONOR node as owner is a leader change
                   * inside the donor group, not a migration: the slot has not moved to
                   * the recipient. Collapsing on it made the new donor leader pass for
                   * the recipient, and reads returned its frozen copy until NARROW.
                   * Follow the donor leg instead, and only on the new leader's own
                   * word (another group's view of this range can be stale). */
                  if (boot != null && !owner.equals(boot) && targetIsDonor
                      && DONOR_HOSTS.contains(owner.getHost())) {
                    if (owner.equals(target) && !endpointDead(owner)) SHARED_BOOT_OWNER[s] = owner;
                    continue;
                  }
                  if (boot != null && !owner.equals(boot) && targetIsDonor) {
                    /* AqRaft fix: never DOWNGRADE the write target from a recipient
                     * (sg4) to a donor. Reads learn the true recipient leader from
                     * the migration-window slot-meta and set SHARED_PEER=sg4, but a
                     * donor's post-NARROW CLUSTER SLOTS is inconsistent (it still
                     * reports ITSELF as owner of a migrated slot), and the poller's
                     * union would overwrite the sg4 value with that donor -> every
                     * write to the migrated slot routes to the donor and -MOVED
                     * storms to sg4 (tens of millions of redirects). Only overwrite
                     * SHARED_PEER when not replacing a recipient with a donor.
                     * SHARED_COLLAPSED stays set so reads keep fast-collapsing. */
                    HostAndPort cur = SHARED_PEER[s];
                    boolean curIsRecipient = (cur != null) && !DONOR_HOSTS.contains(cur.getHost());
                    boolean ownerIsDonor = DONOR_HOSTS.contains(owner.getHost());
                    if (!(curIsRecipient && ownerIsDonor)
                        && !DEAD_HOSTS.contains(owner.getHost())) {
                      SHARED_PEER[s] = owner;
                    }
                    SHARED_COLLAPSED[s] = true;
                  }
                }
              }
            } catch (Exception inner) {
              /* skip this target this tick (leader moving mid-NARROW etc.);
               * remember a dead endpoint so re-resolves + later ticks skip it. */
              markEndpointDead(target);
            } finally {
              try { j.close(); } catch (Exception ignore) { /* drain */ }
            }
          }
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        } catch (Exception e) {
          /* transient — retry next tick */
        }
      }
    }, "redis-slot-poller");
    t.setDaemon(true);
    t.start();
  }

  private static final java.util.concurrent.atomic.AtomicLong INSTR_FAST = new java.util.concurrent.atomic.AtomicLong();
  private static final java.util.concurrent.atomic.AtomicLong INSTR_TWOSIDED = new java.util.concurrent.atomic.AtomicLong();
  private static final java.util.concurrent.atomic.AtomicLong INSTR_COLLAPSE = new java.util.concurrent.atomic.AtomicLong();
  private static final java.util.concurrent.atomic.AtomicLong INSTR_DONOR_MOVED = new java.util.concurrent.atomic.AtomicLong();
  private static final java.util.concurrent.atomic.AtomicLong INSTR_DONOR_META_MIGRATING = new java.util.concurrent.atomic.AtomicLong();
  private static final java.util.concurrent.atomic.AtomicLong INSTR_DONOR_META_MIGRATED = new java.util.concurrent.atomic.AtomicLong();
  private static final java.util.concurrent.atomic.AtomicLong INSTR_LAST_LOG_MS = new java.util.concurrent.atomic.AtomicLong();
  // Per-target-host read latency: host -> [sumNanos, count]. Reveals whether
  // reads to the recipient (sg4) are slower than reads to donors.
  private static final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.LongAdder[]> INSTR_HOST_LAT =
      new java.util.concurrent.ConcurrentHashMap<>();
  private static void instrRecordLat(HostAndPort hp, long nanos) {
    if (hp == null) return;
    java.util.concurrent.atomic.LongAdder[] e = INSTR_HOST_LAT.computeIfAbsent(hp.toString(),
        k -> new java.util.concurrent.atomic.LongAdder[]{
            new java.util.concurrent.atomic.LongAdder(), new java.util.concurrent.atomic.LongAdder()});
    e[0].add(nanos);
    e[1].add(1);
  }
  // Migration-window routing audit: where do the two-sided read legs and the
  // write redirect actually go? donor-leg should hit the donor leader, peer-leg
  // and writes should hit the RECIPIENT LEADER (not its followers).
  private static final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.LongAdder> INSTR_2S_DONOR = new java.util.concurrent.ConcurrentHashMap<>();
  private static final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.LongAdder> INSTR_2S_PEER  = new java.util.concurrent.ConcurrentHashMap<>();
  private static final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.LongAdder> INSTR_WRITE_HOST = new java.util.concurrent.ConcurrentHashMap<>();
  private static void instrCount(java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.LongAdder> m, HostAndPort hp) {
    if (hp == null) return;
    m.computeIfAbsent(hp.toString(), k -> new java.util.concurrent.atomic.LongAdder()).increment();
  }
  private static String instrDump(java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.LongAdder> m) {
    StringBuilder b = new StringBuilder();
    for (java.util.Map.Entry<String, java.util.concurrent.atomic.LongAdder> en : m.entrySet()) {
      b.append(' ').append(en.getKey()).append('=').append(en.getValue().sum());
    }
    return b.toString();
  }
  private static void instrMaybeLog() {
    long now = System.currentTimeMillis();
    long last = INSTR_LAST_LOG_MS.get();
    if (now - last >= 3000 && INSTR_LAST_LOG_MS.compareAndSet(last, now)) {
      System.err.println("[AQRAFT-INSTR] t=" + now
          + " fast=" + INSTR_FAST.get()
          + " twoSided=" + INSTR_TWOSIDED.get()
          + " collapse=" + INSTR_COLLAPSE.get()
          + " donorMoved=" + INSTR_DONOR_MOVED.get()
          + " || 2sidedDonorLeg:" + instrDump(INSTR_2S_DONOR)
          + " || 2sidedPeerLeg:" + instrDump(INSTR_2S_PEER)
          + " || writeRedirect:" + instrDump(INSTR_WRITE_HOST));
    }
  }

  /** Per-slot record. Volatile, no synchronization: per-reply self-healing
   *  via the metadata in every GET response. */
  private static final class SlotEntry {
    volatile int state;
    volatile HostAndPort peer;
    SlotEntry() { this.state = SLOT_STABLE; this.peer = null; }
  }

  // ---- writes ----
  private JedisCommands jedisWrites;   // JedisCluster: drives SET/DEL/ZADD during load

  // ---- reads (per-thread state) ----
  /** One Jedis connection per cluster master, lazily built in init(). */
  private Map<HostAndPort, Jedis> conns;
  /** slot -> bootstrap owner (from CLUSTER SLOTS at init). */
  private HostAndPort[] slotOwner;
  /** slot -> migration state + peer endpoint (refreshed on every reply). */
  private SlotEntry[] slotCache;

  // ---- bootstrap seed ----
  private HostAndPort seedHostPort;
  private Integer timeoutMs;

  /** Single-thread executor used to fan out the recipient probe in parallel
   *  with the donor read whenever the cached slot state is non-STABLE. */
  private ExecutorService peerProbeExec;

  // ---- linearizability-history support (LinHistoryClient) ----
  /** Outcome of the last op on this instance, as seen by the client.
   *  FAIL = the server definitely did not execute it (safe to count as
   *  never-happened); UNKNOWN = it may or may not have taken effect (timeout,
   *  connection drop mid-command, reply out of step). */
  public enum Outcome { OK, FAIL, UNKNOWN }

  /** redis.retry.ambiguous (default true = YCSB behavior). When false,
   *  execForSlot only retries errors that prove the command was not executed
   *  (MOVED, ASK, TRYAGAIN, CLUSTERDOWN, LOADING, NOTLEADER, connect failure)
   *  and returns UNKNOWN on the first ambiguous one. Retrying a write that may
   *  already have committed can re-apply it later, which a linearizability
   *  checker reports as a violation the server never made. */
  private boolean retryAmbiguous = true;
  private Outcome lastOutcome = Outcome.OK;

  public Outcome lastOutcome() {
    return lastOutcome;
  }

  /** Value + outcome of one client-level op. value == null with OK = absent. */
  public static final class OpResult {
    public final Outcome outcome;
    public final String value;
    OpResult(Outcome outcome, String value) {
      this.outcome = outcome;
      this.value = value;
    }
  }

  public void init() throws DBException {
    Properties props = getProperties();
    int port;

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = Protocol.DEFAULT_PORT;
    }
    String host = props.getProperty(HOST_PROPERTY);
    String redisTimeout = props.getProperty(TIMEOUT_PROPERTY);
    if (redisTimeout != null) {
      timeoutMs = Integer.parseInt(redisTimeout);
    }
    seedHostPort = new HostAndPort(host, port);
    retryAmbiguous = Boolean.parseBoolean(props.getProperty("redis.retry.ambiguous", "true"));

    boolean clusterEnabled = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY));
    if (clusterEnabled) {
      Set<HostAndPort> seeds = new HashSet<>();
      seeds.add(seedHostPort);
      // JedisCluster drives writes (workload-load inserts). Reads bypass it.
      // JMX off for its connection pools: every YCSB thread builds its own
      // JedisCluster (one pool per cluster node), and commons-pool2 registers
      // each pool as an MBean under a name it finds by trying "pool", "pool1",
      // "pool2", ... behind one global lock. With 200 threads that is ~2,000
      // pools and a quadratic number of attempts: the threads queue on the MBean
      // server for 60-100 s before the first operation.
      GenericObjectPoolConfig poolCfg = new GenericObjectPoolConfig();
      poolCfg.setJmxEnabled(false);
      jedisWrites = new JedisCluster(seeds, poolCfg);
      bootstrapDoubleReadState();
    } else {
      Jedis j = (timeoutMs != null) ? new Jedis(host, port, timeoutMs) : new Jedis(host, port);
      j.connect();
      jedisWrites = j;
      // Non-cluster mode: keep a single-connection setup.
      slotOwner = new HostAndPort[16384];
      slotCache = new SlotEntry[16384];
      for (int i = 0; i < 16384; i++) {
        slotOwner[i] = seedHostPort;
        slotCache[i] = new SlotEntry();
      }
      conns = new ConcurrentHashMap<>();
      conns.put(seedHostPort, j);
    }

    String password = props.getProperty(PASSWORD_PROPERTY);
    if (password != null) {
      ((BasicCommands) jedisWrites).auth(password);
    }

    peerProbeExec = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "redis-peer-probe");
      t.setDaemon(true);
      return t;
    });
  }

  /** Populate slotOwner[] from CLUSTER SLOTS and slotCache[] from CLUSTER
   *  SLOTSTATE. Open one Jedis per master observed in CLUSTER SLOTS. */
  @SuppressWarnings("unchecked")
  private void bootstrapDoubleReadState() throws DBException {
    slotOwner = new HostAndPort[16384];
    slotCache = new SlotEntry[16384];
    for (int i = 0; i < 16384; i++) slotCache[i] = new SlotEntry();
    conns = new ConcurrentHashMap<>();

    Jedis seed = (timeoutMs != null)
        ? new Jedis(seedHostPort.getHost(), seedHostPort.getPort(), timeoutMs)
        : new Jedis(seedHostPort.getHost(), seedHostPort.getPort());
    seed.connect();

    // CLUSTER SLOTS -> slotOwner[] + initial conns
    try {
      seed.getClient().cluster(new byte[][]{SafeEncoder.encode("SLOTS")});
      Object reply = seed.getClient().getOne();
      List<Object> ranges = (List<Object>) reply;
      for (Object rangeObj : ranges) {
        List<Object> range = (List<Object>) rangeObj;
        long startSlot = (Long) range.get(0);
        long endSlot = (Long) range.get(1);
        List<Object> masterInfo = (List<Object>) range.get(2);
        String masterHost = SafeEncoder.encode((byte[]) masterInfo.get(0));
        long masterPort = (Long) masterInfo.get(1);
        HostAndPort hp = new HostAndPort(masterHost, (int) masterPort);
        for (int s = (int) startSlot; s <= (int) endSlot; s++) {
          slotOwner[s] = hp;
          if (SHARED_BOOT_OWNER[s] == null) SHARED_BOOT_OWNER[s] = hp;  // donor snapshot for the poller
        }
        try { getOrOpen(hp); } catch (JedisConnectionException ce) { DEAD_HOSTS.add(hp.getHost()); }
        POLL_TARGETS.add(hp);   // poller must query EVERY master (no node has the full post-migration map)
        DONOR_HOSTS.add(hp.getHost());   // bootstrap owners are all donor-cluster nodes
        // AqRaft failover fix: also learn the REPLICAS of each range. On a
        // leader crash a replica is promoted; without its endpoint the client
        // only knows the dead master and can NEVER re-resolve (throughput
        // flatlines). Adding replicas to POLL_TARGETS lets the poller/refresh
        // query them — the promoted replica reports itself as master in its own
        // CLUSTER SLOTS, so ownership re-resolves to the new leader.
        for (int ri = 3; ri < range.size(); ri++) {
          try {
            List<Object> rep = (List<Object>) range.get(ri);
            String rh = SafeEncoder.encode((byte[]) rep.get(0));
            long rp = (Long) rep.get(1);
            HostAndPort rhp = new HostAndPort(rh, (int) rp);
            POLL_TARGETS.add(rhp);
            DONOR_HOSTS.add(rhp.getHost());
          } catch (Exception ignore) { /* skip malformed replica entry */ }
        }
      }
    } catch (Exception e) {
      throw new DBException("CLUSTER SLOTS bootstrap failed: " + e.getMessage());
    }

    // CLUSTER SLOTSTATE -> slotCache[]. Optional: may be empty / unsupported.
    try {
      seed.getClient().cluster(new byte[][]{SafeEncoder.encode("SLOTSTATE")});
      Object reply = seed.getClient().getOne();
      if (reply instanceof List) {
        List<Object> entries = (List<Object>) reply;
        for (Object e : entries) {
          List<Object> tup = (List<Object>) e;
          int slot = (int) (long) (Long) tup.get(0);
          int state = (int) (long) (Long) tup.get(1);
          byte[] peerBytes = (byte[]) tup.get(2);
          String peer = (peerBytes != null) ? SafeEncoder.encode(peerBytes) : "";
          SlotEntry se = slotCache[slot];
          se.state = state;
          se.peer = peer.isEmpty() ? null : HostAndPort.parseString(peer);
          if (se.peer != null) {
            try { getOrOpen(se.peer); }
            catch (JedisConnectionException ce) { DEAD_HOSTS.add(se.peer.getHost()); }
          }
        }
      }
    } catch (Exception e) {
      System.err.println("WARN: CLUSTER SLOTSTATE failed (treating all slots as STABLE): "
          + e.getMessage());
    }

    seed.close();

    /* Start the shared proactive-collapse poller (option #2). Interval from
     * redis.slotpoll.ms (default 100ms; set 0 to disable for an A/B baseline). */
    int pollMs = 100;
    try {
      String p = getProperties().getProperty("redis.slotpoll.ms");
      if (p != null) pollMs = Integer.parseInt(p.trim());
    } catch (Exception ignore) { /* keep default */ }
    startSlotPollerOnce(seedHostPort, timeoutMs, pollMs);

    /* redis.preconnect=ip:port,ip:port,...: open this thread's connection to nodes
     * it will be redirected to later (the scale-out's recipient replicas). Without
     * it every worker thread connects to a recipient leader at the moment that
     * leader first serves traffic; the leader accepts 400 connections before it
     * answers anything, and all threads queue behind that (a ~25% dip for 0.2 s
     * at the first round of the busiest recipient, 2026-10-05). Use the IPs the
     * servers put in their MOVED replies, or the connection is not reused. */
    String pre = getProperties().getProperty("redis.preconnect");
    if (pre != null && conns != null) {
      for (String e : pre.split(",")) {
        e = e.trim();
        int c = e.lastIndexOf(':');
        if (c <= 0) continue;
        try {
          getOrOpen(new HostAndPort(e.substring(0, c), Integer.parseInt(e.substring(c + 1))));
        } catch (Exception ignore) { /* not up (yet): connect lazily as before */ }
      }
    }
  }

  private synchronized Jedis getOrOpen(HostAndPort hp) {
    Jedis j = conns.get(hp);
    if (j != null) return j;
    j = (timeoutMs != null) ? new Jedis(hp.getHost(), hp.getPort(), timeoutMs)
                            : new Jedis(hp.getHost(), hp.getPort());
    j.connect();
    conns.put(hp, j);
    DEAD_HOSTS.remove(hp.getHost());
    return j;
  }

  /** getOrOpen variant that returns null instead of throwing when the host
   * is unreachable. Used on the read path, which already tolerates a null
   * Jedis (treats that side as "no answer" and falls back to the peer). A
   * mid-migration connect failure should degrade the read, not kill the
   * worker thread. */
  private Jedis getOrOpenSafe(HostAndPort hp) {
    if (hp == null) return null;
    try {
      return getOrOpen(hp);
    } catch (JedisConnectionException ce) {
      evictConn(hp);
      return null;
    }
  }

  /** Drop a broken cached Jedis so the next getOrOpen reopens it. */
  private synchronized void evictConn(HostAndPort hp) {
    if (hp == null) return;
    Jedis bad = conns.remove(hp);
    if (bad != null) {
      try { bad.close(); } catch (Exception ignore) { /* drain */ }
    }
  }

  /* One CLUSTER SLOTS answer per probed node, shared by every worker thread of
   * this JVM and reused for SLOT_VIEW_TTL_MS. Each thread has its own slotOwner[]
   * and used to re-resolve every slot by itself, each time on a new connection:
   * after a leader died, 400 threads x the ~2700 slots of its group, all asking
   * the same nodes at once. Thread dumps taken 2.4-5.4 s after the kill (S5,
   * 2026-10-04) showed 280-350 of 400 threads waiting for a CLUSTER SLOTS reply,
   * and the clients stayed at zero for 4-5 s after the new leader was elected. */
  private static final long SLOT_VIEW_TTL_MS = 100;
  private static final class SlotView {
    private long ts;
    private int n = -1;            // -1: the node did not give a slot map
    private int[] lo;
    private int[] hi;
    private HostAndPort[] owner;
  }
  private static final ConcurrentHashMap<HostAndPort, SlotView> SLOT_VIEWS = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<HostAndPort, Object> SLOT_VIEW_LOCKS = new ConcurrentHashMap<>();

  @SuppressWarnings("unchecked")
  private static SlotView slotViewOf(HostAndPort probe, Integer soTimeoutMs) {
    SlotView v = SLOT_VIEWS.get(probe);
    if (v != null && System.currentTimeMillis() - v.ts <= SLOT_VIEW_TTL_MS) return v;
    Object lk = SLOT_VIEW_LOCKS.computeIfAbsent(probe, k -> new Object());
    synchronized (lk) {
      v = SLOT_VIEWS.get(probe);
      if (v != null && System.currentTimeMillis() - v.ts <= SLOT_VIEW_TTL_MS) return v;
      v = new SlotView();
      Jedis j = null;
      try {
        // Short CONNECT timeout so a dead endpoint fails in ms, not a full socket timeout.
        j = probeJedis(probe, soTimeoutMs);
        j.connect();
        j.getClient().cluster(new byte[][]{SafeEncoder.encode("SLOTS")});
        Object reply = j.getClient().getOne();
        if (reply instanceof List) {
          List<Object> ranges = (List<Object>) reply;
          int n = ranges.size();
          v.lo = new int[n];
          v.hi = new int[n];
          v.owner = new HostAndPort[n];
          for (int i = 0; i < n; i++) {
            List<Object> range = (List<Object>) ranges.get(i);
            v.lo[i] = (int) (long) (Long) range.get(0);
            v.hi[i] = (int) (long) (Long) range.get(1);
            List<Object> masterInfo = (List<Object>) range.get(2);
            v.owner[i] = new HostAndPort(SafeEncoder.encode((byte[]) masterInfo.get(0)),
                (int) (long) (Long) masterInfo.get(1));
          }
          v.n = n;
        }
      } catch (Exception ignore) {
        // Couldn't reach it (dead / unreachable) — remember so peers skip it too.
        markEndpointDead(probe);
        v.n = -1;
      } finally {
        if (j != null) { try { j.close(); } catch (Exception ignore) { /* drain */ } }
      }
      v.ts = System.currentTimeMillis();
      SLOT_VIEWS.put(probe, v);
      return v;
    }
  }

  /** Re-resolve slot ownership via CLUSTER SLOTS on any reachable host.
   * Used after a JedisConnectionException — typically when the donor
   * relinquishes a slot via RAFT.SHARDGROUP NARROW and the pinned client
   * connection gets dropped. Updates slotOwner[slot] and returns the new
   * owner, or null if no reachable host returns a mapping for this slot. */
  private HostAndPort refreshSlotOwner(int slot) {
    return refreshSlotOwner(slot, false);
  }

  /** movedByDonor: the donor itself just answered -MOVED for this slot, so a
   *  recipient owner is expected.
   *
   *  A recipient group is configured for its whole final range before it has
   *  received anything, and reports itself as the owner of all of it. When a
   *  donor migrates in several rounds, most of that range is still served by the
   *  donor. Taking the recipient's word after losing the donor leader sent reads
   *  and writes of not-yet-migrated slots to the recipient (stale reads, S2).
   *  So a recipient owner is accepted only if a DONOR node reports it, or the
   *  slot is already known to be moving; donor nodes are asked first, and an
   *  answer naming an endpoint known dead (a stale view of the crashed leader)
   *  is skipped. With no acceptable answer the caller retries. */
  @SuppressWarnings("unchecked")
  private HostAndPort refreshSlotOwner(int slot, boolean movedByDonor) {
    SlotEntry rse = (slotCache != null) ? slotCache[slot] : null;
    boolean moving = movedByDonor || SHARED_COLLAPSED[slot] || SHARED_PEER[slot] != null
        || (rse != null && rse.state != SLOT_STABLE);
    List<HostAndPort> all = new ArrayList<>();
    // Probe live cluster members (replicas included via POLL_TARGETS) BEFORE the
    // seed — the seed is often the crashed donor master, and probing it first
    // wastes a full socket timeout per re-resolve. A promoted replica answers
    // CLUSTER SLOTS with the new ownership.
    for (HostAndPort hp : POLL_TARGETS) {
      if (!all.contains(hp)) all.add(hp);
    }
    if (conns != null) {
      for (HostAndPort hp : conns.keySet()) {
        if (!all.contains(hp)) all.add(hp);
      }
    }
    if (!all.contains(seedHostPort)) all.add(seedHostPort);
    List<HostAndPort> probes = new ArrayList<>();
    for (HostAndPort hp : all) {
      if (DONOR_HOSTS.contains(hp.getHost())) probes.add(hp);
    }
    for (HostAndPort hp : all) {
      if (!DONOR_HOSTS.contains(hp.getHost())) probes.add(hp);
    }
    for (HostAndPort probe : probes) {
      boolean probeIsDonor = DONOR_HOSTS.contains(probe.getHost());
      if (!probeIsDonor && !moving) continue;
      // AqRaft follower-crash fix: skip an endpoint we just found dead — otherwise
      // every worker re-resolving at once blocks on the same dead node's timeout.
      if (endpointDead(probe)) continue;
      SlotView v = slotViewOf(probe, timeoutMs);
      if (v.n < 0) continue;                          // unreachable, or not a slot map
      for (int i = 0; i < v.n; i++) {
        if (slot < v.lo[i] || slot > v.hi[i]) continue;
        HostAndPort newOwner = v.owner[i];
        if (endpointDead(newOwner)) break;            // stale view: ask the next node
        if (!DONOR_HOSTS.contains(newOwner.getHost()) && !probeIsDonor && !moving) break;
        if (slotOwner != null) {
          /* The other slots of this range that still point at the same dead
           * endpoint get the same answer from this same reply: move them now
           * instead of one refresh (one CLUSTER SLOTS) per slot. Only when the
           * per-slot rule above cannot differ between them. */
          HostAndPort old = slotOwner[slot];
          if (old != null && !old.equals(newOwner) && endpointDead(old)
              && (probeIsDonor || DONOR_HOSTS.contains(newOwner.getHost()))) {
            for (int s2 = v.lo[i]; s2 <= v.hi[i] && s2 < slotOwner.length; s2++) {
              if (old.equals(slotOwner[s2])) slotOwner[s2] = newOwner;
            }
          }
          slotOwner[slot] = newOwner;
        }
        return newOwner;
      }
    }
    return null;
  }

  public void cleanup() throws DBException {
    try {
      if (peerProbeExec != null) peerProbeExec.shutdownNow();
      if (jedisWrites instanceof Closeable) {
        ((Closeable) jedisWrites).close();
      }
      if (conns != null) {
        for (Jedis j : conns.values()) {
          try { j.close(); } catch (Exception ignore) { /* drain */ }
        }
      }
    } catch (IOException e) {
      throw new DBException("Closing connection failed.");
    }
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  /* Concatenate all field byte-values, in iteration order, into a single
   * string. With fieldcount=N and fieldlength=L (YCSB defaults: 10 / 100),
   * the result is exactly N*L bytes — the value size the workload defines. */
  private static String concatFields(Map<String, ByteIterator> values) {
    StringBuilder sb = new StringBuilder();
    for (ByteIterator v : values.values()) {
      sb.append(v.toString());
    }
    return sb.toString();
  }

  /** Reply from one GET attempt — value plus metadata for cache refresh. */
  private static final class GetReply {
    int state;        // SLOT_STABLE | SLOT_MIGRATING | SLOT_MIGRATED
    HostAndPort peer; // null when STABLE
    String value;     // null when key missing
    boolean ok;       // false on connection / parse failure
    long flags;       // AqRaft 4th slot-meta element: bit0 merged here, bit1 tombstoned here
    boolean moved;    // the node answered -MOVED: `peer` is the target, `value` is NOT an answer

    /** A nil from this node is final: it has merged the slot's migrated data,
     *  or the key was deleted here. Only then may the client skip the donor. */
    boolean authoritative() {
      return (flags & 3) != 0;
    }
  }

  /** Issue GET and parse either the slot-meta-wrapped array reply or a
   *  plain bulk reply (when slot_meta_reply is off on the server). */
  private GetReply sendGet(Jedis j, String key) {
    GetReply r = sendGetOnce(j, key);
    /* A -MOVED that stays on the same side is a leader change, not the slot
     * moving away: a donor follower pointing at the new donor leader, or a
     * recipient follower pointing at its leader. Follow it and return that
     * node's answer. Only donor -> recipient (the slot narrowed) is left to the
     * caller as a "moved" reply. Taking a donor -> donor -MOVED for NARROW made
     * the new donor leader pass for the recipient: reads returned its frozen
     * copy and writes were pinned to it (it redirects every write) until NARROW. */
    Jedis cur = j;
    for (int hop = 0; hop < 3 && r.ok && r.moved && r.peer != null; hop++) {
      boolean fromDonor = DONOR_HOSTS.contains(cur.getClient().getHost());
      boolean toDonor = DONOR_HOSTS.contains(r.peer.getHost());
      if (fromDonor != toDonor) return r;
      HostAndPort target = r.peer;
      Jedis nj = getOrOpenSafe(target);
      if (nj == null) { r.ok = false; return r; }
      if (fromDonor && slotOwner != null) slotOwner[JedisClusterCRC16.getSlot(key)] = target;
      cur = nj;
      r = sendGetOnce(cur, key);
    }
    if (r.ok && r.moved && r.peer != null
        && DONOR_HOSTS.contains(cur.getClient().getHost()) == DONOR_HOSTS.contains(r.peer.getHost())) {
      r.ok = false;   // still being redirected within one side: no answer
    }
    return r;
  }

  @SuppressWarnings("unchecked")
  private GetReply sendGetOnce(Jedis j, String key) {
    GetReply r = new GetReply();
    r.state = SLOT_STABLE;
    r.peer = null;
    r.value = null;
    r.ok = false;
    synchronized (j) {
    try {
      j.getClient().get(SafeEncoder.encode(key));
      Object reply = j.getClient().getOne();
      if (reply == null) {
        // Plain $-1 nil bulk — server.slot_meta_reply is off.
        r.ok = true;
        return r;
      }
      if (reply instanceof byte[]) {
        // Plain bulk reply (knob off).
        r.value = new String((byte[]) reply, StandardCharsets.UTF_8);
        r.ok = true;
        return r;
      }
      if (reply instanceof List) {
        List<Object> tup = (List<Object>) reply;
        if (tup.size() >= 3) {
          Object stateO = tup.get(0);
          Object peerO  = tup.get(1);
          Object valueO = tup.get(2);
          r.state = (stateO instanceof Long) ? (int) (long) (Long) stateO : SLOT_STABLE;
          if (peerO instanceof byte[]) {
            byte[] pb = (byte[]) peerO;
            if (pb.length > 0) {
              try {
                r.peer = HostAndPort.parseString(new String(pb, StandardCharsets.UTF_8));
              } catch (Exception ignore) {
                r.peer = null;
              }
            }
          }
          if (valueO instanceof byte[]) {
            r.value = new String((byte[]) valueO, StandardCharsets.UTF_8);
          }
          if (tup.size() >= 4 && tup.get(3) instanceof Long) {
            r.flags = (Long) tup.get(3);
          }
          r.ok = true;
          return r;
        }
      }
      // Unknown shape — treat as a miss.
      r.ok = true;
      return r;
    } catch (JedisConnectionException ce) {
      /* A read timeout leaves this GET's reply in flight on the socket. If the
       * connection stayed in conns, the NEXT command on it (often a SET) would
       * read this GET's slot-meta array as its own reply -> ClassCastException
       * in Jedis, which killed the worker thread (S2). Drop the socket; Jedis
       * reconnects on the next use, so no stale reply can reach another command. */
      dropConn(j);
      return r;
    } catch (redis.clients.jedis.exceptions.JedisMovedDataException mv) {
      // Donor told us this slot moved. Treat it as if the donor returned
      // a slot-meta "migrating" reply: mark the slot as in-flux with the
      // new owner as peer. The caller's read() path then exercises its
      // existing peer-fallback / parallel-fanout machinery, AND
      // updateSlotCache() refreshes slotOwner[slot] to the new owner so
      // future reads on this slot route directly there.
      //
      // Without this, MOVED was caught by the generic JedisException
      // handler below and the slot's stale slotOwner entry would persist
      // forever — every subsequent read of any key in this slot would
      // hit the wrong (donor) node and silently fail with Status.ERROR.
      HostAndPort target = mv.getTargetNode();
      if (target != null) {
        INSTR_DONOR_MOVED.incrementAndGet();
        r.state = SLOT_MIGRATED;
        r.peer  = target;
        r.moved = true;
        try { getOrOpen(target); }
        catch (JedisConnectionException ce) { DEAD_HOSTS.add(target.getHost()); }
        r.ok    = true;
      }
      return r;
    } catch (JedisException je) {
      dropConn(j);   /* same reason: the reply stream may be out of step */
      return r;
    }
    }
  }

  /** Close a connection whose reply stream may be out of step with its
   *  requests. The Jedis object stays usable: it reconnects on the next command. */
  private static void dropConn(Jedis j) {
    try { j.disconnect(); } catch (Exception ignore) { }
  }

  /** Update the slot cache from a successful GET reply's metadata. When the
   * reply indicates the slot is migrating/migrated away, also retarget
   * slotOwner[slot] to the new peer so subsequent writes route there
   * directly — no MOVED needed, no global cache invalidation. */
  private void updateSlotCache(int slot, GetReply r) {
    if (!r.ok) return;
    SlotEntry se = slotCache[slot];
    /* AqRaft Patch 30: a -MOVED on a READ means the donor has NARROWed this
     * slot away — migration is finalized and the recipient (r.peer) is now
     * the sole STABLE owner. Collapse the slot to the single-read fast path:
     * route directly to the recipient and drop the double-read fanout.
     * (During the migration window the donor still serves reads locally and
     * returns slot-meta MIGRATING — not MOVED — so SLOT_MIGRATED only ever
     * arrives post-NARROW.) Without this the slot stays at SLOT_MIGRATED
     * forever: every read keeps fanning out to donor + recipient, and the
     * narrowed donor leg fails (READ-FAILED, ~2-3ms), which is what pinned
     * post-migration throughput at ~half baseline. */
    if (r.state == SLOT_MIGRATED && r.peer != null) {
      INSTR_COLLAPSE.incrementAndGet();
      /* AqRaft fix: route to the recipient's LEADER, not the round-robin -MOVED
       * target (~2/3 of which are followers → follower-proxy ~11-15ms). Prefer
       * SHARED_PEER[slot] — the recipient leader already learned from the window
       * slot-meta — which is FREE. Only fall back to refreshSlotOwner (a CLUSTER
       * SLOTS round-trip) if we never saw the slot in the window. This matters:
       * keeping slotOwner on the donor through the window (correct double-read)
       * means many more post-NARROW collapses, and a CLUSTER SLOTS per collapse
       * (1.8M of them) was tanking throughput. */
      HostAndPort leader = SHARED_PEER[slot];
      if (leader == null) leader = refreshSlotOwner(slot, true);
      HostAndPort target = (leader != null) ? leader : r.peer;
      if (slotOwner != null) slotOwner[slot] = target;
      if (AQDBG.incrementAndGet() % 300000 == 0)
        System.err.println("[AQDBG-RDMIG] slot=" + slot + " set slotOwner=" + target
          + " sharedPeer=" + SHARED_PEER[slot] + " rpeer=" + r.peer);
      try { getOrOpen(target); }
      catch (JedisConnectionException ce) { DEAD_HOSTS.add(target.getHost()); }
      se.state = SLOT_STABLE;
      se.peer = null;
      return;
    }
    se.state = r.state;
    se.peer = r.peer;
    if (r.peer != null) {
      try { getOrOpen(r.peer); }
      catch (JedisConnectionException ce) { DEAD_HOSTS.add(r.peer.getHost()); }
      /* AqRaft fix: do NOT flip slotOwner to the peer during the migration
       * window. slotOwner is the DONOR leg of the double read; flipping it to
       * the recipient here made BOTH legs hit the recipient (the donor's copy
       * was never read — the "double read" degenerated to reading the recipient
       * twice). During the window slotOwner must stay the donor leader so reads
       * are a true donor+recipient double read. slotOwner only moves to the
       * recipient LEADER at NARROW, in the SLOT_MIGRATED collapse branch above. */
    }
  }

  /** Send a write command to slotOwner[slot]. If the donor returns MOVED
   * (we haven't learned the new owner from a prior read yet), parse it,
   * update slotOwner[slot] for this slot only, and retry once. No global
   * slot-cache invalidation, no CLUSTER SLOTS call. */
  private interface JedisOp {
    Object run(Jedis j);
  }

  private Object execForSlot(String key, JedisOp op) {
    int slot = JedisClusterCRC16.getSlot(key);
    // AqRaft proactive collapse: poller saw this slot narrow → write straight
    // to the recipient, skipping the donor -MOVED rediscovery.
    if (slotOwner != null && SHARED_COLLAPSED[slot] && SHARED_PEER[slot] != null) {
      if (DEAD_HOSTS.contains(SHARED_PEER[slot].getHost())) {
        SHARED_PEER[slot] = null;   // recipient leader crashed — force poller re-learn
      } else {
        slotOwner[slot] = SHARED_PEER[slot];
        if (AQDBG.incrementAndGet() % 300000 == 0)
          System.err.println("[AQDBG-COLLAPSE] slot=" + slot + " slotOwner<-sharedPeer=" + SHARED_PEER[slot]);
      }
    }
    followDonorLeaderChange(slot);
    HostAndPort hp = (slotOwner != null) ? slotOwner[slot] : seedHostPort;
    boolean ask = false;
    lastOutcome = Outcome.OK;
    boolean ambiguousSeen = false;   // a prior attempt may have taken effect
    /* The peer-probe thread (parallel-read path) may be using these same
     * Jedis instances. Synchronize per-connection so reply pipelining
     * doesn't interleave + corrupt Jedis's protocol parser. Loop bounded
     * to handle a brief cascade of ASK/MOVED while the donor + recipient
     * concurrently swap ownership. Budget is large (200) because transient
     * redisraft errors ("TIMEOUT no reply from leader") during a deep
     * migration-window dip can persist for ~1-2 s; at ~10 ms/retry that is
     * up to ~2 s of patience before giving up, enough to ride out a dip
     * without killing the worker thread. MOVED/ASK loops can't run away
     * now that Patch 25 stopped the donor↔recipient redirect ping-pong. */
    /* AqRaft robustness: budget sized to ride out a migrating slot's
     * unavailability. At 30M (~1831 keys/slot) a slot's flip can keep it
     * un-writable for ~10s — longer than the old 200-retry (~2s) budget, which
     * made worker threads throw `exhausted retries` and DIE, collapsing the run
     * to 0. 1500 covers ~15s at ~10ms/retry so the write rides out the window
     * and lands. Does not touch routing/collapse — only how long we wait. */
    for (int attempt = 0; attempt < 1500; attempt++) {
      if (hp == null) hp = seedHostPort;
      boolean connDropped = false;
      boolean ambiguous = false;
      HostAndPort badHp = null;
      Jedis j;
      /* getOrOpen does j.connect(), which can throw JedisConnectionException
       * if the target host is briefly unreachable (e.g., mid-migration when
       * a slot owner is being swapped). Previously this throw escaped the
       * retry loop and killed the YCSB worker thread. Catch it here so a
       * transient connect failure triggers the same evict + re-resolve +
       * retry path as an in-flight drop. */
      try {
        j = getOrOpen(hp);
      } catch (JedisConnectionException ce) {
        DEAD_HOSTS.add(hp.getHost());
        markEndpointDead(hp);   // so another node's stale view naming it is not taken
        evictConn(hp);
        HostAndPort newOwner = refreshSlotOwner(slot);
        hp = (newOwner != null) ? newOwner : seedHostPort;
        ask = false;
        try { Thread.sleep(5); } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
        continue;
      }
      synchronized (j) {
        try {
          if (ask) {
            j.asking();
            ask = false;
          }
          return op.run(j);
        } catch (redis.clients.jedis.exceptions.JedisMovedDataException mv) {
          /* Retry THIS write against the -MOVED target: it completes in both the
           * migration window (donor redirects to sg4 via WRITE_FLIP) and
           * post-NARROW (target is an sg4 node, which owns the slot or
           * follower-proxies it). Do NOT resolve via CLUSTER SLOTS for the retry
           * — during the window it still returns the donor, which -MOVEDs back,
           * producing a donor<->sg4 ping-pong that exhausts the retry budget.
           *
           * AqRaft fix: do NOT pin slotOwner[] to the round-robin -MOVED target.
           * It is often a FOLLOWER, and reads share slotOwner[], so pinning a
           * follower here makes every subsequent READ of the slot pay
           * follower-proxy (~11-15ms vs ~230us at the leader). Leave slotOwner[]
           * to the read-collapse path, which resolves the LEADER via CLUSTER
           * SLOTS once the migration has finalized. */
          /* AqRaft fix: route the write to the RECIPIENT LEADER. The donor's
           * -MOVED round-robins across all recipient-shardgroup nodes, so its
           * target is a FOLLOWER ~2/3 of the time → follower-proxy. The slot
           * meta peer (set by reads, slotCache[slot].peer) is the recipient
           * LEADER, which owns the migrating slot for writes (WRITE_FLIP), so
           * the write commits there directly. Fall back to the -MOVED target
           * only if no peer is known yet (e.g. a write before any read of the
           * slot during the window). */
          HostAndPort sharedLeader = SHARED_PEER[slot];
          SlotEntry mse = (slotCache != null) ? slotCache[slot] : null;
          boolean stableSlot = (mse == null) || mse.state == SLOT_STABLE;
          if (AQDBG.incrementAndGet() % 300000 == 0)
            System.err.println("[AQDBG-MOVED] slot=" + slot + " state=" + (mse==null?-1:mse.state)
              + " collapsed=" + SHARED_COLLAPSED[slot] + " sharedPeer=" + SHARED_PEER[slot]
              + " slotOwner=" + (slotOwner==null?null:slotOwner[slot])
              + " movedTarget=" + mv.getTargetNode().getHost()+":"+mv.getTargetNode().getPort());
          /* A hint is only used if it can be the recipient leader: not the node
           * that just sent this -MOVED, not a donor, not a host seen dead. */
          final HostAndPort redirector = hp;
          if (usableWriteHint(sharedLeader, redirector)) {
            hp = sharedLeader;                         // cross-thread leader hint
          } else if (mse != null && usableWriteHint(mse.peer, redirector)) {
            hp = mse.peer;                             // this thread's leader hint
          } else {
            hp = new HostAndPort(mv.getTargetNode().getHost(),
                                 mv.getTargetNode().getPort()); // -MOVED target = leader (steady state)
          }
          /* AqRaft write-leader-pin: for a STEADY-STATE slot the -MOVED target IS
           * the shardgroup leader (redisraft redirects a follower write to its
           * leader). Pin slotOwner[slot] so subsequent writes go straight to the
           * leader instead of re-bouncing through the follower on EVERY write
           * the post-migration 30M write-MOVED storm (tens of millions of
           * redirects). Pinning the leader also helps reads (a valid local read
           * target). Skipped while the slot is migrating, where the -MOVED target
           * can round-robin to a follower and reads share slotOwner[]. */
          if (stableSlot && slotOwner != null) {
            slotOwner[slot] = hp;
          }
          instrCount(INSTR_WRITE_HOST, hp);   // write redirect -> should be LEADER
          ask = false;
        } catch (redis.clients.jedis.exceptions.JedisAskDataException ax) {
          hp = new HostAndPort(ax.getTargetNode().getHost(),
                               ax.getTargetNode().getPort());
          ask = true;
        } catch (JedisConnectionException ce) {
          /* TCP-level drop. Common after RAFT.SHARDGROUP NARROW: the donor
           * surrenders the slot to sg4 and the pinned client connection
           * gets reset. Evict + re-resolve outside the synchronized block. */
          connDropped = true;
          ambiguous = true;   // the command may have reached the server
          badHp = hp;
        } catch (ClassCastException cce) {
          /* Reply of the wrong type: this connection's reply stream is out of
           * step (a late reply from an earlier command). Drop it and retry the
           * same owner on a fresh socket instead of letting the exception kill
           * the worker thread. */
          dropConn(j);
          if (!retryAmbiguous) {
            lastOutcome = Outcome.UNKNOWN;
            return null;
          }
          ambiguousSeen = true;
          continue;
        } catch (redis.clients.jedis.exceptions.JedisDataException de) {
          /* Transient redisraft errors during the migration window:
           *   - "TIMEOUT no reply from leader": leader saturated (deep dip)
           *     or a brief leader election; the command never committed.
           *   - "TRYAGAIN": slot is MIGRATING/IMPORTING and keys are
           *     mid-transfer.
           *   - "CLUSTERDOWN": momentary loss of quorum.
           * All are retryable — the write simply hasn't landed yet. Sleep a
           * little and retry the same owner. Previously these JedisData
           * exceptions escaped the loop and killed the YCSB worker thread,
           * which collapsed the whole run mid-migration. Genuine errors
           * (wrong type, syntax) are not expected on a pure SET/GET workload,
           * so retrying is safe here. */
          String msg = de.getMessage();
          /* "TIMEOUT not committed yet" / "TIMEOUT no reply from leader": the
           * entry may already be in the Raft log and commit later, so the write
           * is ambiguous. The other errors are replied before the command is
           * appended (NOTLEADER "Failed to proxy" = never forwarded). */
          boolean definite = msg != null && (msg.contains("TRYAGAIN")
              || msg.contains("CLUSTERDOWN") || msg.contains("LOADING")
              || msg.contains("NOTLEADER"));
          if (!definite && !retryAmbiguous) {
            lastOutcome = Outcome.UNKNOWN;
            return null;
          }
          if (!definite) ambiguousSeen = true;
          if (msg != null && (msg.contains("TIMEOUT") || msg.contains("TRYAGAIN")
              || msg.contains("CLUSTERDOWN") || msg.contains("LOADING")
              || msg.contains("NOTLEADER") || msg.contains("Failed to proxy")
              || msg.contains("LEADERSHIP"))) {
            /* NOTLEADER / "Failed to proxy command": redisraft follower-proxy
             * couldn't reach the leader, typically a brief window during the
             * NARROW ownership hand-off or a leader re-election. Re-resolve
             * the slot owner and retry rather than killing the worker. */
            HostAndPort reResolved = refreshSlotOwner(slot);
            if (reResolved != null) hp = reResolved;
            try { Thread.sleep(10); } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
            }
            /* keep hp, ask unchanged — retry the same leader */
          } else {
            lastOutcome = ambiguousSeen ? Outcome.UNKNOWN : Outcome.FAIL;
            throw de;
          }
        }
      }
      if (connDropped) {
        evictConn(badHp);
        if (ambiguous && !retryAmbiguous) {
          lastOutcome = Outcome.UNKNOWN;
          return null;
        }
        if (ambiguous) ambiguousSeen = true;
        HostAndPort newOwner = refreshSlotOwner(slot);
        hp = (newOwner != null) ? newOwner : seedHostPort;
        ask = false;
      }
    }
    /* AqRaft robustness: do NOT throw on exhaustion — that propagates out of
     * update()/read() and KILLS the YCSB worker thread (run collapses to 0).
     * Return null so the op is counted as a single ERROR and the thread keeps
     * serving; the slot becomes writable again the moment its migration
     * finalizes. */
    System.err.println("[AQRAFT] execForSlot exhausted retries for slot=" + slot
        + " (op counted as ERROR, thread continues)");
    lastOutcome = ambiguousSeen ? Outcome.UNKNOWN : Outcome.FAIL;
    return null;
  }

  /** This slot's donor leg points at an endpoint that stopped answering, and the
   *  poller has learned the donor group's new leader: follow it. Without this,
   *  each thread found the new leader slot by slot (a failed connect, then a
   *  CLUSTER SLOTS round trip), and reads of the slot failed until it did. */
  private void followDonorLeaderChange(int slot) {
    if (slotOwner == null) return;
    HostAndPort cur = slotOwner[slot];
    HostAndPort now = SHARED_BOOT_OWNER[slot];
    if (cur == null || now == null || now.equals(cur)) return;
    if (!DONOR_HOSTS.contains(cur.getHost())) return;
    if (endpointDead(cur) && !endpointDead(now)) slotOwner[slot] = now;
  }

  private static boolean usableWriteHint(HostAndPort hint, HostAndPort redirector) {
    return hint != null && !hint.equals(redirector)
        && !DONOR_HOSTS.contains(hint.getHost()) && !DEAD_HOSTS.contains(hint.getHost());
  }

  private String setForSlot(String key, final String value) {
    Object r = execForSlot(key, j -> j.set(key, value));
    return (r instanceof String) ? (String) r : null;
  }

  private Long delForSlot(String key) {
    Object r = execForSlot(key, j -> j.del(key));
    return (r instanceof Long) ? (Long) r : Long.valueOf(0);
  }

  private Long zaddForSlot(String indexKey, final double score, final String member) {
    Object r = execForSlot(indexKey, j -> j.zadd(indexKey, score, member));
    return (r instanceof Long) ? (Long) r : Long.valueOf(0);
  }

  private Long zremForSlot(String indexKey, final String member) {
    Object r = execForSlot(indexKey, j -> j.zrem(indexKey, member));
    return (r instanceof Long) ? (Long) r : Long.valueOf(0);
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    // Use the requested field name as the result-map key so YCSB's
    // dataintegrity=true verifier can compare against
    // buildDeterministicValue(key, fieldname). When fields is null/empty,
    // fall back to "value" for backward compatibility.
    String resultField = "value";
    if (fields != null && !fields.isEmpty()) {
      resultField = fields.iterator().next();
    }

    if (slotOwner == null) {
      String val = ((JedisCommands) jedisWrites).get(key);
      if (val == null) return Status.ERROR;
      result.put(resultField, new StringByteIterator(val));
      return Status.OK;
    }

    OpResult r = readCore(key);
    if (r.outcome != Outcome.OK || r.value == null) return Status.ERROR;
    result.put(resultField, new StringByteIterator(r.value));
    return Status.OK;
  }

  /** GET through the Aqueduct routing (collapse / stable / double-read paths).
   *  OK+null means every leg the path consulted answered "no such key";
   *  UNKNOWN means a consulted leg failed and no value was found. */
  private OpResult readCore(String key) {
    int slot = JedisClusterCRC16.getSlot(key);
    /* AqRaft read rule. The donor keeps a frozen copy of a migrated slot (writes
     * flip to the recipient; EVICT is off), so a recipient nil may only be
     * resolved from the donor when the recipient has NOT merged the slot and the
     * key was NOT deleted there (GetReply.authoritative()). Falling back on any
     * recipient miss returned deleted keys' old values, during the migration
     * window and forever after NARROW. An unreachable recipient is UNKNOWN — the
     * donor's copy could be stale. */
    // AqRaft proactive collapse: the poller saw this slot narrow to the
    // recipient, so read the recipient first (skips the donor -MOVED storm).
    if (SHARED_COLLAPSED[slot] && SHARED_PEER[slot] != null) {
      slotOwner[slot] = SHARED_PEER[slot];
      SlotEntry pc = slotCache[slot];
      pc.state = SLOT_STABLE;
      pc.peer = null;
      Jedis rj = getOrOpenSafe(SHARED_PEER[slot]);
      long tp = System.nanoTime();
      GetReply rr = (rj != null) ? sendGet(rj, key) : null;
      instrRecordLat(SHARED_PEER[slot], System.nanoTime() - tp);
      if (rr != null && rr.value != null) {
        INSTR_COLLAPSE.incrementAndGet();
        return new OpResult(Outcome.OK, rr.value);
      }
      return resolveRecipientMiss(slot, key, rr, SHARED_PEER[slot]);
    }
    followDonorLeaderChange(slot);
    SlotEntry s = slotCache[slot];
    instrMaybeLog();

    HostAndPort ownerHp = slotOwner[slot];
    final Jedis ownerJ = getOrOpenSafe(ownerHp);

    // Stable-slot fast path: one read to the owner. If the owner is the donor
    // and says the slot is migrating, the recipient must be asked too.
    if (s.state == SLOT_STABLE || s.peer == null) {
      INSTR_FAST.incrementAndGet();
      long t0 = System.nanoTime();
      GetReply ra = (ownerJ != null) ? sendGet(ownerJ, key) : null;
      instrRecordLat(ownerHp, System.nanoTime() - t0);
      if (ra != null) updateSlotCache(slot, ra);
      if (ra == null || !ra.ok) return new OpResult(Outcome.UNKNOWN, null);
      boolean ownerIsDonor = ownerHp == null || DONOR_HOSTS.contains(ownerHp.getHost());
      if (!ownerIsDonor) {
        // Owner is the recipient (pinned after a write redirect).
        if (ra.value != null) return new OpResult(Outcome.OK, ra.value);
        return resolveRecipientMiss(slot, key, ra, ownerHp);
      }
      if (ra.state != SLOT_STABLE && ra.peer != null) {
        // Donor copy is frozen: the recipient's answer takes precedence.
        Jedis pj = getOrOpenSafe(ra.peer);
        GetReply rb = (pj != null) ? sendGet(pj, key) : null;
        if (rb == null || !rb.ok) return new OpResult(Outcome.UNKNOWN, null);
        if (rb.value != null) return new OpResult(Outcome.OK, rb.value);
        if (rb.authoritative()) return new OpResult(Outcome.OK, null);
      }
      if (ra.moved) return new OpResult(Outcome.UNKNOWN, null);   // -MOVED carries no value
      return new OpResult(Outcome.OK, ra.value);
    }

    // Migration path: donor + recipient GETs in parallel. The recipient's
    // answer wins; the donor's frozen copy only fills a non-authoritative miss.
    INSTR_TWOSIDED.incrementAndGet();
    final HostAndPort peerHp = s.peer;
    if (peerHp != null) SHARED_PEER[slot] = peerHp;  // window leader → shared so writes route here
    instrCount(INSTR_2S_DONOR, ownerHp);   // donor leg → should be donor leader
    instrCount(INSTR_2S_PEER, peerHp);     // peer leg → should be RECIPIENT LEADER
    final String keyFinal = key;
    Future<GetReply> peerFuture = peerProbeExec.submit(new Callable<GetReply>() {
      @Override
      public GetReply call() {
        Jedis pj = getOrOpenSafe(peerHp);
        return (pj != null) ? sendGet(pj, keyFinal) : null;
      }
    });
    GetReply ra = (ownerJ != null) ? sendGet(ownerJ, key) : null;
    GetReply rb;
    try {
      rb = peerFuture.get();
    } catch (Exception e) {
      rb = null;
    }

    // Cache update from donor reply only — slotOwner[] is the bootstrap owner,
    // so the donor's peer field is "the other side relative to slotOwner".
    // The recipient's peer field points back at the donor, which would invert
    // the cache.
    if (ra != null && ra.ok) {
      if (ra.state == SLOT_MIGRATING) {
        INSTR_DONOR_META_MIGRATING.incrementAndGet();
      } else if (ra.state == SLOT_MIGRATED) {
        INSTR_DONOR_META_MIGRATED.incrementAndGet();
      }
    }
    if (ra != null) updateSlotCache(slot, ra);

    if (rb == null || !rb.ok) return new OpResult(Outcome.UNKNOWN, null);
    if (rb.value != null) return new OpResult(Outcome.OK, rb.value);
    if (rb.authoritative()) return new OpResult(Outcome.OK, null);
    if (ra == null || !ra.ok || ra.moved) return new OpResult(Outcome.UNKNOWN, null);
    return new OpResult(Outcome.OK, ra.value);
  }

  /** The recipient answered `rr` with no value. Final if authoritative;
   *  otherwise the key was never touched there since the flip, so the donor's
   *  frozen copy (the bootstrap owner) is current. */
  private OpResult resolveRecipientMiss(int slot, String key, GetReply rr, HostAndPort recipient) {
    if (rr == null || !rr.ok || rr.moved) return new OpResult(Outcome.UNKNOWN, null);
    if (rr.authoritative()) return new OpResult(Outcome.OK, null);
    HostAndPort donor = SHARED_BOOT_OWNER[slot];
    if (donor == null || donor.equals(recipient)) return new OpResult(Outcome.OK, null);
    Jedis dj = getOrOpenSafe(donor);
    GetReply dr = (dj != null) ? sendGet(dj, key) : null;
    if (dr == null || !dr.ok || dr.moved) return new OpResult(Outcome.UNKNOWN, null);
    return new OpResult(Outcome.OK, dr.value);
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    String reply = ((JedisCommands) jedisWrites).set(key, concatFields(values));
    if (reply != null && reply.equals("OK")) {
      ((JedisCommands) jedisWrites).zadd(INDEX_KEY, hash(key), key);
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    if (slotOwner == null) {
      return ((JedisCommands) jedisWrites).del(key) == 0
          && ((JedisCommands) jedisWrites).zrem(INDEX_KEY, key) == 0 ? Status.ERROR
          : Status.OK;
    }
    Long delN = delForSlot(key);
    Long zremN = zremForSlot(INDEX_KEY, key);
    return (delN == 0 && zremN == 0) ? Status.ERROR : Status.OK;
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    /* YCSB update sends only the modified fields (typically 1 of N). With
     * the single-string representation we just SET the new concatenated
     * payload, replacing the prior contents. Route via slotOwner[] so
     * MOVED triggers a single-slot update instead of Jedis's global cache
     * rebuild. */
    if (slotOwner == null) {
      String reply = ((JedisCommands) jedisWrites).set(key, concatFields(values));
      return (reply != null && reply.equals("OK")) ? Status.OK : Status.ERROR;
    }
    String reply = setForSlot(key, concatFields(values));
    return (reply != null && reply.equals("OK")) ? Status.OK : Status.ERROR;
  }

  /** GET for LinHistoryClient: value (null = absent) + outcome. */
  public OpResult getValue(String key) {
    return readCore(key);
  }

  /** SET routed like update(); outcome distinguishes definite failure from
   *  ambiguous (see redis.retry.ambiguous). */
  public OpResult setValue(String key, String value) {
    String reply;
    try {
      reply = setForSlot(key, value);
    } catch (RuntimeException e) {
      return new OpResult(lastOutcome == Outcome.FAIL ? Outcome.FAIL : Outcome.UNKNOWN, null);
    }
    if (reply != null && reply.equals("OK")) return new OpResult(Outcome.OK, null);
    return new OpResult(lastOutcome == Outcome.OK ? Outcome.UNKNOWN : lastOutcome, null);
  }

  /** DEL of a single key (no index maintenance). */
  public OpResult delValue(String key) {
    Object r;
    try {
      r = execForSlot(key, j -> j.del(key));
    } catch (RuntimeException e) {
      return new OpResult(lastOutcome == Outcome.FAIL ? Outcome.FAIL : Outcome.UNKNOWN, null);
    }
    if (r instanceof Long) return new OpResult(Outcome.OK, String.valueOf(r));
    return new OpResult(lastOutcome == Outcome.OK ? Outcome.UNKNOWN : lastOutcome, null);
  }

  /** INCR; value is the post-increment counter. */
  public OpResult incrValue(String key) {
    Object r;
    try {
      r = execForSlot(key, j -> j.incr(key));
    } catch (RuntimeException e) {
      return new OpResult(lastOutcome == Outcome.FAIL ? Outcome.FAIL : Outcome.UNKNOWN, null);
    }
    if (r instanceof Long) return new OpResult(Outcome.OK, String.valueOf(r));
    return new OpResult(lastOutcome == Outcome.OK ? Outcome.UNKNOWN : lastOutcome, null);
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    Set<String> keys = ((JedisCommands) jedisWrites).zrangeByScore(INDEX_KEY,
        hash(startkey), Double.POSITIVE_INFINITY, 0, recordcount);

    HashMap<String, ByteIterator> values;
    for (String key : keys) {
      values = new HashMap<String, ByteIterator>();
      read(table, key, fields, values);
      result.add(values);
    }

    return Status.OK;
  }
}
