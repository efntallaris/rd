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
  private static void instrMaybeLog() {
    long now = System.currentTimeMillis();
    long last = INSTR_LAST_LOG_MS.get();
    if (now - last >= 3000 && INSTR_LAST_LOG_MS.compareAndSet(last, now)) {
      StringBuilder hl = new StringBuilder();
      for (java.util.Map.Entry<String, java.util.concurrent.atomic.LongAdder[]> en : INSTR_HOST_LAT.entrySet()) {
        long c = en.getValue()[1].sum();
        long us = (c > 0) ? (en.getValue()[0].sum() / c / 1000) : 0;
        hl.append(' ').append(en.getKey()).append("=").append(us).append("us/").append(c);
      }
      System.err.println("[AQRAFT-INSTR] t=" + now
          + " fast=" + INSTR_FAST.get()
          + " twoSided=" + INSTR_TWOSIDED.get()
          + " collapse=" + INSTR_COLLAPSE.get()
          + " donorMoved=" + INSTR_DONOR_MOVED.get()
          + " | fastReadLatByHost(cumulative):" + hl);
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

    boolean clusterEnabled = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY));
    if (clusterEnabled) {
      Set<HostAndPort> seeds = new HashSet<>();
      seeds.add(seedHostPort);
      // JedisCluster drives writes (workload-load inserts). Reads bypass it.
      jedisWrites = new JedisCluster(seeds);
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
        }
        getOrOpen(hp);
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
          if (se.peer != null) getOrOpen(se.peer);
        }
      }
    } catch (Exception e) {
      System.err.println("WARN: CLUSTER SLOTSTATE failed (treating all slots as STABLE): "
          + e.getMessage());
    }

    seed.close();
  }

  private synchronized Jedis getOrOpen(HostAndPort hp) {
    Jedis j = conns.get(hp);
    if (j != null) return j;
    j = (timeoutMs != null) ? new Jedis(hp.getHost(), hp.getPort(), timeoutMs)
                            : new Jedis(hp.getHost(), hp.getPort());
    j.connect();
    conns.put(hp, j);
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

  /** Re-resolve slot ownership via CLUSTER SLOTS on any reachable host.
   * Used after a JedisConnectionException — typically when the donor
   * relinquishes a slot via RAFT.SHARDGROUP NARROW and the pinned client
   * connection gets dropped. Updates slotOwner[slot] and returns the new
   * owner, or null if no reachable host returns a mapping for this slot. */
  @SuppressWarnings("unchecked")
  private HostAndPort refreshSlotOwner(int slot) {
    List<HostAndPort> probes = new ArrayList<>();
    probes.add(seedHostPort);
    if (conns != null) {
      for (HostAndPort hp : conns.keySet()) {
        if (!hp.equals(seedHostPort)) probes.add(hp);
      }
    }
    for (HostAndPort probe : probes) {
      Jedis j = null;
      try {
        j = (timeoutMs != null) ? new Jedis(probe.getHost(), probe.getPort(), timeoutMs)
                                : new Jedis(probe.getHost(), probe.getPort());
        j.connect();
        j.getClient().cluster(new byte[][]{SafeEncoder.encode("SLOTS")});
        Object reply = j.getClient().getOne();
        if (!(reply instanceof List)) continue;
        for (Object rangeObj : (List<Object>) reply) {
          List<Object> range = (List<Object>) rangeObj;
          long startSlot = (Long) range.get(0);
          long endSlot   = (Long) range.get(1);
          if (slot < startSlot || slot > endSlot) continue;
          List<Object> masterInfo = (List<Object>) range.get(2);
          String mh = SafeEncoder.encode((byte[]) masterInfo.get(0));
          long   mp = (Long) masterInfo.get(1);
          HostAndPort newOwner = new HostAndPort(mh, (int) mp);
          if (slotOwner != null) slotOwner[slot] = newOwner;
          return newOwner;
        }
      } catch (Exception ignore) {
        /* try next probe */
      } finally {
        if (j != null) { try { j.close(); } catch (Exception ignore) { /* drain */ } }
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
  }

  /** Issue GET and parse either the slot-meta-wrapped array reply or a
   *  plain bulk reply (when slot_meta_reply is off on the server). */
  @SuppressWarnings("unchecked")
  private GetReply sendGet(Jedis j, String key) {
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
          r.ok = true;
          return r;
        }
      }
      // Unknown shape — treat as a miss.
      r.ok = true;
      return r;
    } catch (JedisConnectionException ce) {
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
        getOrOpen(target);
        r.ok    = true;
      }
      return r;
    } catch (JedisException je) {
      return r;
    }
    }
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
      /* AqRaft fix: route to the recipient's LEADER, not the -MOVED target.
       * The donor's -MOVED round-robins across ALL recipient-shardgroup nodes
       * (raft.c getSlotShardGroup: sg->nodes[next_redir]), so ~2/3 of targets
       * are FOLLOWERS. With follower-proxy on, a read to a follower is
       * forwarded to the leader → ~11-15ms vs ~230us straight to the leader,
       * which is what pinned post-migration throughput below baseline.
       * CLUSTER SLOTS returns the leader as the range's first node, so
       * refreshSlotOwner() resolves+pins the leader (and sets slotOwner[slot]).
       * Done once per slot at collapse; steady-state reads then hit the leader
       * directly. Fall back to the -MOVED target if CLUSTER SLOTS is unreachable. */
      HostAndPort leader = refreshSlotOwner(slot);
      HostAndPort target = (leader != null) ? leader : r.peer;
      if (slotOwner != null) slotOwner[slot] = target;
      getOrOpen(target);
      se.state = SLOT_STABLE;
      se.peer = null;
      return;
    }
    se.state = r.state;
    se.peer = r.peer;
    if (r.peer != null) {
      getOrOpen(r.peer);
      if (r.state != SLOT_STABLE && slotOwner != null) {
        slotOwner[slot] = r.peer;
      }
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
    HostAndPort hp = (slotOwner != null) ? slotOwner[slot] : seedHostPort;
    boolean ask = false;
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
    for (int attempt = 0; attempt < 200; attempt++) {
      if (hp == null) hp = seedHostPort;
      boolean connDropped = false;
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
          hp = new HostAndPort(mv.getTargetNode().getHost(),
                               mv.getTargetNode().getPort());
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
          badHp = hp;
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
            throw de;
          }
        }
      }
      if (connDropped) {
        evictConn(badHp);
        HostAndPort newOwner = refreshSlotOwner(slot);
        hp = (newOwner != null) ? newOwner : seedHostPort;
        ask = false;
      }
    }
    throw new redis.clients.jedis.exceptions.JedisException(
        "execForSlot: exhausted retries for slot=" + slot);
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

    int slot = JedisClusterCRC16.getSlot(key);
    SlotEntry s = slotCache[slot];
    instrMaybeLog();

    HostAndPort ownerHp = slotOwner[slot];
    final Jedis ownerJ = getOrOpenSafe(ownerHp);

    // Stable-slot fast path: single donor read. May discover a state flip on
    // the reply; if so, follow up with the freshly-learned peer.
    if (s.state == SLOT_STABLE || s.peer == null) {
      INSTR_FAST.incrementAndGet();
      long t0 = System.nanoTime();
      GetReply ra = (ownerJ != null) ? sendGet(ownerJ, key) : null;
      instrRecordLat(ownerHp, System.nanoTime() - t0);
      if (ra != null) updateSlotCache(slot, ra);
      if (ra != null && ra.value != null) {
        result.put(resultField, new StringByteIterator(ra.value));
        return Status.OK;
      }
      if (ra != null && ra.state != SLOT_STABLE && ra.peer != null) {
        Jedis pj = getOrOpenSafe(ra.peer);
        GetReply rb = (pj != null) ? sendGet(pj, key) : null;
        if (rb != null && rb.value != null) {
          result.put(resultField, new StringByteIterator(rb.value));
          return Status.OK;
        }
      }
      return Status.ERROR;
    }

    // Migration path: fan out donor + recipient GETs in parallel. Recipient
    // wins when both have the key — the donor's copy is the older snapshot
    // once ownership has flipped to the recipient.
    INSTR_TWOSIDED.incrementAndGet();
    final HostAndPort peerHp = s.peer;
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

    if (rb != null && rb.value != null) {
      result.put(resultField, new StringByteIterator(rb.value));
      return Status.OK;
    }
    if (ra != null && ra.value != null) {
      result.put(resultField, new StringByteIterator(ra.value));
      return Status.OK;
    }
    return Status.ERROR;
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
