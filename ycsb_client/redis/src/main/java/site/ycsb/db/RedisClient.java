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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

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

  public void cleanup() throws DBException {
    try {
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
    } catch (JedisException je) {
      return r;
    }
  }

  /** Update the slot cache from a successful GET reply's metadata. */
  private void updateSlotCache(int slot, GetReply r) {
    if (!r.ok) return;
    SlotEntry se = slotCache[slot];
    se.state = r.state;
    se.peer = r.peer;
    if (r.peer != null) getOrOpen(r.peer);
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    if (slotOwner == null) {
      String val = ((JedisCommands) jedisWrites).get(key);
      if (val == null) return Status.ERROR;
      result.put("value", new StringByteIterator(val));
      return Status.OK;
    }

    int slot = JedisClusterCRC16.getSlot(key);
    SlotEntry s = slotCache[slot];

    // 1) Always try the bootstrap owner first.
    HostAndPort ownerHp = slotOwner[slot];
    Jedis ownerJ = (ownerHp != null) ? getOrOpen(ownerHp) : null;
    GetReply ra = (ownerJ != null) ? sendGet(ownerJ, key) : null;
    if (ra != null) updateSlotCache(slot, ra);

    if (ra != null && ra.value != null) {
      result.put("value", new StringByteIterator(ra.value));
      return Status.OK;
    }

    // 2) If owner returned nil and the slot is in a migration state, double-read
    //    the peer (= the other side of the migration).
    if (s.state != SLOT_STABLE && s.peer != null) {
      Jedis peerJ = getOrOpen(s.peer);
      GetReply rb = sendGet(peerJ, key);
      updateSlotCache(slot, rb);
      if (rb.value != null) {
        result.put("value", new StringByteIterator(rb.value));
        return Status.OK;
      }
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
    return ((JedisCommands) jedisWrites).del(key) == 0
        && ((JedisCommands) jedisWrites).zrem(INDEX_KEY, key) == 0 ? Status.ERROR
        : Status.OK;
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    /* YCSB update sends only the modified fields (typically 1 of N). With
     * the single-string representation we just SET the new concatenated
     * payload, replacing the prior contents. */
    String reply = ((JedisCommands) jedisWrites).set(key, concatFields(values));
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
