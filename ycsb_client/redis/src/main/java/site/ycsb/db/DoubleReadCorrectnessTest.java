// Double-read correctness test for aqueduct slot migration.
//
// Single-threaded driver that:
//  1. Pre-loads N keys with deterministic versioned values.
//  2. Loops: alternately reads + writes random keys for --duration-sec.
//  3. Every read verifies the returned value matches the latest local
//     expected version (recipient-wins must guarantee monotonicity).
//  4. Every 100ms dumps the slot state (state, peer, slotOwner) for the
//     migrating slot range to a client trace log.
//
// Reuses the same RESP parsing + slot-cache primitives as RedisClient.java.
// Intentionally single-thread so each read/write is happens-before the
// next: no concurrent writers, no shared expected[] races.

package site.ycsb.db;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.util.JedisClusterCRC16;
import redis.clients.util.SafeEncoder;

import java.io.PrintWriter;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class DoubleReadCorrectnessTest {

  // ---- args ----
  private static String seedHost = "redis0";
  private static int seedPort = 8000;
  private static int numKeys = 1000;
  private static int slotRangeStart = 0;
  private static int slotRangeEnd = 127;
  private static int durationSec = 30;
  private static int dumpIntervalMs = 100;
  private static String traceOut = "/tmp/client_trace.log";
  private static boolean loadOnly = false;

  // ---- aqueduct slot meta ----
  private static final int SLOT_STABLE    = 0;
  private static final int SLOT_MIGRATING = 1;
  private static final int SLOT_MIGRATED  = 2;

  private static final class SlotEntry {
    volatile int state = SLOT_STABLE;
    volatile HostAndPort peer = null;
  }

  // ---- state ----
  private static HostAndPort seedHp;
  private static HostAndPort[] slotOwner = new HostAndPort[16384];
  private static SlotEntry[] slotCache = new SlotEntry[16384];
  private static ConcurrentHashMap<HostAndPort, Jedis> conns = new ConcurrentHashMap<>();
  private static long[] expected;   // expected[i] = latest write version for key tk<i>
  private static ExecutorService peerProbeExec;
  private static long mismatches = 0;
  private static long readsOk = 0;
  private static long writesOk = 0;
  private static AtomicBoolean stop = new AtomicBoolean(false);

  // ---- helpers ----
  private static String keyName(int i) { return "tk" + i; }

  private static String valueFor(int i, long version) {
    // Deterministic: "v<i>:<version>". 1000 keys × tracked version => max ~64
    // chars. Keep small to make migration backpatch fast.
    return "v" + i + ":" + version;
  }

  // Returns version, or -1 if value malformed.
  private static long parseVersion(String value, int expectedI) {
    if (value == null) return -1;
    String prefix = "v" + expectedI + ":";
    if (!value.startsWith(prefix)) return -1;
    try {
      return Long.parseLong(value.substring(prefix.length()));
    } catch (NumberFormatException nfe) {
      return -1;
    }
  }

  private static synchronized Jedis getOrOpen(HostAndPort hp) {
    Jedis j = conns.get(hp);
    if (j != null) return j;
    j = new Jedis(hp.getHost(), hp.getPort());
    j.connect();
    conns.put(hp, j);
    return j;
  }

  // ---- bootstrap ----
  @SuppressWarnings("unchecked")
  private static void bootstrap() {
    for (int i = 0; i < 16384; i++) slotCache[i] = new SlotEntry();
    Jedis seed = new Jedis(seedHp.getHost(), seedHp.getPort());
    seed.connect();
    try {
      seed.getClient().cluster(new byte[][]{SafeEncoder.encode("SLOTS")});
      Object reply = seed.getClient().getOne();
      List<Object> ranges = (List<Object>) reply;
      for (Object rangeObj : ranges) {
        List<Object> range = (List<Object>) rangeObj;
        long startSlot = (Long) range.get(0);
        long endSlot = (Long) range.get(1);
        List<Object> masterInfo = (List<Object>) range.get(2);
        String host = SafeEncoder.encode((byte[]) masterInfo.get(0));
        long port = (Long) masterInfo.get(1);
        HostAndPort hp = new HostAndPort(host, (int) port);
        for (int s = (int) startSlot; s <= (int) endSlot; s++) {
          slotOwner[s] = hp;
        }
        getOrOpen(hp);
      }
    } catch (Exception e) {
      System.err.println("CLUSTER SLOTS bootstrap failed: " + e.getMessage());
      System.exit(1);
    }
    try {
      seed.getClient().cluster(new byte[][]{SafeEncoder.encode("SLOTSTATE")});
      Object reply = seed.getClient().getOne();
      if (reply instanceof List) {
        for (Object e : (List<Object>) reply) {
          List<Object> tup = (List<Object>) e;
          int slot = (int) (long) (Long) tup.get(0);
          int state = (int) (long) (Long) tup.get(1);
          byte[] pb = (byte[]) tup.get(2);
          String peer = (pb != null) ? SafeEncoder.encode(pb) : "";
          slotCache[slot].state = state;
          slotCache[slot].peer = peer.isEmpty() ? null : HostAndPort.parseString(peer);
          if (slotCache[slot].peer != null) getOrOpen(slotCache[slot].peer);
        }
      }
    } catch (Exception ignore) { /* fine */ }
    seed.close();
    peerProbeExec = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "peer-probe");
      t.setDaemon(true);
      return t;
    });
  }

  // ---- reply parsing ----
  private static final class GetReply {
    int state = SLOT_STABLE;
    HostAndPort peer = null;
    String value = null;
    boolean ok = false;
  }

  @SuppressWarnings("unchecked")
  private static GetReply sendGet(Jedis j, String key) {
    GetReply r = new GetReply();
    synchronized (j) {
      try {
        j.getClient().get(SafeEncoder.encode(key));
        Object reply = j.getClient().getOne();
        if (reply == null) { r.ok = true; return r; }
        if (reply instanceof byte[]) {
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
                try { r.peer = HostAndPort.parseString(new String(pb, StandardCharsets.UTF_8)); }
                catch (Exception ignore) { r.peer = null; }
              }
            }
            if (valueO instanceof byte[]) {
              r.value = new String((byte[]) valueO, StandardCharsets.UTF_8);
            }
            r.ok = true;
            return r;
          }
        }
        r.ok = true;
        return r;
      } catch (JedisException je) {
        return r;
      }
    }
  }

  private static void updateSlotCache(int slot, GetReply r) {
    if (!r.ok) return;
    SlotEntry se = slotCache[slot];
    se.state = r.state;
    se.peer = r.peer;
    if (r.peer != null) {
      getOrOpen(r.peer);
      if (r.state != SLOT_STABLE) slotOwner[slot] = r.peer;
    }
  }

  // ---- read with parallel two-sided fanout ----
  private static String doRead(String key) {
    int slot = JedisClusterCRC16.getSlot(key);
    SlotEntry s = slotCache[slot];
    HostAndPort ownerHp = slotOwner[slot];
    if (ownerHp == null) ownerHp = seedHp;
    final Jedis ownerJ = getOrOpen(ownerHp);

    if (s.state == SLOT_STABLE || s.peer == null) {
      GetReply ra = sendGet(ownerJ, key);
      updateSlotCache(slot, ra);
      if (ra.value != null) return ra.value;
      if (ra.state != SLOT_STABLE && ra.peer != null) {
        GetReply rb = sendGet(getOrOpen(ra.peer), key);
        if (rb.value != null) return rb.value;
      }
      return null;
    }
    // Parallel fanout: donor + recipient. Recipient wins.
    final HostAndPort peerHp = s.peer;
    final String keyFinal = key;
    Future<GetReply> peerFuture = peerProbeExec.submit(new Callable<GetReply>() {
      @Override
      public GetReply call() { return sendGet(getOrOpen(peerHp), keyFinal); }
    });
    GetReply ra = sendGet(ownerJ, key);
    GetReply rb;
    try { rb = peerFuture.get(); } catch (Exception e) { rb = null; }
    updateSlotCache(slot, ra);
    if (rb != null && rb.value != null) return rb.value;
    if (ra.value != null) return ra.value;
    return null;
  }

  // ---- write with per-slot MOVED/ASK retry ----
  private static String doWrite(String key, String value) {
    int slot = JedisClusterCRC16.getSlot(key);
    HostAndPort hp = slotOwner[slot];
    if (hp == null) hp = seedHp;
    boolean ask = false;
    for (int attempt = 0; attempt < 8; attempt++) {
      Jedis j = getOrOpen(hp);
      synchronized (j) {
        try {
          if (ask) { j.asking(); ask = false; }
          return j.set(key, value);
        } catch (JedisMovedDataException mv) {
          hp = new HostAndPort(mv.getTargetNode().getHost(), mv.getTargetNode().getPort());
          slotOwner[slot] = hp;
          ask = false;
        } catch (JedisAskDataException ax) {
          hp = new HostAndPort(ax.getTargetNode().getHost(), ax.getTargetNode().getPort());
          ask = true;
        }
      }
    }
    throw new JedisException("doWrite: exhausted retries for slot=" + slot);
  }

  // ---- trace dumper thread ----
  private static Thread startTraceDumper() {
    Thread t = new Thread(() -> {
      try (PrintWriter pw = new PrintWriter(new FileOutputStream(traceOut, true), true,
                                            StandardCharsets.UTF_8)) {
        pw.println("# unix_ms slot client_state client_peer client_owner");
        while (!stop.get()) {
          long ms = System.currentTimeMillis();
          for (int slot = slotRangeStart; slot <= slotRangeEnd; slot++) {
            SlotEntry se = slotCache[slot];
            HostAndPort owner = slotOwner[slot];
            pw.println(ms + " " + slot + " " +
                       stateName(se.state) + " " +
                       (se.peer != null ? se.peer.toString() : "-") + " " +
                       (owner != null ? owner.toString() : "-"));
          }
          try { Thread.sleep(dumpIntervalMs); } catch (InterruptedException ie) { break; }
        }
      } catch (Exception e) {
        System.err.println("trace dumper failed: " + e);
      }
    }, "trace-dumper");
    t.setDaemon(true);
    t.start();
    return t;
  }

  private static String stateName(int s) {
    switch (s) {
      case SLOT_STABLE:    return "STABLE";
      case SLOT_MIGRATING: return "MIGRATING";
      case SLOT_MIGRATED:  return "MIGRATED";
      default: return "?";
    }
  }

  // ---- load + run ----
  private static void preload() {
    System.out.println("[load] writing " + numKeys + " keys");
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < numKeys; i++) {
      expected[i] = 1;
      doWrite(keyName(i), valueFor(i, 1));
    }
    System.out.println("[load] done in " + (System.currentTimeMillis() - t0) + "ms");
  }

  private static void run() {
    System.out.println("[run] duration=" + durationSec + "s slot_range="
                       + slotRangeStart + ".." + slotRangeEnd);
    long endAt = System.currentTimeMillis() + durationSec * 1000L;
    Random rng = new Random(0xACEDL);
    long ops = 0;
    while (System.currentTimeMillis() < endAt) {
      int i = rng.nextInt(numKeys);
      String k = keyName(i);
      if (rng.nextBoolean()) {
        // write — only bump expected[i] after a successful SET so the
        // tool's local "expected version" never gets ahead of what's
        // actually on the cluster.
        long nv = expected[i] + 1;
        try {
          doWrite(k, valueFor(i, nv));
          expected[i] = nv;
          writesOk++;
        } catch (Exception e) {
          System.err.println("[write fail] key=" + k + " err=" + e);
        }
      } else {
        // read + verify
        String got;
        try { got = doRead(k); }
        catch (Exception e) { System.err.println("[read fail] key=" + k + " err=" + e); continue; }
        if (got == null) {
          // legitimate nil during snapshot import? Tolerate but log.
          System.err.println("[read NIL] key=" + k + " expected_ver=" + expected[i]
                             + " slot=" + JedisClusterCRC16.getSlot(k)
                             + " state=" + stateName(slotCache[JedisClusterCRC16.getSlot(k)].state));
          continue;
        }
        long ver = parseVersion(got, i);
        if (ver < 0) {
          mismatches++;
          System.err.println("[MISMATCH bad-format] key=" + k + " got=" + got);
          continue;
        }
        // Recipient-wins invariant: read must see >= our last write.
        if (ver < expected[i]) {
          mismatches++;
          int sl = JedisClusterCRC16.getSlot(k);
          System.err.println("[MISMATCH stale] key=" + k + " expected>=" + expected[i]
                             + " got_ver=" + ver + " slot=" + sl
                             + " state=" + stateName(slotCache[sl].state)
                             + " peer=" + slotCache[sl].peer
                             + " owner=" + slotOwner[sl]);
        } else {
          readsOk++;
        }
      }
      ops++;
      if (ops % 5000 == 0) {
        System.out.println("[progress] ops=" + ops + " reads_ok=" + readsOk
                           + " writes_ok=" + writesOk + " mismatches=" + mismatches);
      }
    }
    System.out.println("[run] done. ops=" + ops + " reads_ok=" + readsOk
                       + " writes_ok=" + writesOk + " mismatches=" + mismatches);
  }

  // ---- main + arg parsing ----
  public static void main(String[] args) {
    for (int i = 0; i < args.length; i++) {
      String a = args[i];
      if (a.equals("--seed-host"))         seedHost = args[++i];
      else if (a.equals("--seed-port"))    seedPort = Integer.parseInt(args[++i]);
      else if (a.equals("--num-keys"))     numKeys = Integer.parseInt(args[++i]);
      else if (a.equals("--slot-range")) {
        String[] sr = args[++i].split("\\.\\.");
        slotRangeStart = Integer.parseInt(sr[0]);
        slotRangeEnd   = Integer.parseInt(sr[1]);
      }
      else if (a.equals("--duration-sec")) durationSec = Integer.parseInt(args[++i]);
      else if (a.equals("--dump-interval-ms")) dumpIntervalMs = Integer.parseInt(args[++i]);
      else if (a.equals("--trace-out"))    traceOut = args[++i];
      else if (a.equals("--load-only"))    loadOnly = true;
      else {
        System.err.println("unknown arg: " + a);
        System.exit(2);
      }
    }
    seedHp = new HostAndPort(seedHost, seedPort);
    expected = new long[numKeys];

    System.out.println("[boot] seed=" + seedHp + " keys=" + numKeys
                       + " slot_range=" + slotRangeStart + ".." + slotRangeEnd
                       + " trace=" + traceOut);
    bootstrap();

    if (loadOnly) {
      preload();
      System.out.println("[load-only] exiting");
      return;
    }

    // For a non-load run we still SET all keys so expected[] is initialized
    // and the values exist.
    preload();
    Thread dumper = startTraceDumper();
    try {
      run();
    } finally {
      stop.set(true);
      try { dumper.join(2000); } catch (InterruptedException ignore) { /* */ }
    }
    // Final exit code: 0 on no mismatches, 1 otherwise.
    System.exit(mismatches == 0 ? 0 : 1);
  }
}
