// Linearizability history recorder for Aqueduct slot migration.
//
// Runs T concurrent client threads doing GET / SET / DEL (and optionally INCR
// on a separate counter namespace) against K keys that hash into the slots
// being migrated, and writes one JSON line per operation:
//
//   {"proc":P,"key":K,"op":"set|get|del|incr","arg":A,"out":O,
//    "status":"ok|fail|unknown","inv":ns,"ret":ns,"phase":"run|final"}
//
// inv/ret are System.nanoTime() offsets from start, all taken in this one JVM,
// so real-time order between operations is exact. Every SET writes a unique
// value (c<thread>-<counter>) so each read maps to exactly one write.
//
// status: ok      = client got a definite reply
//         fail    = server definitely did not execute the op (drop it)
//         unknown = may or may not have taken effect (Jepsen :info); the
//                   thread then continues under a fresh proc id, as a crashed
//                   Jepsen process would
//
// Routing, redirects and the slot-meta double read are RedisClient's, with
// redis.retry.ambiguous=false so an ambiguous write is never re-sent.
//
// The workload runs until --stop-file exists (or --max-duration-sec), then a
// final pass reads every key once (phase "final"), retrying reads that come
// back unknown. The checker (lincheck/) consumes the output.

package site.ycsb.db;

import redis.clients.util.JedisClusterCRC16;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class LinHistoryClient {

  private LinHistoryClient() {
  }

  // ---- args ----
  private static String seedHost = "redis0";
  private static int seedPort = 8000;
  private static int numKeys = 200;
  private static int numCtrKeys = 0;
  private static int threads = 32;
  private static int ratePerThread = 50;      // ops/s per thread
  private static int readPct = 45;
  private static int delPct = 10;             // rest are SETs
  private static int incrPct = 0;             // share of ops on ctr keys
  private static String slotRanges = "0-1364,5461-6825,10922-12286";
  private static String out = "/tmp/lin_history.jsonl";
  private static String stopFile = "/tmp/lin_stop";
  private static int maxDurationSec = 3600;
  private static int timeoutMs = 2000;
  private static int slotPollMs = 100;
  private static long rngSeed = 42;

  private static final AtomicBoolean STOP = new AtomicBoolean(false);
  private static final AtomicInteger NEXT_PROC = new AtomicInteger(0);
  private static final AtomicLong N_OK = new AtomicLong();
  private static final AtomicLong N_FAIL = new AtomicLong();
  private static final AtomicLong N_UNKNOWN = new AtomicLong();
  private static long t0;
  private static BufferedWriter w;

  public static void main(String[] args) throws Exception {
    parseArgs(args);
    List<int[]> ranges = parseRanges(slotRanges);
    List<String> keys = pickKeys("lin", numKeys, ranges);
    List<String> ctrKeys = pickKeys("ctr", numCtrKeys, ranges);

    new File(stopFile).delete();
    w = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out), StandardCharsets.UTF_8));
    t0 = System.nanoTime();

    System.out.println("[lin] keys=" + keys.size() + " ctrKeys=" + ctrKeys.size()
        + " threads=" + threads + " rate/thread=" + ratePerThread + " ranges=" + slotRanges
        + " out=" + out + " stopFile=" + stopFile);

    List<Thread> workers = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      final int tid = t;
      Thread th = new Thread(() -> runWorker(tid, keys, ctrKeys), "lin-worker-" + t);
      th.start();
      workers.add(th);
    }

    long deadline = System.currentTimeMillis() + maxDurationSec * 1000L;
    long lastReport = 0;
    while (!new File(stopFile).exists() && System.currentTimeMillis() < deadline) {
      Thread.sleep(200);
      long now = System.currentTimeMillis();
      if (now - lastReport >= 5000) {
        lastReport = now;
        System.out.println("[lin] t=" + (System.nanoTime() - t0) / 1_000_000_000L + "s ok=" + N_OK.get()
            + " fail=" + N_FAIL.get() + " unknown=" + N_UNKNOWN.get());
      }
    }
    STOP.set(true);
    for (Thread th : workers) {
      th.join();
    }
    System.out.println("[lin] workload stopped; final read pass");

    finalPass(keys, ctrKeys);
    synchronized (w) {
      w.close();
    }
    System.out.println("[lin] done ok=" + N_OK.get() + " fail=" + N_FAIL.get()
        + " unknown=" + N_UNKNOWN.get());
    System.exit(0);
  }

  private static RedisClient newClient() throws Exception {
    Properties p = new Properties();
    p.setProperty(RedisClient.HOST_PROPERTY, seedHost);
    p.setProperty(RedisClient.PORT_PROPERTY, String.valueOf(seedPort));
    p.setProperty(RedisClient.CLUSTER_PROPERTY, "true");
    p.setProperty(RedisClient.TIMEOUT_PROPERTY, String.valueOf(timeoutMs));
    p.setProperty("redis.slotpoll.ms", String.valueOf(slotPollMs));
    p.setProperty("redis.retry.ambiguous", "false");
    RedisClient c = new RedisClient();
    c.setProperties(p);
    c.init();
    return c;
  }

  private static void runWorker(int tid, List<String> keys, List<String> ctrKeys) {
    RedisClient c;
    try {
      c = newClient();
    } catch (Exception e) {
      System.err.println("[lin] worker " + tid + " init failed: " + e);
      return;
    }
    Random rnd = new Random(rngSeed * 1_000_003L + tid);
    int proc = NEXT_PROC.getAndIncrement();
    long counter = 0;
    long intervalNs = ratePerThread > 0 ? 1_000_000_000L / ratePerThread : 0;
    long next = System.nanoTime();
    while (!STOP.get()) {
      if (intervalNs > 0) {
        long now = System.nanoTime();
        if (now < next) {
          sleepNs(next - now);
        }
        next += intervalNs;
        if (next < System.nanoTime() - 10 * intervalNs) {
          next = System.nanoTime();   // don't burst after a stall
        }
      }
      String op;
      String key;
      String arg = null;
      boolean onCtr = !ctrKeys.isEmpty() && rnd.nextInt(100) < incrPct;
      if (onCtr) {
        key = ctrKeys.get(rnd.nextInt(ctrKeys.size()));
        op = rnd.nextBoolean() ? "incr" : "get";
      } else {
        key = keys.get(rnd.nextInt(keys.size()));
        int r = rnd.nextInt(100);
        if (r < readPct) {
          op = "get";
        } else if (r < readPct + delPct) {
          op = "del";
        } else {
          op = "set";
          arg = "c" + tid + "-" + (counter++);
        }
      }
      long inv = System.nanoTime() - t0;
      RedisClient.OpResult res;
      try {
        switch (op) {
        case "get":
          res = c.getValue(key);
          break;
        case "set":
          res = c.setValue(key, arg);
          break;
        case "del":
          res = c.delValue(key);
          break;
        default:
          res = c.incrValue(key);
          break;
        }
      } catch (RuntimeException e) {
        res = null;
      }
      long ret = System.nanoTime() - t0;
      RedisClient.Outcome oc = (res == null) ? RedisClient.Outcome.UNKNOWN : res.outcome;
      record(proc, key, op, arg, res == null ? null : res.value, oc, inv, ret, "run");
      if (oc == RedisClient.Outcome.UNKNOWN && !"get".equals(op)) {
        proc = NEXT_PROC.getAndIncrement();   // the old proc may still be "in flight"
      }
    }
    try {
      c.cleanup();
    } catch (Exception ignore) {
      // exiting
    }
  }

  private static void finalPass(List<String> keys, List<String> ctrKeys) throws Exception {
    RedisClient c = newClient();
    int proc = NEXT_PROC.getAndIncrement();
    List<String> all = new ArrayList<>(keys);
    all.addAll(ctrKeys);
    int unresolved = 0;
    for (String key : all) {
      boolean done = false;
      for (int attempt = 0; attempt < 20 && !done; attempt++) {
        long inv = System.nanoTime() - t0;
        RedisClient.OpResult res;
        try {
          res = c.getValue(key);
        } catch (RuntimeException e) {
          res = null;
        }
        long ret = System.nanoTime() - t0;
        if (res != null && res.outcome == RedisClient.Outcome.OK) {
          record(proc, key, "get", null, res.value, RedisClient.Outcome.OK, inv, ret, "final");
          done = true;
        } else {
          sleepNs(250_000_000L);
        }
      }
      if (!done) {
        unresolved++;
        System.err.println("[lin] final read of " + key + " never succeeded");
      }
    }
    System.out.println("[lin] final pass: " + all.size() + " keys, unresolved=" + unresolved);
    try {
      c.cleanup();
    } catch (Exception ignore) {
      // exiting
    }
  }

  private static void record(int proc, String key, String op, String arg, String outv,
                             RedisClient.Outcome oc, long inv, long ret, String phase) {
    String status;
    switch (oc) {
    case OK:
      status = "ok";
      N_OK.incrementAndGet();
      break;
    case FAIL:
      status = "fail";
      N_FAIL.incrementAndGet();
      break;
    default:
      status = "unknown";
      N_UNKNOWN.incrementAndGet();
      break;
    }
    StringBuilder b = new StringBuilder(160);
    b.append("{\"proc\":").append(proc)
        .append(",\"key\":").append(jstr(key))
        .append(",\"op\":\"").append(op).append('"')
        .append(",\"arg\":").append(jstr(arg))
        .append(",\"out\":").append(jstr(outv))
        .append(",\"status\":\"").append(status).append('"')
        .append(",\"inv\":").append(inv)
        .append(",\"ret\":").append(ret)
        .append(",\"phase\":\"").append(phase).append("\"}\n");
    synchronized (w) {
      try {
        w.write(b.toString());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Keys and values here are ASCII identifiers; escape defensively anyway. */
  private static String jstr(String s) {
    if (s == null) {
      return "null";
    }
    StringBuilder b = new StringBuilder(s.length() + 2).append('"');
    for (char ch : s.toCharArray()) {
      if (ch == '"' || ch == '\\') {
        b.append('\\').append(ch);
      } else if (ch < 0x20) {
        b.append(String.format("\\u%04x", (int) ch));
      } else {
        b.append(ch);
      }
    }
    return b.append('"').toString();
  }

  /** n keys spread round-robin over the ranges; each hashes into its range. */
  private static List<String> pickKeys(String prefix, int n, List<int[]> ranges) {
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      int[] r = ranges.get(i % ranges.size());
      for (int salt = 0;; salt++) {
        String k = prefix + ":" + i + ":" + salt;
        int slot = JedisClusterCRC16.getSlot(k);
        if (slot >= r[0] && slot <= r[1]) {
          keys.add(k);
          break;
        }
      }
    }
    return keys;
  }

  private static List<int[]> parseRanges(String spec) {
    List<int[]> res = new ArrayList<>();
    for (String part : spec.split(",")) {
      String[] lohi = part.trim().split("-");
      int lo = Integer.parseInt(lohi[0]);
      int hi = lohi.length > 1 ? Integer.parseInt(lohi[1]) : lo;
      res.add(new int[]{lo, hi});
    }
    return res;
  }

  private static void sleepNs(long ns) {
    try {
      Thread.sleep(ns / 1_000_000L, (int) (ns % 1_000_000L));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void parseArgs(String[] a) {
    for (int i = 0; i < a.length; i++) {
      String v = (i + 1 < a.length) ? a[i + 1] : null;
      switch (a[i]) {
      case "--seed-host": seedHost = v; i++; break;
      case "--seed-port": seedPort = Integer.parseInt(v); i++; break;
      case "--keys": numKeys = Integer.parseInt(v); i++; break;
      case "--ctr-keys": numCtrKeys = Integer.parseInt(v); i++; break;
      case "--threads": threads = Integer.parseInt(v); i++; break;
      case "--rate": ratePerThread = Integer.parseInt(v); i++; break;
      case "--read-pct": readPct = Integer.parseInt(v); i++; break;
      case "--del-pct": delPct = Integer.parseInt(v); i++; break;
      case "--incr-pct": incrPct = Integer.parseInt(v); i++; break;
      case "--slot-ranges": slotRanges = v; i++; break;
      case "--out": out = v; i++; break;
      case "--stop-file": stopFile = v; i++; break;
      case "--max-duration-sec": maxDurationSec = Integer.parseInt(v); i++; break;
      case "--timeout-ms": timeoutMs = Integer.parseInt(v); i++; break;
      case "--slotpoll-ms": slotPollMs = Integer.parseInt(v); i++; break;
      case "--seed": rngSeed = Long.parseLong(v); i++; break;
      default:
        System.err.println("unknown arg: " + a[i]);
        System.exit(2);
      }
    }
  }
}
