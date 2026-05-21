import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.db.RedisClient;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Monotonic-version correctness test for aqueduct migration.
 *
 * For each of N keys: writes "v0", "v1", "v2", ... in increasing order.
 * Tracks the most recently *successfully-acked* version per key locally.
 * After the run, reads every key and verifies the read value matches the
 * tracked latest version. A mismatch means a stale read — either the donor
 * served an old snapshot value after the recipient already accepted a newer
 * write, or a backpatched snapshot clobbered a post-FLIP write.
 *
 * Usage:
 *   java VersionedWriteTest <seed-host> <seed-port> <num-keys> <write-duration-sec>
 *
 * Migration is triggered externally (e.g. via ansible) while the writer is
 * running.
 */
public final class VersionedWriteTest {

    private VersionedWriteTest() {}

    static volatile long[] latestVersion; // index -> last successfully written version (long)
    static String[] keys;
    static final AtomicBoolean stop = new AtomicBoolean(false);
    static final AtomicLong writesAcked = new AtomicLong(0);
    static final AtomicLong writesFailed = new AtomicLong(0);

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("usage: VersionedWriteTest <host> <port> <num-keys> <write-duration-sec>");
            System.exit(2);
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        int N = Integer.parseInt(args[2]);
        int durationSec = Integer.parseInt(args[3]);

        keys = new String[N];
        for (int i = 0; i < N; i++) keys[i] = "vkey_" + i;
        latestVersion = new long[N];

        // Two clients: one as writer (uses JedisCluster underneath; handles MOVED),
        // one as reader (uses the parallel two-sided lookup). They are independent
        // RedisClient instances so they have independent slot caches / executors.
        RedisClient writer = buildClient(host, port);
        RedisClient reader = buildClient(host, port);

        System.out.println("[load] writing initial v0 for " + N + " keys");
        for (int i = 0; i < N; i++) {
            writeKey(writer, i, 0);
            if (i % 10000 == 0) System.out.println("[load]   " + i);
        }
        System.out.println("[load] done. writesAcked=" + writesAcked.get());

        // Drop a marker so the orchestrating playbook can trigger the migration
        // only AFTER the load phase has completed (avoids redirect storm during
        // load). Writes resume below.
        try {
            java.nio.file.Files.write(java.nio.file.Paths.get("/tmp/vwt_load_done"),
                "ok\n".getBytes());
        } catch (Exception ignore) {
            /* drain */
        }

        // Reset acked counter for the run phase.
        writesAcked.set(0);

        System.out.println("[run] starting writer for " + durationSec + "s — trigger the migration now");
        long t0 = System.nanoTime();
        long deadlineNs = t0 + (long) durationSec * 1_000_000_000L;

        // Multi-threaded writer to drive enough load that the migration window has
        // many writes overlapping it.
        int nThreads = 16;
        Thread[] workers = new Thread[nThreads];
        for (int t = 0; t < nThreads; t++) {
            final int tid = t;
            workers[t] = new Thread(() -> {
                long iter = 0;
                while (!stop.get() && System.nanoTime() < deadlineNs) {
                    int i = (int) ((tid + iter * nThreads) % N);
                    iter++;
                    long curr = latestVersion[i];
                    long next = curr + 1;
                    if (writeKey(writer, i, next)) {
                        // Atomic update: only advance if our write was the latest.
                        // (Multi-threaded race on same key: the higher version wins.)
                        synchronized (latestVersion) {
                            if (latestVersion[i] < next) latestVersion[i] = next;
                        }
                    }
                }
            }, "writer-" + tid);
            workers[t].setDaemon(true);
            workers[t].start();
        }

        for (Thread w : workers) w.join();
        long t1 = System.nanoTime();
        double seconds = (t1 - t0) / 1e9;
        System.out.printf("[run] done. duration=%.1fs writesAcked=%d writesFailed=%d ops/sec=%.0f%n",
            seconds, writesAcked.get(), writesFailed.get(), writesAcked.get() / seconds);

        // Verification — read every key and compare to tracked latest.
        System.out.println("[verify] reading back " + N + " keys");
        long mismatches = 0;
        long notFound = 0;
        long readErr = 0;
        StringBuilder mismatchSample = new StringBuilder();
        for (int i = 0; i < N; i++) {
            HashMap<String, ByteIterator> r = new HashMap<>();
            Status st = reader.read("t", keys[i], null, r);
            if (st != Status.OK) {
                readErr++;
                continue;
            }
            ByteIterator v = r.get("value");
            if (v == null) v = r.values().iterator().next();
            if (v == null) {
                notFound++;
                continue;
            }
            String val = v.toString();
            String expected = "v" + latestVersion[i];
            if (!val.equals(expected)) {
                mismatches++;
                if (mismatchSample.length() < 2000) {
                    mismatchSample.append("  key=").append(keys[i])
                                  .append(" expected=").append(expected)
                                  .append(" got=").append(val).append("\n");
                }
            }
        }

        System.out.println("[verify] N=" + N
            + " mismatches=" + mismatches
            + " not-found=" + notFound
            + " read-error=" + readErr);
        if (mismatches > 0) {
            System.out.println("[verify] sample mismatches:");
            System.out.println(mismatchSample.toString());
        }

        if (mismatches == 0 && notFound == 0 && readErr == 0) {
            System.out.println("RESULT: PASS — every read matched the locally-tracked latest write");
            System.exit(0);
        } else {
            System.out.println("RESULT: FAIL");
            System.exit(1);
        }
    }

    static boolean writeKey(RedisClient db, int idx, long version) {
        HashMap<String, ByteIterator> values = new HashMap<>();
        values.put("value", new StringByteIterator("v" + version));
        Status st = db.update("t", keys[idx], values);
        if (st == Status.OK) {
            writesAcked.incrementAndGet();
            return true;
        }
        // First write (initial load) uses insert path because key doesn't exist.
        st = db.insert("t", keys[idx], values);
        if (st == Status.OK) {
            writesAcked.incrementAndGet();
            return true;
        }
        writesFailed.incrementAndGet();
        return false;
    }

    static RedisClient buildClient(String host, int port) throws Exception {
        RedisClient c = new RedisClient();
        Properties p = new Properties();
        p.setProperty("redis.host", host);
        p.setProperty("redis.port", Integer.toString(port));
        p.setProperty("redis.cluster", "true");
        c.setProperties(p);
        c.init();
        return c;
    }
}
