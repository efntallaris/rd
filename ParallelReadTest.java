import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.db.RedisClient;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.util.JedisClusterCRC16;

import java.io.*;
import java.lang.reflect.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Mock-server test for RedisClient.read() parallel two-sided lookup.
 *
 * Exercises four cases for a slot whose cached state is MIGRATING:
 *   1. both donor and recipient have the key -> recipient's value wins
 *   2. only donor has the key                -> donor's value returned
 *   3. only recipient has the key            -> recipient's value returned
 *   4. neither has the key                   -> Status.ERROR
 *
 * Uses ServerSocket-based RESP servers (no real Redis needed). Populates
 * RedisClient's private fields via reflection (skips full init() bootstrap).
 */
public final class ParallelReadTest {

    private ParallelReadTest() {}

    private static final String KEY = "testkey";

    /** Minimal RESP server. Accepts Jedis connections, parses GETs, replies
     *  with a configurable slot-meta-wrapped array [state, peer, value]. */
    static final class MockRedis implements Runnable, Closeable {
        final ServerSocket server;
        final int state;
        final String peer;
        final String value; // null -> nil reply
        volatile boolean stop = false;
        Thread thread;

        MockRedis(int state, String peer, String value) throws IOException {
            this.server = new ServerSocket(0);
            this.state = state;
            this.peer = peer != null ? peer : "";
            this.value = value;
        }

        int port() { return server.getLocalPort(); }

        void start() {
            thread = new Thread(this, "mock-redis-" + port());
            thread.setDaemon(true);
            thread.start();
        }

        public void run() {
            try {
                while (!stop) {
                    Socket s = server.accept();
                    Thread t = new Thread(() -> serve(s), "mock-redis-conn-" + port());
                    t.setDaemon(true);
                    t.start();
                }
            } catch (IOException ignore) { /* server closed */ }
        }

        private void serve(Socket s) {
            try {
                InputStream in = s.getInputStream();
                OutputStream out = s.getOutputStream();
                while (true) {
                    String[] argv = readRespArray(in);
                    if (argv == null) return;
                    if (argv.length >= 1 && argv[0].equalsIgnoreCase("GET")) {
                        writeMetaReply(out);
                    } else {
                        out.write("+OK\r\n".getBytes());
                        out.flush();
                    }
                }
            } catch (IOException ignore) {
                /* connection closed */
            } finally {
                try { s.close(); } catch (IOException ignore) { /* drain */ }
            }
        }

        private void writeMetaReply(OutputStream out) throws IOException {
            StringBuilder sb = new StringBuilder();
            sb.append("*3\r\n");
            sb.append(":").append(state).append("\r\n");
            sb.append("$").append(peer.length()).append("\r\n").append(peer).append("\r\n");
            if (value == null) {
                sb.append("$-1\r\n");
            } else {
                sb.append("$").append(value.length()).append("\r\n").append(value).append("\r\n");
            }
            out.write(sb.toString().getBytes());
            out.flush();
        }

        private static String[] readRespArray(InputStream in) throws IOException {
            int c = in.read();
            if (c == -1) return null;
            if (c != '*') throw new IOException("expected '*' got " + (char) c);
            int n = readInt(in);
            String[] out = new String[n];
            for (int i = 0; i < n; i++) {
                int b = in.read();
                if (b != '$') throw new IOException("expected '$' got " + (char) b);
                int len = readInt(in);
                byte[] buf = new byte[len];
                int got = 0;
                while (got < len) {
                    int r = in.read(buf, got, len - got);
                    if (r < 0) throw new EOFException();
                    got += r;
                }
                in.read(); in.read(); // \r\n
                out[i] = new String(buf);
            }
            return out;
        }

        private static int readInt(InputStream in) throws IOException {
            StringBuilder sb = new StringBuilder();
            int c;
            while ((c = in.read()) != -1) {
                if (c == '\r') { in.read(); return Integer.parseInt(sb.toString()); }
                sb.append((char) c);
            }
            throw new EOFException();
        }

        public void close() throws IOException {
            stop = true;
            server.close();
        }
    }

    static RedisClient makeClient(HostAndPort donor, HostAndPort recipient,
                                  int slot, int state) throws Exception {
        RedisClient c = new RedisClient();
        Field fOwner = RedisClient.class.getDeclaredField("slotOwner");
        Field fCache = RedisClient.class.getDeclaredField("slotCache");
        Field fConns = RedisClient.class.getDeclaredField("conns");
        Field fExec  = RedisClient.class.getDeclaredField("peerProbeExec");
        fOwner.setAccessible(true);
        fCache.setAccessible(true);
        fConns.setAccessible(true);
        fExec.setAccessible(true);

        HostAndPort[] owners = new HostAndPort[16384];
        for (int i = 0; i < 16384; i++) owners[i] = donor;
        fOwner.set(c, owners);

        Class<?> seClass = Class.forName("site.ycsb.db.RedisClient$SlotEntry");
        Constructor<?> seCtor = seClass.getDeclaredConstructor();
        seCtor.setAccessible(true);
        Object cacheArr = Array.newInstance(seClass, 16384);
        for (int i = 0; i < 16384; i++) Array.set(cacheArr, i, seCtor.newInstance());
        Field fState = seClass.getDeclaredField("state");
        Field fPeer  = seClass.getDeclaredField("peer");
        fState.setAccessible(true);
        fPeer.setAccessible(true);
        Object slotEntry = Array.get(cacheArr, slot);
        fState.set(slotEntry, state);
        fPeer.set(slotEntry, recipient);
        fCache.set(c, cacheArr);

        Map<HostAndPort, Jedis> conns = new ConcurrentHashMap<>();
        Jedis donorJ = new Jedis(donor.getHost(), donor.getPort());
        Jedis recipJ = new Jedis(recipient.getHost(), recipient.getPort());
        donorJ.connect();
        recipJ.connect();
        conns.put(donor, donorJ);
        conns.put(recipient, recipJ);
        fConns.set(c, conns);

        ExecutorService exec = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "redis-peer-probe-test");
            t.setDaemon(true);
            return t;
        });
        fExec.set(c, exec);

        return c;
    }

    static String runRead(RedisClient c) throws Exception {
        HashMap<String, ByteIterator> result = new HashMap<>();
        Status st = c.read("t", KEY, null, result);
        if (st != Status.OK) return null;
        return result.get("value").toString();
    }

    static void assertEq(String label, String expected, String actual) {
        if (!Objects.equals(expected, actual)) {
            System.out.println("FAIL: " + label + " expected=" + expected + " actual=" + actual);
            System.exit(1);
        }
        System.out.println("PASS: " + label);
    }

    public static void main(String[] args) throws Exception {
        final int SLOT = JedisClusterCRC16.getSlot(KEY);
        final int MIGRATING = 1;

        // Case 1: both have key -> recipient wins
        {
            MockRedis donor = new MockRedis(MIGRATING, "", "DONOR_OLD");
            MockRedis recip = new MockRedis(MIGRATING, "", "RECIPIENT_NEW");
            donor.start(); recip.start();
            HostAndPort dHp = new HostAndPort("127.0.0.1", donor.port());
            HostAndPort rHp = new HostAndPort("127.0.0.1", recip.port());
            RedisClient c = makeClient(dHp, rHp, SLOT, MIGRATING);
            assertEq("recipient wins when both have the key",
                     "RECIPIENT_NEW", runRead(c));
            donor.close(); recip.close();
        }

        // Case 2: only donor has key -> donor's value
        {
            MockRedis donor = new MockRedis(MIGRATING, "", "DONOR_ONLY");
            MockRedis recip = new MockRedis(MIGRATING, "", null);
            donor.start(); recip.start();
            HostAndPort dHp = new HostAndPort("127.0.0.1", donor.port());
            HostAndPort rHp = new HostAndPort("127.0.0.1", recip.port());
            RedisClient c = makeClient(dHp, rHp, SLOT, MIGRATING);
            assertEq("donor's value when recipient is nil",
                     "DONOR_ONLY", runRead(c));
            donor.close(); recip.close();
        }

        // Case 3: only recipient has key -> recipient's value
        {
            MockRedis donor = new MockRedis(MIGRATING, "", null);
            MockRedis recip = new MockRedis(MIGRATING, "", "RECIPIENT_ONLY");
            donor.start(); recip.start();
            HostAndPort dHp = new HostAndPort("127.0.0.1", donor.port());
            HostAndPort rHp = new HostAndPort("127.0.0.1", recip.port());
            RedisClient c = makeClient(dHp, rHp, SLOT, MIGRATING);
            assertEq("recipient's value when donor is nil",
                     "RECIPIENT_ONLY", runRead(c));
            donor.close(); recip.close();
        }

        // Case 4: neither has the key -> not found
        {
            MockRedis donor = new MockRedis(MIGRATING, "", null);
            MockRedis recip = new MockRedis(MIGRATING, "", null);
            donor.start(); recip.start();
            HostAndPort dHp = new HostAndPort("127.0.0.1", donor.port());
            HostAndPort rHp = new HostAndPort("127.0.0.1", recip.port());
            RedisClient c = makeClient(dHp, rHp, SLOT, MIGRATING);
            assertEq("not-found when neither has it",
                     null, runRead(c));
            donor.close(); recip.close();
        }

        System.out.println("ALL PASS");
    }
}
