#!/usr/bin/env python3
"""opsamp.py <out file> <host:port> ...: every 0.2 s, total_commands_processed of each endpoint
(one line: epoch then one counter per endpoint, -1 when unreachable). Stops when <out file>.stop exists."""
import socket, sys, time, os
out, eps = sys.argv[1], [e.split(":") for e in sys.argv[2:]]
def q(h, p):
    try:
        s = socket.create_connection((h, int(p)), timeout=0.15); s.settimeout(0.15)
        s.sendall(b"*2\r\n$4\r\nINFO\r\n$5\r\nstats\r\n"); buf = b""
        while b"total_commands_processed:" not in buf or b"\r\n" not in buf.split(b"total_commands_processed:")[1]:
            c = s.recv(8192)
            if not c: break
            buf += c
        s.close()
        return int(buf.split(b"total_commands_processed:")[1].split(b"\r\n")[0])
    except Exception:
        return -1
with open(out, "w") as f:
    while not os.path.exists(out + ".stop"):
        t = time.time()
        f.write("%.3f %s\n" % (t, " ".join(str(q(h, p)) for h, p in eps))); f.flush()
        time.sleep(max(0, 0.2 - (time.time() - t)))
