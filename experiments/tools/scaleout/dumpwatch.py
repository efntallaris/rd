#!/usr/bin/env python3
"""dumpwatch.py <out_dir> <host> <port>: once host:port has answered for 60 s in a row and then
refuses (the crash injector's kill), ask the YCSB JVM on ycsb0 for thread dumps (SIGQUIT -> its
stdout) about 1.5, 2.5, 3.5 and 4.5 s after the kill."""
import socket, subprocess, sys, time
out, host, port = sys.argv[1], sys.argv[2], int(sys.argv[3])


def up():
    try:
        socket.create_connection((host, port), timeout=0.3).close()
        return True
    except OSError:
        return False


since = None
while True:
    if up():
        since = since or time.time()
    else:
        if since and time.time() - since >= 60:
            break
        since = None
    time.sleep(0.1)
tk = time.time()
with open(out + "/dumpwatch.txt", "a") as f:
    f.write("down detected %.3f\n" % tk)
    for at in (1.4, 2.4, 3.4, 4.4):
        time.sleep(max(0, tk + at - time.time()))
        r = subprocess.run(["sudo", "ssh", "ycsb0",
                            "for p in $(pgrep -x java); do grep -qa site.ycsb /proc/$p/cmdline && kill -QUIT $p; done; date -u +%T.%N"],
                           capture_output=True, text=True)
        f.write("dump at +%.2f: %s" % (time.time() - tk, r.stdout))
        f.flush()
