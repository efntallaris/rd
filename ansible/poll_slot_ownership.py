#!/usr/bin/env python3
"""
Sample CLUSTER SLOTSTATE from each redis node every N ms, using persistent
TCP connections (so we can actually catch a sub-second migration window).

Output format (one line per non-STABLE slot per sample):
    <unix_ms> <host> <slot> <state_name> <peer>

Slots not present in a sample = STABLE; not logged.
Exit on SIGTERM.

usage:
    poll_slot_ownership.py --out FILE [--hosts h1 h2 ...] [--interval-ms N]
                            [--slot-lo N] [--slot-hi N]
"""
import argparse
import os
import signal
import socket
import sys
import time
from pathlib import Path

STATE_NAME = {0: 'STABLE', 1: 'MIGRATING', 2: 'MIGRATED'}


class RespError(Exception):
    pass


def resp_encode_cmd(*parts):
    """Encode a RESP command. Parts may be str or bytes."""
    out = [f"*{len(parts)}\r\n".encode()]
    for p in parts:
        if isinstance(p, str):
            p = p.encode()
        out.append(f"${len(p)}\r\n".encode())
        out.append(p)
        out.append(b"\r\n")
    return b"".join(out)


def _readline(buf, sock):
    """Read until \\r\\n. Returns bytes (without trailing crlf)."""
    while True:
        idx = buf['data'].find(b"\r\n")
        if idx >= 0:
            line = buf['data'][:idx]
            buf['data'] = buf['data'][idx + 2:]
            return line
        chunk = sock.recv(4096)
        if not chunk:
            raise RespError("connection closed")
        buf['data'] += chunk


def _readn(buf, sock, n):
    while len(buf['data']) < n + 2:
        chunk = sock.recv(8192)
        if not chunk:
            raise RespError("connection closed")
        buf['data'] += chunk
    out = buf['data'][:n]
    buf['data'] = buf['data'][n + 2:]
    return out


def resp_decode(buf, sock):
    line = _readline(buf, sock)
    if not line:
        raise RespError("empty line")
    t = chr(line[0])
    rest = line[1:]
    if t == '+':
        return rest.decode()
    if t == '-':
        raise RespError(rest.decode())
    if t == ':':
        return int(rest)
    if t == '$':
        n = int(rest)
        if n == -1:
            return None
        return _readn(buf, sock, n)
    if t == '*':
        n = int(rest)
        if n == -1:
            return None
        return [resp_decode(buf, sock) for _ in range(n)]
    raise RespError(f"unknown prefix {t}")


class RedisConn:
    def __init__(self, host, port=8000, timeout=2.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock = None
        self.buf = {'data': b''}

    def connect(self):
        self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self.sock.settimeout(self.timeout)
        # TCP_NODELAY for low-latency polling
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = {'data': b''}

    def reconnect(self):
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
        self.connect()

    def cmd(self, *parts):
        if self.sock is None:
            self.connect()
        self.sock.sendall(resp_encode_cmd(*parts))
        return resp_decode(self.buf, self.sock)


def sample(conn, host, lo, hi, now_ms, out):
    try:
        reply = conn.cmd('CLUSTER', 'SLOTSTATE')
    except (RespError, socket.error, socket.timeout):
        try:
            conn.reconnect()
        except Exception:
            pass
        return
    if not isinstance(reply, list):
        return
    for entry in reply:
        if not isinstance(entry, list) or len(entry) < 3:
            continue
        try:
            slot = int(entry[0])
            state = int(entry[1])
            peer_b = entry[2]
        except Exception:
            continue
        if slot < lo or slot > hi:
            continue
        peer = peer_b.decode() if isinstance(peer_b, (bytes, bytearray)) else (peer_b or '')
        if not peer:
            peer = '-'
        out.write(f"{now_ms} {host} {slot} {STATE_NAME.get(state,'?')} {peer}\n")
    out.flush()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', required=True)
    ap.add_argument('--hosts', nargs='+', default=['redis0', 'redis1', 'redis2', 'redis3'])
    ap.add_argument('--interval-ms', type=int, default=50)
    ap.add_argument('--slot-lo', type=int, default=0)
    ap.add_argument('--slot-hi', type=int, default=16383)
    ap.add_argument('--port', type=int, default=8000)
    ap.add_argument('--pid-file', default='/tmp/slot_poller.pid')
    args = ap.parse_args()

    with open(args.pid_file, 'w') as f:
        f.write(str(os.getpid()))

    stop = {'flag': False}
    def _stop(*_):
        stop['flag'] = True
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    conns = {}
    for h in args.hosts:
        c = RedisConn(h, args.port)
        try:
            c.connect()
            conns[h] = c
            print(f"connected: {h}:{args.port}", file=sys.stderr)
        except Exception as e:
            print(f"WARN: cannot connect to {h}:{args.port}: {e}", file=sys.stderr)

    interval = args.interval_ms / 1000.0
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'a') as out:
        while not stop['flag']:
            t0 = time.time()
            now_ms = int(t0 * 1000)
            for h, c in conns.items():
                sample(c, h, args.slot_lo, args.slot_hi, now_ms, out)
            elapsed = time.time() - t0
            sleep_for = max(0, interval - elapsed)
            if sleep_for > 0:
                time.sleep(sleep_for)


if __name__ == '__main__':
    main()
