#!/usr/bin/env python3
"""AqRaft post-migration ownership reconcile (leader-aware).

After NARROW, each shardgroup knows only the handover it participated in and
sg4 still advertises its full configured range, so the cluster-wide slot map is
inconsistent per node -> clients under-route to the recipient -> no throughput
gain. This computes the authoritative final topology (each donor's OWN narrowed
range + sg4 = the complement) and pushes the complete world-view to EVERY
shardgroup leader via RAFT.SHARDGROUP REPLACE, so all nodes agree. Idempotent.

Leader-aware: discovers each shardgroup's CURRENT leader (survives crash/
promotion in the failure scenarios) instead of assuming the configured one.
Reaches nodes over the redis protocol (redis-cli -h). Optional argv: redis-cli.
"""
import subprocess, sys, time

CLI = sys.argv[1] if len(sys.argv) > 1 else "/users/entall/rd/redis/src/redis-cli"
MAXSLOT = 16383

# (id, port, [node_ip, ...])  -- membership matches group_vars all.yml; raft_id = index+1
SGS = [
    ("sg1", 8000, ["10.10.1.1", "10.10.1.2", "10.10.1.3"]),
    ("sg2", 8001, ["10.10.1.2", "10.10.1.3", "10.10.1.1"]),
    ("sg3", 8002, ["10.10.1.3", "10.10.1.1", "10.10.1.2"]),
    ("sg4", 8000, ["10.10.1.4", "10.10.1.5", "10.10.1.6"]),
]
RECIPIENT = "sg4"

def cli(ip, port, *args):
    return subprocess.run([CLI, "-h", ip, "-p", str(port)] + [str(a) for a in args],
                          capture_output=True, text=True, timeout=30).stdout

def info(ip, port):
    d = {}
    for l in cli(ip, port, "INFO", "raft").splitlines():
        if ":" in l:
            k, v = l.split(":", 1); d[k.strip()] = v.strip()
    return d

def find_leader(port, ips):
    """Return (leader_ip, dbid) for the shardgroup; retry to ride out an election."""
    for _ in range(60):
        for ip in ips:
            d = info(ip, port)
            if d.get("raft_role", "").lower() == "leader" and d.get("raft_dbid"):
                return ip, d["raft_dbid"]
        time.sleep(2)
    raise RuntimeError("no leader for port %d among %s" % (port, ips))

def is_alive(ip, port):
    d = info(ip, port)
    return bool(d.get("raft_role"))

def own_range(ip, port):
    out = cli(ip, port, "RAFT.SHARDGROUP", "GET").split()
    return int(out[1]), int(out[2])

def complement(ranges):
    covered = [False] * (MAXSLOT + 1)
    for lo, hi in ranges:
        for s in range(lo, hi + 1):
            covered[s] = True
    out, s = [], 0
    while s <= MAXSLOT:
        if not covered[s]:
            e = s
            while e + 1 <= MAXSLOT and not covered[e + 1]:
                e += 1
            out.append((s, e)); s = e + 1
        else:
            s += 1
    return out

def main():
    meta = {}
    for sid, port, ips in SGS:
        leader_ip, dbid = find_leader(port, ips)
        meta[sid] = {"dbid": dbid, "leader": leader_ip, "port": port, "ips": ips}
        print("%s leader=%s:%d dbid=%s" % (sid, leader_ip, port, dbid))
    donor_ranges = {sid: own_range(m["leader"], m["port"])
                    for sid, m in meta.items() if sid != RECIPIENT}
    ranges = {sid: [r] for sid, r in donor_ranges.items()}
    ranges[RECIPIENT] = complement(list(donor_ranges.values()))

    # Crash cleanup: a leader-crash leaves the DEAD node in the shardgroup's raft
    # membership (re-advertised via gossip) and in the donors' write-flip spec. The
    # external-shardgroup MOVED round-robins over the advertised nodes, so ~1/N of
    # migrated-slot writes get MOVED to the dead node and STALL (write latency ~2x,
    # throughput drops instead of rising). Evict dead nodes from raft + clear the
    # stale write-flip spec so every write routes to a reachable node.
    for sid, port, ips in SGS:
        m = meta[sid]
        for k, ip in enumerate(ips):
            if not is_alive(ip, port):
                try:
                    r = cli(m["leader"], m["port"], "RAFT.NODE", "REMOVE", str(k + 1)).strip()
                    print("evicted dead node raft_id=%d (%s) from %s -> %s" % (k + 1, ip, sid, r))
                except Exception as e:
                    print("evict dead node %s from %s failed: %s" % (ip, sid, e))
    for sid, port, ips in SGS:
        if sid == RECIPIENT:
            continue
        m = meta[sid]
        cli(m["leader"], m["port"], "CONFIG", "SET", "rdma-writeflip-spec", "")
    print("Computed final ownership:")
    for sid, _, _ in SGS:
        print("  %s: %s" % (sid, ", ".join("%d-%d" % r for r in ranges[sid])))
    # nodes advertised in the spec keep the configured membership (raft_id=index+1)
    argv = ["4"]
    for sid, port, ips in SGS:
        m = meta[sid]; rs = ranges[sid]
        # Advertise only LIVE nodes, leader FIRST. redisraft's external-shardgroup
        # MOVED round-robins over the advertised node list; a crash-killed node left
        # in the list makes ~1/N of migrated-slot writes MOVED to a dead node ->
        # client stalls (write latency ~2x). Excluding it (+ leader first) routes
        # every write to a reachable node. node_id keeps its raft_id (index+1).
        live = [(k, ips[k]) for k in range(len(ips)) if is_alive(ips[k], port)]
        live.sort(key=lambda ki: (ki[1] != m["leader"], ki[0]))
        argv += [m["dbid"], str(len(rs)), str(len(live))]
        for lo, hi in rs:
            argv += [str(lo), str(hi), "1", "0"]
        for k, ip in live:
            argv += ["%s%08x" % (m["dbid"], k + 1), "%s:%d" % (ip, port)]
    ok = True
    for sid, port, ips in SGS:
        m = meta[sid]
        out = cli(m["leader"], m["port"], "RAFT.SHARDGROUP", "REPLACE", *argv).strip()
        print("REPLACE on %s (%s:%d) -> %s" % (sid, m["leader"], m["port"], out))
        ok = ok and out == "OK"
    if not ok:
        print("FATAL: a REPLACE did not return OK", file=sys.stderr); sys.exit(1)
    print("ownership reconcile: OK (all leaders pushed consistent topology)")

if __name__ == "__main__":
    main()
