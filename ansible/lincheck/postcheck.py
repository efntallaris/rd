#!/usr/bin/env python3
"""Post-run checks for a linearizability run (ansible/lincheck/run_lin_scenario.sh).

The history checker (lincheck) proves the client-visible behaviour of the test
keys. This adds the migration / fault-tolerance side, once the run is over:

  migration  playbook rc == 0 and the CURRENT sg4 leader reports every migrated
             donor range durable (RDMA MGN-RESUME-STATUS: 0 pending, 0 missing)
  crashes    no crash signature in any node's log (the injected kill is a
             SIGKILL and leaves none)
  replicas   every live sg4 replica holds the same migrated-slot keyspace
             (keys + values, read locally via RAFT.DEBUG EXEC, so followers
             answer from their own data) — the durability claim behind RECP_DURABLE
  bulk       every YCSB key of the migrated slots in the donor's frozen copy is
             present on the recipient leader (YCSB never deletes, EVICT is off)

Exit 0 = all PASS, 1 = some FAIL. One line per check on stdout.
"""
import argparse
import binascii
import hashlib
import socket
import subprocess
import sys
import time

import os
# SG4_HOSTS="h1 h2 ..." overrides the recipient group's members (5-replica tests).
SG4 = [(h, 8000) for h in os.environ.get("SG4_HOSTS", "redis3 redis4 redis5").split()]
DONORS = {  # range lo, hi -> donor shardgroup members (any may be leader now)
    (0, 1364): [("redis0", 8000), ("redis1", 8000), ("redis2", 8000)],
    (5461, 6825): [("redis1", 8001), ("redis2", 8001), ("redis0", 8001)],
    (10922, 12286): [("redis2", 8002), ("redis0", 8002), ("redis1", 8002)],
}
RECIPIENTS = {"sg4": SG4}                     # recipient group -> members
RANGE_RECIPIENT = {r: "sg4" for r in DONORS}  # migrated range -> its recipient group
# AQ_TOPOLOGY=<file>: the topology written by tasks/cluster/scaleout_pairs_prepare.yml
# (3 -> 6 scale-out: several recipient groups, each donor's range on its own).
if os.environ.get("AQ_TOPOLOGY"):
    import json
    _t = json.load(open(os.environ["AQ_TOPOLOGY"]))
    _g = {g["id"]: g for g in _t["groups"]}
    _m = lambda g: [(n["host"], int(g["port"])) for n in g["nodes"]]
    DONORS, RECIPIENTS, RANGE_RECIPIENT = {}, {}, {}
    for g in _g.values():
        if g.get("role") == "donor":
            lo = int(str(g["slot_config"]).split(":")[0])
            r = (lo, lo + int(_t["slots_per_source"]) - 1)
            DONORS[r] = _m(g)
            RECIPIENTS[g["recipient"]] = _m(_g[g["recipient"]])
            RANGE_RECIPIENT[r] = g["recipient"]
HOSTS = sorted({h for ms in list(DONORS.values()) + list(RECIPIENTS.values()) for h, _ in ms})
CRASH_RE = "ASSERTION FAILED|REDIS BUG REPORT|crashed by signal|Guru Meditation"


# ---------------- minimal RESP client ----------------
class Resp:
    def __init__(self, host, port, timeout=10.0):
        self.s = socket.create_connection((host, port), timeout=timeout)
        self.f = self.s.makefile("rb")

    def close(self):
        try:
            self.s.close()
        except OSError:
            pass

    @staticmethod
    def enc(args):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            b = a if isinstance(a, bytes) else str(a).encode()
            out.append(b"$%d\r\n%s\r\n" % (len(b), b))
        return b"".join(out)

    def read(self):
        line = self.f.readline()
        if not line:
            raise ConnectionError("closed")
        t, rest = line[:1], line[1:-2]
        if t == b"+":
            return rest.decode()
        if t == b"-":
            return RuntimeError(rest.decode())
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            data = self.f.read(n + 2)[:-2]
            return data
        if t == b"*":
            n = int(rest)
            if n < 0:
                return None
            return [self.read() for _ in range(n)]
        raise ValueError("bad RESP type %r" % t)

    def call(self, *args):
        self.s.sendall(self.enc(args))
        return self.read()

    def pipeline(self, cmds, batch=1000):
        out = []
        for i in range(0, len(cmds), batch):
            chunk = cmds[i:i + batch]
            self.s.sendall(b"".join(self.enc(c) for c in chunk))
            out.extend(self.read() for _ in chunk)
        return out


def crc16(b):
    # CRC-16/XMODEM (poly 0x1021, init 0) = Redis Cluster's key hash. binascii's C
    # implementation: the bitwise Python loop took over 20 minutes on a 30M dataset.
    return binascii.crc_hqx(b, 0)


def slot_of(key):
    s = key.find(b"{")
    if s != -1:
        e = key.find(b"}", s + 1)
        if e != -1 and e != s + 1:
            key = key[s + 1:e]
    return crc16(key) % 16384


def in_ranges(slot):
    return any(lo <= slot <= hi for lo, hi in DONORS)


def connect(host, port):
    try:
        return Resp(host, port)
    except OSError:
        return None


def raft_role(host, port):
    c = connect(host, port)
    if c is None:
        return None
    try:
        info = c.call("INFO", "raft")
        if isinstance(info, bytes):
            for ln in info.decode().splitlines():
                if ln.startswith("raft_role:"):
                    return ln.split(":", 1)[1].strip()
        return "?"
    except (OSError, ConnectionError):
        return None
    finally:
        c.close()


def leader_of(members):
    for h, p in members:
        if raft_role(h, p) == "leader":
            return (h, p)
    return None


def local_keys(host, port, want_slot=in_ranges):
    """All keys of the migrated slots in this node's OWN keyspace."""
    c = Resp(host, port, timeout=60)
    keys, cur = [], b"0"
    try:
        while True:
            r = c.call("RAFT.DEBUG", "EXEC", "SCAN", cur, "COUNT", "10000")
            if isinstance(r, Exception):
                raise RuntimeError("SCAN on %s:%d: %s" % (host, port, r))
            cur, batch = r[0], r[1]
            keys.extend(k for k in batch if want_slot(slot_of(k)))
            if cur in (b"0", "0", 0):
                break
    finally:
        c.close()
    return keys


def local_values(host, port, keys):
    c = Resp(host, port, timeout=60)
    try:
        rs = c.pipeline([("RAFT.DEBUG", "EXEC", "GET", k) for k in keys])
    finally:
        c.close()
    vals = []
    for r in rs:
        if isinstance(r, list) and len(r) >= 3:   # slot-meta GET reply
            r = r[2]
        vals.append(r if isinstance(r, (bytes, type(None))) else repr(r).encode())
    return vals


def digest(host, port, want_slot=in_ranges):
    keys = sorted(set(local_keys(host, port, want_slot=want_slot)))
    vals = local_values(host, port, keys)
    h = hashlib.sha256()
    for k, v in zip(keys, vals):
        h.update(k + b"\0" + (v if v is not None else b"<nil>") + b"\n")
    return len(keys), h.hexdigest(), dict(zip(keys, vals))


# ---------------- checks ----------------
def check_migration(playbook_rc):
    if playbook_rc != 0:
        return False, "playbook rc=%d" % playbook_rc
    bad, leaders = [], []
    for rid, members in RECIPIENTS.items():
        ldr = leader_of(members)
        if ldr is None:
            return False, "no %s leader" % rid
        leaders.append("%s leader %s:%d" % (rid, ldr[0], ldr[1]))
        c = Resp(*ldr)
        try:
            for (lo, hi), x in RANGE_RECIPIENT.items():
                if x != rid:
                    continue
                r = c.call("RDMA", "MGN-RESUME-STATUS", lo, hi)
                if isinstance(r, Exception) or not isinstance(r, list) or len(r) < 2:
                    bad.append("%d-%d:%r" % (lo, hi, r))
                elif int(r[0]) != 0 or int(r[1]) != 0:
                    bad.append("%d-%d pending=%s missing=%s" % (lo, hi, r[0], r[1]))
        finally:
            c.close()
    return (not bad), "%s; %s" % (", ".join(leaders), "all %d ranges durable" % len(DONORS) if not bad else "; ".join(bad))


def check_crashes(logs_dir=None):
    hits = []
    # The playbook's collect_results deletes /tmp/redis_logs on most hosts before
    # this runs, so also read the copies the runner took during the run
    # (<logs_dir>/<host>/*.log): a node that crashed mid-run is only there.
    if logs_dir:
        import glob
        import re
        for f in sorted(glob.glob(os.path.join(logs_dir, "*", "*.log"))):
            with open(f, errors="replace") as fh:
                if re.search(CRASH_RE, fh.read()):
                    hits.append("%s:%s" % (os.path.basename(os.path.dirname(f)), os.path.basename(f)))
    for h in HOSTS:
        try:
            out = subprocess.run(
                ["sudo", "ssh", "-o", "ConnectTimeout=8", h,
                 "grep -lE '%s' /tmp/redis_logs/*.log 2>/dev/null" % CRASH_RE],
                capture_output=True, text=True, timeout=30).stdout.split()
        except subprocess.TimeoutExpired:
            out = ["<ssh timeout>"]
        hits += ["%s:%s" % (h, f.rsplit("/", 1)[-1]) for f in out]
    hits = sorted(set(hits))
    return (not hits), ("none" if not hits else ", ".join(hits))


def check_replicas(settle_s):
    """Every recipient group, each on the range(s) it received."""
    oks, msgs, allres = [], [], {}
    for rid, members in RECIPIENTS.items():
        ok, msg, res = check_group_replicas(rid, members, settle_s)
        oks.append(ok)
        msgs.append(msg if len(RECIPIENTS) == 1 else "%s: %s" % (rid, msg))
        allres.update(res or {})
    return all(oks), " ;; ".join(msgs), allres or None


def check_group_replicas(rid, members, settle_s):
    mine = [r for r, x in RANGE_RECIPIENT.items() if x == rid]
    want = lambda sl: any(lo <= sl <= hi for lo, hi in mine)
    live = [(h, p) for h, p in members if raft_role(h, p) is not None]
    if len(live) < 2:
        return False, "only %d live %s replica(s)" % (len(live), rid), None
    deadline = time.time() + settle_s
    while True:
        res = {hp: digest(*hp, want_slot=want) for hp in live}
        digs = {d for _, d, _ in res.values()}
        if len(digs) == 1 or time.time() > deadline:
            break
        time.sleep(5)   # followers may still be applying / merging
    detail = ", ".join("%s n=%d %s" % (h, n, d[:12]) for (h, _), (n, d, _) in res.items())
    if len(digs) == 1:
        return True, "%d live replicas identical (%s)" % (len(live), detail), res
    # explain the first divergence
    maps = {hp: m for hp, (_, _, m) in res.items()}
    ref_hp = next(iter(maps))
    ex = []
    for hp, m in maps.items():
        if hp == ref_hp:
            continue
        ref = maps[ref_hp]
        only_ref = [k for k in ref if k not in m][:3]
        only_hp = [k for k in m if k not in ref][:3]
        diffv = [k for k in ref if k in m and ref[k] != m[k]][:3]
        ex.append("%s vs %s: missing=%s extra=%s diff=%s" % (ref_hp[0], hp[0], only_ref, only_hp, diffv))
    try:
        why = explain_divergence(res)
    except Exception as e:   # noqa: BLE001
        why = "explain failed: %s" % e
    return False, "DIVERGED (%s) %s || WHERE: %s" % (detail, " | ".join(ex), why), res


def check_donor_replicas(settle_s):
    """Each donor shardgroup's live replicas hold the same keys in its migrated
    range (they keep the frozen copy: EVICT is off)."""
    bad, notes = False, []
    for (lo, hi), members in DONORS.items():
        live = [hp for hp in members if raft_role(*hp) is not None]
        want = lambda sl, lo=lo, hi=hi: lo <= sl <= hi
        deadline = time.time() + settle_s
        while True:
            sets = {hp: set(local_keys(*hp, want_slot=want)) for hp in live}
            if len({frozenset(v) for v in sets.values()}) <= 1 or time.time() > deadline:
                break
            time.sleep(5)
        if len({frozenset(v) for v in sets.values()}) > 1:
            bad = True
            ref = live[0]
            for hp in live[1:]:
                notes.append("%d-%d %s:%d vs %s:%d missing=%d extra=%d e.g. %s" % (
                    lo, hi, ref[0], ref[1], hp[0], hp[1], len(sets[ref] - sets[hp]),
                    len(sets[hp] - sets[ref]), sorted(sets[ref] ^ sets[hp])[:3]))
        else:
            notes.append("%d-%d: %d live replicas agree (%d keys)" % (
                lo, hi, len(live), len(next(iter(sets.values()))) if sets else 0))
    return (not bad), "; ".join(notes)


def explain_divergence(res):
    """For keys some sg4 replica lacks, say which donor replicas hold them."""
    maps = {hp: m for hp, (_, _, m) in res.items()}
    allk = set().union(*[set(m) for m in maps.values()])
    odd = sorted(k for k in allk if not all(k in m for m in maps.values()))[:200]
    out = []
    for k in odd[:5]:
        sl = slot_of(k)
        holders = [h for (h, _), m in maps.items() if k in m]
        dons = []
        for (lo, hi), members in DONORS.items():
            if lo <= sl <= hi:
                for hp in members:
                    c = connect(*hp)
                    if c is None:
                        dons.append("%s:%d=dead" % hp)
                        continue
                    try:
                        r = c.call("RAFT.DEBUG", "EXEC", "EXISTS", k)
                        dons.append("%s:%d=%s" % (hp[0], hp[1], r))
                    finally:
                        c.close()
        out.append("%s slot=%d sg4-holders=%s donors[%s]" % (k.decode(), sl, holders, " ".join(dons)))
    with open("/tmp/lincheck/last_divergent_keys.txt", "w") as f:
        f.write("\n".join(k.decode() for k in odd))
    return " || ".join(out)


def check_bulk(res):
    """res: check_replicas' digests (reused for the recipient leaders' keys)."""
    missing_total, checked = 0, 0
    notes = []
    for (lo, hi), members in DONORS.items():
        rid = RANGE_RECIPIENT[(lo, hi)]
        rl = leader_of(RECIPIENTS[rid])
        if rl is None:
            notes.append("%d-%d: no %s leader" % (lo, hi, rid))
            missing_total += 1
            continue
        sg4_leader_keys = res[rl][2] if res and rl in res else dict.fromkeys(
            local_keys(*rl, want_slot=lambda s, lo=lo, hi=hi: lo <= s <= hi))
        ldr = leader_of(members) or next(((h, p) for h, p in members if raft_role(h, p) is not None), None)
        if ldr is None:
            notes.append("%d-%d: no live donor replica" % (lo, hi))
            missing_total += 1
            continue
        dk = [k for k in local_keys(*ldr, want_slot=lambda s: lo <= s <= hi) if k.startswith(b"user")]
        miss = [k for k in dk if k not in sg4_leader_keys]
        checked += len(dk)
        missing_total += len(miss)
        if miss:
            notes.append("%d-%d: %d/%d missing e.g. %s" % (lo, hi, len(miss), len(dk), miss[:3]))
    ok = missing_total == 0 and checked > 0
    return ok, "%d donor YCSB keys checked, %d missing%s" % (
        checked, missing_total, ("; " + "; ".join(notes)) if notes else "")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--playbook-rc", type=int, required=True)
    ap.add_argument("--settle-s", type=int, default=90)
    ap.add_argument("--logs-dir", default=None, help="node logs copied during the run (<dir>/<host>/*.log)")
    a = ap.parse_args()

    results = []
    ok, msg = check_migration(a.playbook_rc)
    results.append(("migration", ok, msg))
    ok, msg = check_crashes(a.logs_dir)
    results.append(("crashes", ok, msg))
    try:
        ok, msg, res = check_replicas(a.settle_s)
    except Exception as e:   # noqa: BLE001 — report, don't die
        ok, msg, res = False, "error: %s" % e, None
    results.append(("replicas", ok, msg))
    try:
        ok, msg = check_donor_replicas(min(a.settle_s, 30))
    except Exception as e:   # noqa: BLE001
        ok, msg = False, "error: %s" % e
    results.append(("donors", ok, msg))
    try:
        ok, msg = check_bulk(res)
    except Exception as e:   # noqa: BLE001
        ok, msg = False, "error: %s" % e
    results.append(("bulk", ok, msg))

    for name, ok, msg in results:
        print("POSTCHECK %-9s %s  %s" % (name, "PASS" if ok else "FAIL", msg))
    allok = all(ok for _, ok, _ in results)
    print("POSTCHECK VERDICT: %s" % ("PASS" if allok else "FAIL"))
    sys.exit(0 if allok else 1)


if __name__ == "__main__":
    main()
