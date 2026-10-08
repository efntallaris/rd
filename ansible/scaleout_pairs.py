#!/usr/bin/env python3
"""3 -> 6 scale-out driver: every donor shardgroup migrates to its OWN recipient.

The single-recipient reshard (reshard_cluster_rdma_v2_orchestrated_nround.yml)
sends all donors to sg4 through RDMA MIGRATE-ALL. Here each donor is dispatched
on its own with RDMA MIGRATE (sg1 -> sg4, sg2 -> sg5, sg3 -> sg6), so there is no
orchestrator node; this script is the driver and it only talks to CURRENT
leaders, so it keeps working when a leader is killed.

  scaleout_pairs.py plan     <topology.json>
      print the pairs, ranges and ports (no cluster access)
  scaleout_pairs.py prepare  <topology.json> [--rounds N] [--warm-skip sg1]
      RDMA ports + write-flip spec on the donors, MIGRATE-WARM, CHAIN-WARM
  scaleout_pairs.py migrate  <topology.json> [--rounds N] [--mode parallel|sequential]
                             [--async-apply yes|no]
      dispatch, poll to DONE, re-drive whatever a crash interrupted, wait for
      the recipients' merges, NARROW the donors, push the final 6-group map

The topology file is written by tasks/cluster/scaleout_pairs_prepare.yml from
redisraft_shardgroups: {"slots_per_source": N, "groups": [...]}.
Exit 0 = every pair migrated and the ownership handed over.
"""
import argparse
import json
import re
import socket
import subprocess
import sys
import time

CLI = "/users/entall/rd/redis_bin/bin/redis-cli"


def say(msg):
    print(msg, flush=True)


class _Resp:
    """One request over a plain socket, rendered the way redis-cli prints it when
    its output is not a terminal (one element per line, errors without the '-').
    Spawning redis-cli costs ~5 ms a call and the migrate loop makes ~20 calls per
    transfer: 0.1 s between transfers, 1.8 s of an 11 s scale-out (2026-10-03)."""

    def __init__(self, ip, port, t):
        self.s = socket.create_connection((ip, port), timeout=t)
        self.s.settimeout(t)
        self.f = self.s.makefile("rb")

    def call(self, args):
        b = b"*%d\r\n" % len(args)
        for a in args:
            a = str(a).encode()
            b += b"$%d\r\n%s\r\n" % (len(a), a)
        self.s.sendall(b)
        out = []
        self._read(out)
        return ("\n".join(out) + "\n").replace("\r", "")

    def _read(self, out):
        ln = self.f.readline()
        if not ln:
            raise ConnectionError("closed")
        k, v = ln[:1], ln[1:].rstrip(b"\r\n").decode(errors="replace")
        if k in (b"+", b"-", b":"):
            out.append(v)
        elif k == b"$":
            n = int(v)
            if n < 0:
                out.append("")
            else:
                out.append(self.f.read(n + 2)[:-2].decode(errors="replace"))
        elif k == b"*":
            for _ in range(max(int(v), 0)):
                self._read(out)
        else:
            raise ConnectionError("unexpected reply %r" % ln)

    def close(self):
        try:
            self.s.close()
        except OSError:
            pass


def cli(ip, port, *args, t=5):
    c = None
    try:
        c = _Resp(ip, port, t)
        return c.call(args)
    except ConnectionRefusedError:
        return "Could not connect to Redis at %s:%s: Connection refused\n" % (ip, port)
    except socket.timeout:
        return "ERR timeout"
    except Exception:
        return cli_proc(ip, port, *args, t=t)
    finally:
        if c is not None:
            c.close()


def cli_proc(ip, port, *args, t=5):
    try:
        r = subprocess.run([CLI, "-t", str(t), "-h", ip, "-p", str(port)] + [str(a) for a in args],
                           capture_output=True, text=True, timeout=t + 30)
        return (r.stdout + r.stderr).replace("\r", "")
    except subprocess.TimeoutExpired:
        return "ERR timeout"


def unreachable(out):
    return bool(re.search(r"could not connect|connection refused|ERR timeout", out, re.I))


class Group:
    def __init__(self, g):
        self.id = g["id"]
        self.port = int(g["port"])
        self.role = g.get("role") or ("recipient" if g["id"] == "sg4" else "donor")
        self.recipient = g.get("recipient")
        self.rdma_port = g.get("rdma_port")
        self.lo, self.hi = (int(x) for x in str(g["slot_config"]).split(":"))
        self.nodes = [(n["host"], int(n["raft_id"])) for n in g["nodes"]]
        self.ips = {}

    def resolve(self):
        self.ips = {h: socket.gethostbyname(h) for h, _ in self.nodes}

    def members(self):
        return [(h, self.ips[h], rid) for h, rid in self.nodes]

    def info(self, ip, section="raft"):
        d = {}
        for ln in cli(ip, self.port, "INFO", section, t=2).splitlines():
            if ":" in ln:
                k, v = ln.split(":", 1)
                d[k.strip()] = v.strip()
        return d

    def leader(self):
        """(host, ip) of the current leader, or None."""
        m = self.members()
        m.sort(key=lambda x: x[1] != getattr(self, "_last_leader", None))   # last known leader first
        for h, ip, _ in m:
            if self.info(ip).get("raft_role", "").lower() == "leader":
                self._last_leader = ip
                return h, ip
        return None

    def wait_leader(self, secs=120):
        t0 = time.time()
        while time.time() - t0 < secs:
            ld = self.leader()
            if ld:
                return ld
            time.sleep(1)
        raise RuntimeError("no leader for %s within %ds" % (self.id, secs))

    def dbid(self):
        _, ip = self.wait_leader()
        return self.info(ip)["raft_dbid"]

    def node_argv(self, dbid, only=None):
        out = []
        for h, ip, rid in self.members():
            if only is None or ip in only:
                out += ["%s%08x" % (dbid, rid), "%s:%d" % (ip, self.port)]
        return out


class Pair:
    def __init__(self, donor, recip, per):
        self.donor, self.recip, self.per = donor, recip, per
        self.lo, self.hi = donor.lo, donor.lo + per - 1          # migrated range
        self.keep = (donor.lo + per, donor.hi)                   # what the donor keeps

    def __str__(self):
        return "%s->%s" % (self.donor.id, self.recip.id)


def load(path, resolve=True):
    topo = json.load(open(path))
    groups = {g["id"]: Group(g) for g in topo["groups"]}
    per = int(topo["slots_per_source"])
    pairs = []
    for g in groups.values():
        if g.role != "donor":
            continue
        if g.recipient not in groups or groups[g.recipient].role != "recipient":
            sys.exit("%s: recipient %r is not a recipient group" % (g.id, g.recipient))
        if not 0 < per < g.hi - g.lo + 1:
            sys.exit("%s: cannot give %d of its %d slots" % (g.id, per, g.hi - g.lo + 1))
        r = groups[g.recipient]
        if (r.lo, r.hi) != (g.lo, g.lo + per - 1):
            sys.exit("%s: slot_config %d:%d must be the range it receives from %s, %d:%d" % (
                r.id, r.lo, r.hi, g.id, g.lo, g.lo + per - 1))
        pairs.append(Pair(g, groups[g.recipient], per))
    if resolve:
        for g in groups.values():
            g.resolve()
    return groups, pairs


def round_range(p, ri, rounds):
    chunk = p.per // rounds
    cnt = chunk if ri < rounds - 1 else p.per - chunk * (rounds - 1)
    lo = p.lo + chunk * ri
    return lo, lo + cnt - 1, cnt


# ---------------- plan ----------------
def cmd_plan(a):
    groups, pairs = load(a.topology, resolve=False)
    for p in pairs:
        say("%s  %s:%d -> %s:%d  migrates %d-%d (%d slots), keeps %d-%d  rdma-port %s/%s" % (
            p, p.donor.nodes[0][0], p.donor.port, p.recip.nodes[0][0], p.recip.port,
            p.lo, p.hi, p.per, p.keep[0], p.keep[1], p.donor.rdma_port, p.recip.rdma_port))
        for ri in range(a.rounds):
            say("    round %d: %d-%d (%d slots)" % ((ri,) + round_range(p, ri, a.rounds)))
    say("lin slot ranges: " + ",".join("%d-%d" % (p.lo, p.hi) for p in pairs))


# ---------------- prepare ----------------
def cmd_prepare(a):
    groups, pairs = load(a.topology)
    skip = set(a.warm_skip.split(",")) if a.warm_skip else set()
    for p in pairs:
        dbid = p.recip.dbid()
        spec = " ".join([dbid] + p.recip.node_argv(dbid))
        # On EVERY donor replica: one promoted after a donor-leader crash issues
        # the WRITE_FLIP for the resumed session itself, on the pair's RDMA port.
        for h, ip, _ in p.donor.members():
            if p.donor.rdma_port:
                cli(ip, p.donor.port, "CONFIG", "SET", "rdma-migration-port", p.donor.rdma_port)
            out = cli(ip, p.donor.port, "CONFIG", "SET", "rdma-writeflip-spec", spec).strip()
            say("%s: write-flip spec on %s -> %s" % (p, h, out))
            if out != "OK":
                sys.exit("%s: could not set rdma-writeflip-spec on %s" % (p, h))
    chunk = max(round_range(pairs[0], ri, a.rounds)[2] for ri in range(a.rounds))
    for p in pairs:
        rhost, _ = p.recip.wait_leader()
        if p.donor.id in skip:
            say("%s: donor warm skipped (--warm-skip)" % p)
        else:
            _, dip = p.donor.wait_leader()
            # The followers first (best effort), the leader last. A follower
            # promoted after a donor-leader crash is otherwise cold: it registers
            # every block with the NIC inside TRANSFER (57 MRs, ~85 ms per chunk),
            # so each of its remaining rounds copies in 0.70 s instead of 0.35 s
            # with the group's clients stalled (S2). Warming it after the crash,
            # under load, is worse: 4 s with the group frozen (tried 2026-10-04).
            if a.warm_followers == "yes":
                for h, ip, _ in p.donor.members():
                    if ip != dip:
                        o = cli(ip, p.donor.port, "RDMA", "MIGRATE-WARM", rhost, p.recip.port, p.per, t=30).strip()
                        say("%s: MIGRATE-WARM on follower %s -> %s" % (p, h, o[:80]))
            for attempt in range(3):
                out = cli(dip, p.donor.port, "RDMA", "MIGRATE-WARM", rhost, p.recip.port, p.per, t=30).strip()
                if "OK" in out:
                    break
                time.sleep(1)
            say("%s: MIGRATE-WARM -> %s" % (p, out))
            if "OK" not in out:
                sys.exit("%s: MIGRATE-WARM failed" % p)
        _, rip = p.recip.wait_leader()
        for attempt in range(3):
            out = cli(rip, p.recip.port, "RDMA", "CHAIN-WARM", chunk, t=30).strip()
            if "OK" in out:
                break
            time.sleep(1)
        say("%s: CHAIN-WARM %d -> %s" % (p, chunk, out))
        if "OK" not in out:
            sys.exit("%s: CHAIN-WARM failed" % p)
        # A recipient group has served nothing yet: its first Raft entries (and its
        # followers' first replies) are slow, and the first round's setup then took
        # 80 ms instead of 30 ms with the round's writes already redirected (a 0.2 s
        # dip of ~25% in every fault-free run, 2026-10-05). Put a few writes through
        # each recipient group now. The key is in the recipient's range and is
        # deleted again.
        import binascii
        wk = next("__warm_%d" % i for i in range(100000)
                  if p.lo <= binascii.crc_hqx(("__warm_%d" % i).encode(), 0) % 16384 <= p.hi)
        okw = 0
        for i in range(200):
            if cli(rip, p.recip.port, "SET", wk, i, t=2).strip() == "OK":
                okw += 1
        cli(rip, p.recip.port, "DEL", wk, t=2)
        say("%s: warm-up writes through %s: %d/200 ok (key %s)" % (p, p.recip.id, okw, wk))
    time.sleep(a.settle)   # registrations and chain establishment run off-thread


# ---------------- migrate ----------------
def dispatch(p, cnt):
    """Start one donor's migration. Returns (donor leader ip, migration id) or (None, reason)."""
    dl, rl = p.donor.leader(), p.recip.leader()
    if dl is None or rl is None:
        return None, "no %s leader" % (p.donor.id if dl is None else p.recip.id)
    out = cli(dl[1], p.donor.port, "RDMA", "MIGRATE", rl[0], p.recip.port, cnt).strip()
    m = re.search(r"migration_id=(\d+)", out)
    if not m:
        return None, out
    say("%s: dispatched %d slots from %s to %s:%d (migration %s)" % (p, cnt, dl[0], rl[0], p.recip.port, m.group(1)))
    return dl[1], m.group(1)


def wait_tail_hop(p, prev, timeout=3.0):
    """Before starting p's copy: if p's recipient leader runs on the host that is the LAST replica
    of the previous pair's replication chain, wait until that replica holds the previous round.

    The hop to the last replica is not needed for the commit, so it is still running when the
    next pair starts. Normally it lands on another host. After a recipient-leader failover the
    new leader can sit on that host, and then the donor's copy and the hop share one 25 Gb/s
    port: both ran at half rate and client throughput dipped at every one of that pair's rounds
    (3 -> 6 S1, 2026-10-05: copy 0.34 -> 0.67 s, hop 0.36 -> 1.2 s)."""
    if not prev or prev[0] is p:
        return
    q, lo, cnt, qri = prev
    rl, ql = p.recip.leader(), q.recip.leader()
    if rl is None or ql is None or rl[1] == ql[1]:
        return
    cf = cli(ql[1], q.recip.port, "CONFIG", "GET", "rdma-chain-followers", t=2).split("\n")
    order = cf[1].split() if len(cf) > 1 else []
    if not order:
        return
    try:
        tail_ip = socket.gethostbyname(order[-1].rsplit(":", 1)[0])
    except OSError:
        return
    if tail_ip != rl[1]:
        return
    # A replica files what it received under the chain session, 7e17 + the number of rounds this
    # group's current leader has replicated: the round index, or less after a leader change
    # there. Session 0 is not "any session" (it answered 0 slots for good: every wait ran into
    # the 3 s timeout, 2026-10-05). Ask for the expected one, and for the others now and then.
    t0, it, sess = time.time(), 0, [700000000000000000 + qri] + [700000000000000000 + k for k in range(8) if k != qri]
    done = False
    while not done and time.time() - t0 < timeout:
        for sid in (sess if it % 5 == 4 else sess[:1]):
            out = cli(rl[1], q.recip.port, "RDMA", "CHAIN-STATUS", sid, lo, lo + cnt - 1, t=2)
            if unreachable(out) or len(re.findall(r"^\d+$", out, re.M)) >= cnt:
                done = True
                break
        it += 1
        if not done:
            time.sleep(0.01)
    w = time.time() - t0
    if w > 0.05:
        say("%s: waited %.2f s for %s's last replica on %s to receive slots %d-%d before copying to that host"
            % (p, w, q.recip.id, rl[0], lo, lo + cnt - 1))


def poll(running, timeout):
    """running: {pair: (donor ip, migration id, first slot)} -> {pair: DONE|FAILED|DONORDOWN|TIMEOUT}"""
    final, down, down_t = {}, {p: 0 for p in running}, {}
    t0 = time.time()
    nxt = {p: t0 + 2 for p in running}
    while len(final) < len(running) and time.time() - t0 < timeout:
        for p, (ip, mid, lo) in running.items():
            if p in final:
                continue
            out = cli(ip, p.donor.port, "RDMA", "MIGRATE-STATUS", mid, t=2)
            st = re.search(r"DONE|FAILED", out.split("\n", 1)[0])
            if not st and time.time() >= nxt[p]:
                # A donor whose recipient leader died re-ships the round itself under
                # id 8e17 + first slot and leaves the original id in BACKPATCH for
                # good (S1 sequential, 2026-10-03: 120 s spent polling the dead id).
                nxt[p] = time.time() + 1
                o2 = cli(ip, p.donor.port, "RDMA", "MIGRATE-STATUS", 800000000000000000 + max(lo, 1), t=2)
                st = re.search(r"DONE|FAILED", o2.split("\n", 1)[0])
                if st:
                    say("%s: migration %s superseded by the donor's own re-ship (%s)" % (p, mid, st.group(0)))
            if st:
                final[p] = st.group(0)
            elif unreachable(out) or not re.match(r"[A-Z]+\n", out):
                # No status at all: the donor leader was killed (refused), or it
                # crashed and hangs in its crash report (no answer). Its status
                # can never turn DONE; after 1 s of that (and 3 polls) leave it to the re-drive.
                down[p] += 1
                down_t.setdefault(p, time.time())
                if down[p] >= 3 and time.time() - down_t[p] >= 1.0:
                    final[p] = "DONORDOWN"
            else:
                down[p] = 0
                down_t.pop(p, None)
        time.sleep(0.005)
    for p in running:
        final.setdefault(p, "TIMEOUT")
    return final


def redrive(todo, ri, rounds):
    """Roll forward the pairs whose round did not end DONE. The recipient's
    durable state is the source of truth: ask its CURRENT leader what is durable
    (MGN-RESUME-STATUS) and re-ship the rest from the donor's CURRENT leader with
    MGN-RECOVER (exact slots; a re-ship is idempotent on the recipient)."""
    # Polls every 0.1 s (was 2 s per pass and a fixed 3 s after every re-drive: 4.2 s
    # without a transfer in S8, 2026-10-04).
    tries, stuck = {}, {}
    for w in range(1, 6001):
        ndur, summary = 0, []
        for p in todo:
            lo, hi, cnt = round_range(p, ri, rounds)
            rl = p.recip.leader()
            if rl is None:
                summary.append("%s=no-recipient-leader" % p)
                continue
            st = cli(rl[1], p.recip.port, "RDMA", "MGN-RESUME-STATUS", lo, hi, t=3).split()
            if len(st) < 2 or not (st[0].isdigit() and st[1].isdigit()):
                summary.append("%s=?" % p)
                continue
            pend, miss = int(st[0]), int(st[1])
            if pend == 0 and miss == 0:
                ndur += 1
                summary.append("%s=durable" % p)
                continue
            dl = p.donor.leader()
            if dl is None:
                summary.append("%s=no-donor-leader" % p)
                continue
            mst = cli(dl[1], p.donor.port, "RDMA", "MIGRATE-STATUS", t=2).split("\n", 1)[0].strip()
            running = bool(mst) and not re.search(r"DONE|FAILED|ERR|no migrations", mst)
            # Pending = the recipient has the blocks but has not made them durable.
            # While the donor's migration runs it will finish that itself. If the
            # donor has stopped (FAILED) the batch is orphaned, e.g. the recipient
            # gave up on a slow transfer; give it ~10 s, then re-drive it too (the
            # donor's resume waits for a pending batch and re-ships what is left).
            stuck[p] = (stuck.get(p) or time.time()) if (pend and not running) else 0
            if running or (pend and time.time() - stuck[p] < 10):
                summary.append("%s=waiting(p=%d,m=%d,donor=%s)" % (p, pend, miss, mst))
                continue
            n = tries.get((p, rl[1]), 0)
            if n >= 3:
                summary.append("%s=gave-up" % p)
                continue
            tok = 700 + ri * 100 + n * 10 + int(re.sub(r"\D", "", p.donor.id) or 0)
            cli(dl[1], p.donor.port, "RDMA", "MGN-RECOVER", "donor", tok,
                "sess=%d slots=%d-%d n=%d recipient=%s:%d" % (tok, lo, hi, cnt, rl[1], p.recip.port))
            tries[(p, rl[1])] = n + 1
            stuck[p] = 0
            say("  re-drive: %s slots=%d-%d from %s to %s leader %s (missing=%d, pending=%d, attempt %d, token %d)" % (
                p, lo, hi, dl[0], p.recip.id, rl[0], miss, pend, n + 1, tok))
            summary.append("%s=dispatched" % p)
            for _ in range(60):   # let MIGRATE-STATUS show the new worker before the next pass (at most 3 s)
                m2 = cli(dl[1], p.donor.port, "RDMA", "MIGRATE-STATUS", t=2).split("\n", 1)[0].strip()
                if m2 and not re.search(r"DONE|FAILED|ERR|no migrations", m2):
                    break
                time.sleep(0.05)
        if w % 50 == 1:
            say("  re-drive poll %d: %d/%d durable: %s" % (w, ndur, len(todo), " ".join(summary)))
        if ndur == len(todo):
            say("RE-DRIVE COMPLETE: %s durable on the recipient leader(s)" % ", ".join(str(p) for p in todo))
            return True
        time.sleep(0.1)
    say("RE-DRIVE INCOMPLETE: " + " ".join(summary))
    return False


def drain(pairs):
    """async-apply: a recipient reports a donor done at commit, before its merge
    has run. NARROW must not hand the slots over while keys are still unmerged."""
    prev, stable = None, 0
    for w in range(1, 301):
        vals = []
        for p in pairs:
            rl = p.recip.leader()
            v = p.recip.info(rl[1], "cluster").get("rdma_recipient_backpatch_in_progress") if rl else None
            vals.append(v)
        if all(v == "0" for v in vals):
            say("DRAINED: backpatch_in_progress=0 on every recipient after %ds -> NARROW" % w)
            return
        stable = stable + 1 if (vals == prev and None not in vals) else 0
        if stable >= 30:
            say("STALL: backpatch_in_progress=%s unchanged 30s -> proceeding to NARROW" % vals)
            return
        prev = vals
        time.sleep(1)
    say("TIMEOUT: backpatch_in_progress=%s not drained in 300s -> proceeding" % vals)


def narrow(pairs):
    ok = True
    for p in pairs:
        dbid = p.recip.dbid()
        argv = ["%d:%d" % p.keep, dbid, 1, len(p.recip.nodes), p.lo, p.hi, 1, 0] + p.recip.node_argv(dbid)
        for attempt in range(5):
            _, dip = p.donor.wait_leader()
            out = cli(dip, p.donor.port, "RAFT.SHARDGROUP", "NARROW", *argv, t=30).strip()
            if out == "OK":
                break
            time.sleep(1)
        say("%s: NARROW donor to %d:%d -> %s" % ((p,) + p.keep + (out,)))
        ok = ok and out == "OK"
    return ok


def reconcile(groups, pairs):
    """Push the final 6-group slot map to every leader (RAFT.SHARDGROUP REPLACE),
    as reconcile_ownership.py does for the single-recipient setup: NARROW leaves
    each group knowing only its own handover. Dead nodes are removed from their
    group and left out of the map, or clients get MOVED to them."""
    meta, ranges = {}, {}
    for g in groups.values():
        _, lip = g.wait_leader()
        live = [ip for _, ip, _ in g.members() if g.info(ip).get("raft_role")]
        for h, ip, rid in g.members():
            if ip not in live:
                say("evicting dead node %s (raft_id=%d) from %s -> %s" % (
                    h, rid, g.id, cli(lip, g.port, "RAFT.NODE", "REMOVE", rid).strip()))
        meta[g.id] = (lip, g.info(lip)["raft_dbid"], [lip] + [ip for ip in live if ip != lip])
    for p in pairs:
        lip = meta[p.donor.id][0]
        cli(lip, p.donor.port, "CONFIG", "SET", "rdma-writeflip-spec", "")
        own = cli(lip, p.donor.port, "RAFT.SHARDGROUP", "GET").split()
        ranges[p.donor.id] = (int(own[1]), int(own[2]))
        ranges[p.recip.id] = (p.lo, ranges[p.donor.id][0] - 1)
        if ranges[p.recip.id] != (p.lo, p.hi):
            say("FATAL: %s owns %d-%d after NARROW, expected %d-%d" % ((p.donor.id,) + ranges[p.donor.id] + p.keep))
            return False
    argv = [len(groups)]
    for g in groups.values():
        lip, dbid, live = meta[g.id]
        argv += [dbid, 1, len(live), ranges[g.id][0], ranges[g.id][1], 1, 0]
        for ip in live:   # leader first
            argv += g.node_argv(dbid, only=[ip])
        say("  %s: %d-%d  nodes %s" % (g.id, ranges[g.id][0], ranges[g.id][1], " ".join(live)))
    ok = True
    for g in groups.values():
        out = cli(meta[g.id][0], g.port, "RAFT.SHARDGROUP", "REPLACE", *argv, t=30).strip()
        say("REPLACE on %s (%s:%d) -> %s" % (g.id, meta[g.id][0], g.port, out))
        ok = ok and out == "OK"
    return ok


def migrate_pipelined(a, pairs):
    """One donor -> recipient copy at a time, like sequential, but the next
    transfer is dispatched as soon as the previous one's copy is over (its donor
    reports BACKPATCH): the wait for that round's commit (chain replication,
    merge, MGN_RECP_DURABLE, TXN_DONE) overlaps the next pair's copy instead of
    sitting between two copies. A pair's own next round still waits for its
    previous round to be DONE. Any transfer that does not end DONE stops the
    pipeline: everything in flight is polled to the end and re-driven as in
    the other modes, then the pipeline resumes."""
    pending = []   # dispatched, not yet seen DONE: [round, pair, donor ip, migration id, first slot]

    def drain():
        if not pending:
            return True
        final = poll({e[1]: (e[2], e[3], e[4]) for e in pending}, a.poll_timeout)
        ok = True
        for ri in sorted({e[0] for e in pending}):
            mine = [e[1] for e in pending if e[0] == ri]
            say("round %d: %s" % (ri, " ".join("%s=%s" % (p, final[p]) for p in mine)))
            todo = [p for p in mine if final[p] != "DONE"]
            if todo and not redrive(todo, ri, a.rounds):
                ok = False
        del pending[:]
        return ok

    def ready(p):
        """True once p has nothing in flight and the newest transfer has finished copying."""
        t0 = time.time()
        while time.time() - t0 < 3:
            for e in list(pending):
                first = cli(e[2], e[1].donor.port, "RDMA", "MIGRATE-STATUS", e[3], t=2).split("\n", 1)[0]
                if re.search(r"DONE", first):
                    pending.remove(e)
                elif re.search(r"FAILED", first) or not re.match(r"[A-Z]+$", first):
                    return False
                else:
                    e[5] = first
            if not any(e[1] is p for e in pending) and (not pending or pending[-1][5] == "BACKPATCH"):
                return True
            time.sleep(0.002)
        return False

    for ri in range(a.rounds):
        for p in pairs:
            if not ready(p) and not drain():
                sys.exit(1)
            lo, _, cnt = round_range(p, ri, a.rounds)
            _, dip = p.donor.wait_leader()
            cli(dip, p.donor.port, "CONFIG", "SET", "rdma-reshard-migrated", lo - p.lo)
            ip, mid = dispatch(p, cnt)
            if ip is None:
                say("%s: round %d DISPATCH FAILED: %s" % (p, ri, mid))
                if not drain() or not redrive([p], ri, a.rounds):
                    sys.exit(1)
            else:
                pending.append([ri, p, ip, mid, lo, ""])
    if not drain():
        sys.exit(1)


def cmd_migrate(a):
    groups, pairs = load(a.topology)
    if a.mode == "pipelined":
        migrate_pipelined(a, pairs)
    prev = None   # (pair, first slot, slot count) of the round dispatched last
    for ri in range(a.rounds if a.mode != "pipelined" else 0):
        # The donor picks "the next N slots it owns after rdma-reshard-migrated" and
        # advances that counter itself, but only on the node that ran the round. Set
        # it on the current leader before every round, so a leader elected between
        # rounds (or a round finished by a re-drive) still takes the right chunk.
        for p in pairs:
            _, dip = p.donor.wait_leader()
            cli(dip, p.donor.port, "CONFIG", "SET", "rdma-reshard-migrated", round_range(p, ri, a.rounds)[0] - p.lo)
        final = {}
        batches = [pairs] if a.mode == "parallel" else [[p] for p in pairs]
        for batch in batches:
            running = {}
            for p in batch:
                lo, _, cnt = round_range(p, ri, a.rounds)
                if a.mode != "parallel":
                    wait_tail_hop(p, prev)
                prev = (p, lo, cnt, ri)
                ip, mid = dispatch(p, cnt)
                if ip is None:
                    say("%s: round %d DISPATCH FAILED: %s" % (p, ri, mid))
                    final[p] = "NODISPATCH"
                else:
                    running[p] = (ip, mid, lo)
            final.update(poll(running, a.poll_timeout))
        say("round %d: %s" % (ri, " ".join("%s=%s" % (p, final[p]) for p in pairs)))
        todo = [p for p in pairs if final[p] != "DONE"]
        if todo and not redrive(todo, ri, a.rounds):
            sys.exit(1)
    if a.async_apply == "yes":
        drain(pairs)
    if not narrow(pairs):
        sys.exit("NARROW failed")
    if not reconcile(groups, pairs):
        sys.exit("ownership reconcile failed")
    say("scale-out complete: %s" % ", ".join("%s %d-%d" % (p, p.lo, p.hi) for p in pairs))


def main():
    global CLI
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["plan", "prepare", "migrate"])
    ap.add_argument("topology")
    ap.add_argument("--cli", default=CLI)
    ap.add_argument("--rounds", type=int, default=1)
    ap.add_argument("--warm-followers", choices=["yes", "no"], default="yes")
    ap.add_argument("--mode", choices=["parallel", "sequential", "pipelined"], default="sequential")
    ap.add_argument("--async-apply", default="no")
    ap.add_argument("--warm-skip", default="")
    ap.add_argument("--settle", type=int, default=10)
    ap.add_argument("--poll-timeout", type=int, default=120)
    a = ap.parse_args()
    CLI = a.cli
    {"plan": cmd_plan, "prepare": cmd_prepare, "migrate": cmd_migrate}[a.cmd](a)


if __name__ == "__main__":
    main()
