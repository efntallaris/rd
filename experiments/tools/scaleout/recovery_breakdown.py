#!/usr/bin/env python3
"""Recovery-time breakdown of one 3 -> 6 crash run: what happens after the kill, what runs in
parallel, and how long the keys of each slot class are unavailable to the probe client.

usage: recovery_breakdown.py <scenario> <lincheck run dir> <ycsb run dir> <out.png>
  <scenario>  S1..S9, C1..C16 (which groups lose their leader: see DEAD below)
Prints the phase table (seconds after the first kill) and writes the timeline figure.

Server side: every replica's log, times relative to the first kill (run.log t_kill).
Client side: the linearizability probe (history.jsonl). Its clock is the JVM's nanoTime from
client start, so it is anchored to the kill by the first operation on an affected key that
hangs (> 600 ms): an operation in flight on a dead leader is the first to stall. Per key, the
outage is the time from the kill to the first successful completion after it.
Key classes, per pair, from where its slot was at the kill: moved (round committed and
NARROWed), in flight (TXN_START applied, not NARROWed), on donor (not started)."""
import sys, re, glob, os, json, datetime, binascii, collections, statistics
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.lines import Line2D

sc, d, y, out = sys.argv[1], sys.argv[2].rstrip("/"), sys.argv[3].rstrip("/"), sys.argv[4]
PAIRS = {"A": ("sg1", "sg4", 0, 2729), "B": ("sg2", "sg5", 5461, 8190), "C": ("sg3", "sg6", 10922, 13651)}
PAIR_OF = {g: p for p, (dg, rg, _, _) in PAIRS.items() for g in (dg, rg)}
# groups whose LEADER the scenario kills (followers: S3 sg1 on redis1, S4 sg4 on redis4)
# (C1-C16: ansible/experiments/custom_scaleout_3to6/scenarios.env; C11's second kill is the
# new sg4 leader, C12/C14 a follower killed in turn / frozen, C15/C16 kill nothing)
# S6 / S7 are host crashes: all three instances of redis3 / redis0 die at once. Only one of the
# three is a leader (sg4 for S6, sg1 for S7); the other two are followers of the pairs B and C.
DEAD = {"S1": ["sg4"], "S2": ["sg1"], "S8": ["sg1"], "S5": ["sg4", "sg1"], "S9": ["sg1", "sg5"], "S3": [], "S4": [],
        "S6": ["sg4"], "S7": ["sg1"],
        "C1": ["sg1"], "C2": ["sg1"], "C3": ["sg1"], "C4": ["sg4"], "C5": ["sg4"], "C6": ["sg4"], "C7": ["sg4"],
        "C8": [], "C9": [], "C10": ["sg4", "sg1"], "C11": ["sg4"], "C12": [], "C13": ["sg4"], "C14": [],
        "C15": [], "C16": [], "C17": ["sg4"],
        "S10": ["sg4"], "S11": ["sg4"], "S12": ["sg4"]}[sc]   # S10-S12 = C5, C7, C6
FOLLOWER = {"S3": ("sg1", "redis1"), "S4": ("sg4", "redis4"), "C8": ("sg4", "redis4"), "C9": ("sg1", "redis1"),
            "C12": ("sg4", "redis4"), "C14": ("sg4", "redis4")}.get(sc)
LOGT = re.compile(r"\d+:\w (\d\d \w\w\w \d{4} \d\d:\d\d:\d\d\.\d+) [*#] (.*)")
ts = lambda s: datetime.datetime.strptime(s, "%d %b %Y %H:%M:%S.%f").timestamp()

kills = sorted((float(m.group(2)), m.group(1)) for l in open(d + "/run.log", errors="ignore")
               for m in [re.search(r"(?:KILLED|STOPPED|PARTITIONED) (\S+) .*t_(?:kill|stop|part)=([\d.]+)", l)] if m)
if not kills:
    sys.exit("%s: no fault in %s/run.log (nothing to break down)" % (sc, d))
# what each fault was, for the figures: kill, or a partition / freeze with its length
faults = []
for l in open(d + "/run.log", errors="ignore"):
    m = re.search(r"\] (KILLED|STOPPED|PARTITIONED) (\S+)(?: <-> (\S+))?.*t_\w+=([\d.]+)", l)
    if m:
        kind = {"KILLED": "kill", "STOPPED": "freeze", "PARTITIONED": "partition"}[m.group(1)]
        dur = re.search(r" for ([\d.]+)s ", l)
        sgm = re.search(r"\((sg\d)\)", l)
        faults.append({"kind": kind, "host": m.group(2), "peer": m.group(3), "sg": sgm.group(1) if sgm else None,
                       "secs": float(dur.group(1)) if dur else None, "t": float(m.group(4))})
    m = re.search(r"\] RESTARTED (\S+) (\S+) .*t_restart=([\d.]+)", l)
    if m:   # a killed instance relaunched (fault_steps.sh restart)
        faults.append({"kind": "restart", "host": m.group(1), "peer": None, "secs": None,
                       "t": float(m.group(3))})
k0 = kills[0][0]
# ... and back in sync with the leader (fault_steps.sh waitsync). Older runs log the injector's
# wall-clock time only (UTC, HH:MM:SS) and only in playbook.log.
for f in (d + "/run.log", d + "/playbook.log"):
    if not os.path.exists(f):
        continue
    for l in open(f, errors="ignore"):
        m = re.search(r"\[crash_inject (\d\d):(\d\d):(\d\d)\] IN SYNC \S+ \S+ = (\S+) .*?(?:t_sync=([\d.]+))?$", l.rstrip())
        if not m:
            continue
        if m.group(5):
            t = float(m.group(5))
        else:
            day = datetime.datetime.fromtimestamp(k0, datetime.timezone.utc).replace(hour=0, minute=0, second=0,
                                                                                     microsecond=0)
            t = (day + datetime.timedelta(hours=int(m.group(1)), minutes=int(m.group(2)),
                                          seconds=int(m.group(3)))).timestamp()
            if t < k0 - 3600:
                t += 86400
        if not any(x["kind"] == "insync" and x["host"] == m.group(4) and abs(x["t"] - t) < 2 for x in faults):
            faults.append({"kind": "insync", "host": m.group(4), "peer": None, "secs": None, "t": t})
ev = []                                           # (t, host, sg, text)
for f in glob.glob(d + "/logs/redis*/*.log"):
    host, sg = os.path.basename(f)[:-4].split("_")
    for l in open(f, errors="ignore"):
        m = LOGT.match(l)
        if m:
            ev.append((ts(m.group(1)) - k0, host, sg, m.group(2)))
ev.sort()


def firstm(pat, sgs=None, after=-1e9, before=1e9, host=None):
    """As first(), but (t, match) of the first matching line, or None."""
    r = re.compile(pat)
    for t, h, g, x in ev:
        if after <= t < before and (sgs is None or g in sgs) and (host is None or h == host):
            m = r.search(x)
            if m:
                return t, m
    return None


def first(pat, sgs=None, after=-1e9, before=1e9, host=None):
    r = re.compile(pat)
    for t, h, g, x in ev:
        if after <= t < before and (sgs is None or g in sgs) and (host is None or h == host) and r.search(x):
            return t
    return None


# ---- transfers on the donors (as in downtime.py) ----------------------------------------------
tr = {}
for t, h, g, x in ev:
    mm = re.search(r"RDMA MIGRATE worker: (started id=(\d+)|id=(\d+) state=(TRANSFER|BACKPATCH)|id=(\d+) (DONE|FAILED))", x)
    if mm and g in ("sg1", "sg2", "sg3"):
        i = mm.group(2) or mm.group(3) or mm.group(5)
        k = "start" if mm.group(2) else (mm.group(4) or mm.group(6))
        tr.setdefault((g, h, i), {"sg": g, "id": i, "host": h})[k] = t

# ---- phases ------------------------------------------------------------------------------------
bars = []    # (lane, start, end, category, label)
marks = []   # (lane, t, marker, label)
L_CLIENT, L_RAFT, L_CTRL, L_COPY, L_REPL, L_IDX, L_CHAIN, L_BG = (
    "client", "raft", "ctrl", "copy", "repl", "idx", "chain", "bg")
rows = []    # phase table: (name, start, end)

LEADER_HOST = {"sg1": "redis0", "sg2": "redis1", "sg3": "redis2", "sg4": "redis3", "sg5": "redis4", "sg6": "redis5"}
kill_of = {g: next((k - k0 for k, h in kills if h == LEADER_HOST[g]), 0.0) for g in DEAD}
for g in sorted(DEAD, key=lambda g: kill_of[g]):
    tl = first(r"now a leader", [g], after=0)
    tp = first(r"Election starting", [g], after=0)
    if tl is not None:
        bars.append((L_RAFT, kill_of[g], tl, "nolead", "%s: no leader until new leader" % g))
        rows.append(("%s: failure detection (election timeout) -> pre-candidate" % g, kill_of[g], tp))
        rows.append(("%s: election (pre-vote + vote) -> new leader" % g, tp, tl))

recip_dead = [g for g in DEAD if g in ("sg4", "sg5", "sg6")]
donor_dead = [g for g in DEAD if g in ("sg1", "sg2", "sg3")]

# recipient-side recovery, step 1: the new leader's index update of the data it already holds
# as a follower (durable). After it the leader serves; everything below runs while serving.
for g in recip_dead:
    a = first(r"recipient-recover: .*held-block merge staged", [g], after=0)
    b = first(r"recipient-recover: .*merge queue drained", [g], after=a or 0)
    if a is not None and b is not None:
        tl = first(r"now a leader", [g], after=0)
        bars.append((L_IDX, tl, b, "idx", "%s: index update of durable data, then serve" % g))
        rows.append(("%s new leader: INDEX UPDATE of durable data (then serves)" % g, tl, b))
# recipient-side recovery, step 2 (while serving): check what is durable for the in-flight round
for g in recip_dead:
    a = first(r"MGN-RECOVER: role=recipient", [g], after=0)
    # 2026-11-06: end at the "told donor" for the range that was in flight (the one the leader
    # probed its peers for), not the first one: a closed range is re-homed 1 ms after promotion,
    # which drew the 0.5 s peer probe as nothing.
    b = None
    for t_, h_, g_, x_ in ev:
        if g_ == g and t_ >= (a or 0):
            m_ = re.search(r"range=(\d+)-(\d+) — I hold \d+/\d+ slots; probing", x_)
            if m_:
                b = first(r"told donor .* for slots=%s-%s" % m_.groups(), [g], after=t_)
                break
    if b is None:
        b = first(r"told donor .* the recipient leader", [g], after=a or 0)
    if a is not None and b is not None:
        bars.append((L_CTRL, a, b, "ctrl", "%s: probe peers for durable state" % g))
        rows.append(("%s new leader: probe peers + donor for durable state" % g, a, b))

# donor-side: (re-)drive the round (re-home to the new recipient leader / resume after a donor failover)
for t, h, g, x in ev:
    own = t > 0 and re.search(r"donor re-home \((transfer|backpatch)\): recipient leader gone", x)
    if own:   # 2026-11-06: the donor re-ships by itself once told where the new leader is
        start = t
        for t_, h_, g_, x_ in ev:   # from the re-home of the in-flight range: the last one before t
            if g_ in recip_dead and t_ <= t and "told donor" in x_:
                start = t_
        bars.append((L_CTRL, start, t, "ctrl", "%s: re-home -> re-ship" % g))
        rows.append(("%s: donor decides the roll-forward (re-home / resume plan)" % g, start, t))
        continue
    if t > 0 and "MGN-RECOVER: role=donor" in x:
        e = first(r"RESUMED — re-dispatched|resume-plan:", [g], after=t)
        start = t
        prev = first(r"told donor .* the recipient leader", recip_dead, after=0) if recip_dead else None
        if prev is not None and prev < t:
            start = prev
        bars.append((L_CTRL, start, e if e else t + 0.01, "ctrl", "%s: re-home + resume plan" % g))
        rows.append(("%s: donor decides the roll-forward (re-home / resume plan)" % g, start, e))

# the re-shipped round(s): ids >= 8e17
for (g, h, i), e in sorted(tr.items(), key=lambda kv: kv[1].get("start", 0)):
    if int(i) < 800000000000000000 or e.get("start", -1) < 0:
        continue
    if "TRANSFER" in e and "BACKPATCH" in e:
        bars.append((L_COPY, e["TRANSFER"], e["BACKPATCH"], "copy", "re-ship copy %s" % PAIR_OF[g]))
        rows.append(("%s re-ship: setup" % g, e["start"], e["TRANSFER"]))
        rows.append(("%s re-ship: copy donor -> recipient leader" % g, e["TRANSFER"], e["BACKPATCH"]))
        rg = PAIRS[PAIR_OF[g]][1]
        fp = first(r"forward FIRST-POST", [rg], after=e["TRANSFER"] - 0.05)
        wr = first(r"wrote \d+ bytes .*leader → F1", [rg], after=fp or e["TRANSFER"])
        du = first(r"RECP_DURABLE logged", [rg], after=wr or e["TRANSFER"])
        dn = e.get("DONE")
        if fp is not None and wr is not None:
            bars.append((L_REPL, fp, wr, "repl", "replicate to F1"))
            rows.append(("%s re-ship: replication leader -> F1" % rg, fp, wr))
        # Index update = the recipient leader's merge of this round. A slot is merged once its
        # block has reached the first follower (the forward gate), so it starts with the forward
        # and ends at merge_done ("wall_since_first" counts from the leader's first merge ever,
        # so it is not used for the start).
        md = first(r"bg-merge: session mig_id=%s merge_done" % i, [rg], after=e["TRANSFER"])
        if fp is not None and md is not None:
            bars.append((L_IDX, fp, md, "idx", "index update (leader merge)"))
            rows.append(("%s re-ship: INDEX UPDATE (leader merge, gated on F1 holding each block)" % rg, fp, md))
        if du is not None and dn is not None:
            bars.append((L_REPL, du, dn, "commit", "commit"))
            rows.append(("%s re-ship: durable -> TXN_DONE + NARROW" % g, du, dn))
    elif "FAILED" in e:
        marks.append((L_COPY, e["FAILED"], "x", "re-ship failed"))

# chain re-establishment by a new recipient leader (connect to the dead peer times out)
for g in recip_dead:
    tl = first(r"now a leader", [g], after=0)
    a = first(r"spawned chainEstablishThread", [g], after=tl or 0)
    b = first(r"INIT-QP failed", [g], after=a or 0)
    c = first(r"RDMA chain established", [g], after=b or a or 0)
    if a is not None and c is not None:
        bars.append((L_CHAIN, a, c, "chain", "%s: rebuild chain (dead peer times out)" % g))
        rows.append(("%s new leader: rebuild replication chain (dead-peer connect timeout)" % g, a, c))

# follower crash on the recipient: the leader's pipelined forward stalls until RDMA gives up
if FOLLOWER and FOLLOWER[0] in ("sg4", "sg5", "sg6"):
    g = FOLLOWER[0]
    ff = first(r"pipelined forward failed", [g], after=0)
    # The stall starts at the FIRST-POST of the forward that was in flight when the peer died,
    # i.e. the last one at or before the failure. The old "last FIRST-POST before +0.05" picked
    # the PREVIOUS round's forward (which had already completed) whenever the kill landed just
    # after a round boundary: S4 2026-11-04 drew -0.60 -> +7.25 = 7.85 s for a 7.18 s stall.
    fp = None
    if ff is not None:
        for t, h, gg, x in ev:
            if gg == g and t <= ff and "forward FIRST-POST" in x:
                fp = t
    rf = first(r"RE-FORM", [g], after=ff or 0)
    if fp is not None and ff is not None:
        bars.append((L_REPL, fp, ff, "repl", "forward to dead F1 (stalls)"))
        rows.append(("%s leader: forward to the dead F1 until RDMA gives up" % g, fp, ff))
    if ff is not None and rf is not None:
        rows.append(("%s leader: chain RE-FORM around the dead follower" % g, ff, rf))
    wr = first(r"wrote \d+ bytes .*leader → F1", [g], after=rf or 0)
    if rf is not None and wr is not None:
        bars.append((L_REPL, rf, wr, "repl", "re-forward to F2"))
        rows.append(("%s leader: re-forward the round to the surviving follower" % g, rf, wr))

    # 2026-11-04: the kill can also land BEFORE the forward starts, while the leader is still
    # establishing the chain for that round. Then there is no "pipelined forward failed" and no
    # "RE-FORM": the leader PINGs the dead peer twice (300 ms each), drops it at establish and
    # re-heads the chain on F2. Nothing above matches, so S4 drew an empty Recovery row and the
    # round looked like an unexplained 0.8 s gap between the copy and its replication.
    ed = first(r"treating it as dead", [g], after=0)
    eh = first(r"dropped \d+ dead follower\(s\) at establish", [g], after=ed or 0)
    ep = None
    if ed is not None:
        for t, h, gg, x in ev:
            if gg == g and t <= ed and "spawned chainEstablishThread" in x:
                ep = t          # the establish this kill interrupted (last one at or before)
    if ed is not None and eh is not None:
        a = ep if ep is not None else ed
        bars.append((L_CHAIN, a, eh, "chain", "%s: drop dead F1 at establish (2 x 300 ms PING)" % g))
        rows.append(("%s leader: dead follower found at establish, chain re-heads on F2" % g, a, eh))

# every other transfer (background): whole transfer start -> DONE
for (g, h, i), e in tr.items():
    if int(i) >= 800000000000000000 or "start" not in e:
        continue
    end = e.get("DONE", e.get("FAILED"))
    if end is None:
        end = e.get("BACKPATCH", e["start"] + 0.05)
    if end > -1.5 and e["start"] < 12:
        bars.append((L_BG, e["start"], end, "bg" + PAIR_OF[g], ""))

# ---- client: probe outage per key class --------------------------------------------------------
own = {}     # (pair, lo, hi) -> (txn_start t, narrow t)
for t, h, g, x in ev:
    m = re.search(r"MGN_TXN_START applied: payload=\"sess=\d+ slots=(\d+)-(\d+)", x)
    if m and g in ("sg1", "sg2", "sg3"):
        k = (PAIR_OF[g], int(m.group(1)), int(m.group(2)))
        own.setdefault(k, [None, None])
        if own[k][0] is None:
            own[k][0] = t
    m = re.search(r"NARROW\(donor\) applied: slots (\d+):(\d+)", x)
    if m and g in ("sg1", "sg2", "sg3"):
        k = (PAIR_OF[g], int(m.group(1)), int(m.group(2)))
        own.setdefault(k, [None, None])
        if own[k][1] is None:
            own[k][1] = t


# A pair's keys are classified at the kill that hits that pair (its first crashed group), not at
# the run's first kill: in S9 the first kill is pair B's recipient, and pair A's round in flight
# then had already finished by the time A's donor was killed.
PAIR_REF = {p: min([kill_of[g] for g in DEAD if PAIR_OF[g] == p] or [0.0]) for p in PAIRS}


def class_ref(c):
    """The kill a key class's outage counts from: keys still on the donor from the donor
    leader's kill, moved keys from the recipient leader's (C10/S5 kill both, 0.36 s apart:
    timing the donor's keys from the recipient's kill measured the wrong gap)."""
    p, cls = c
    if p is None:
        return 0.0
    dg, rg = PAIRS[p][0], PAIRS[p][1]
    if cls == "on donor" and dg in DEAD:
        return kill_of[dg]
    if cls == "moved" and rg in DEAD:
        return kill_of[rg]
    return PAIR_REF[p]


def klass(s):
    for p, (dg, rg, lo, hi) in PAIRS.items():
        if lo <= s <= hi:
            ref = PAIR_REF[p]
            for (pp, a, b), (st, nw) in own.items():
                if pp == p and a <= s <= b:
                    if nw is not None and nw < ref:
                        return p, "moved"
                    if st is not None and st < ref:
                        return p, "in flight"
            return p, "on donor"
    return None, None


affected = set()
for g in DEAD:
    p = PAIR_OF[g]
    if g == PAIRS[p][1]:
        affected |= {(p, "moved"), (p, "in flight")}
    else:
        affected |= {(p, "on donor"), (p, "in flight")}
slot = lambda k: binascii.crc_hqx(k.encode(), 0) % 16384
hist = collections.defaultdict(list)
for l in open(d + "/history.jsonl", errors="ignore"):
    try:
        r = json.loads(l)
    except ValueError:
        continue
    if r.get("phase") == "run":
        hist[r["key"]].append(r)
keycls = {k: klass(slot(k)) for k in hist}
# Anchor the probe clock on the kill that stalls one key class: the kill lies between the last
# successful completion on those keys and the first of their operations that fails or hangs
# (3-10 ms apart in these runs). Use the class only that kill affects: a dead recipient leader's
# moved keys (else its in-flight ones), a dead donor leader's not-yet-migrated keys.
anchor = None    # probe-clock ns of the FIRST kill
for g in sorted(DEAD, key=lambda g: kill_of[g]):
    p = PAIR_OF[g]
    want = [(p, "moved"), (p, "in flight")] if g == PAIRS[p][1] else [(p, "on donor")]
    for c in want:
        ops = [r for k, rs in hist.items() if keycls[k] == c for r in rs]
        bad = [r for r in ops if r["status"] != "ok" or r["ret"] - r["inv"] > 600e6]
        if not bad:
            continue
        F = min(r["inv"] for r in bad)
        anchor = F - kill_of[g] * 1e9
        break
    if anchor is not None:
        break
client = {}
if anchor is not None:
    # Per key, the gap between successful operations that spans the kill hitting its pair: the
    # key is unavailable from that kill until the end of the gap (outage = end - kill).
    per = collections.defaultdict(list)     # class -> [(outage s, gap start s, gap end s)]
    for k, rs in hist.items():
        ref = class_ref(keycls[k])
        oks = sorted((r["ret"] - anchor) / 1e9 for r in rs if r["status"] == "ok")
        span = [(b - ref, a, b) for a, b in zip(oks, oks[1:]) if a <= ref < b]
        if span:
            per[keycls[k]].append(span[0])
    for c, v in per.items():
        client[c] = (statistics.median(x[0] for x in v), max(x[0] for x in v), len(v),
                     statistics.median(x[1] for x in v), statistics.median(x[2] for x in v), max(x[2] for x in v))
    for c in sorted(affected):
        if c in client:
            med, mx, n, s_med, e_med, e_max = client[c]
            bars.append((L_CLIENT, s_med, e_med, "client",
                         "pair %s, keys %s: %d keys, outage median %.2f s, max %.2f s" % (c[0], c[1], n, med, mx)))
            rows.append(("CLIENT pair %s keys %s (%d keys): outage median (typical %+.2f -> %+.2f)" % (c[0], c[1], n, s_med, e_med), 0.0, med))
            rows.append(("CLIENT pair %s keys %s (%d keys): outage max" % (c[0], c[1], n), 0.0, mx))
            # when the class is served again: its median key's next success (the earliest single
            # key was too fragile: one in-flight operation completing at the kill gave ~0 s)
            marks.append((L_CLIENT, e_med, "|", ""))
            rows.append(("CLIENT pair %s keys %s: served again (median key)" % (c[0], c[1]), 0.0, e_med))
    unaff = [v for c, v in client.items() if c not in affected and c[0] is not None]
    if unaff:
        rows.append(("CLIENT unaffected keys: outage median (probe threads stuck on dead keys)", 0.0,
                     statistics.median(v[0] for v in unaff)))
        rows.append(("CLIENT unaffected keys: outage max", 0.0, max(v[1] for v in unaff)))

# ---- YCSB total throughput per second (as in downtime.py) ------------------------------------------
tot = collections.Counter(); seen = set()
for f in glob.glob(y + "/ycsb/*/tmp/ycsb_output_ycsb*"):
    if "load" in os.path.basename(f):
        continue
    for l in open(f, errors="ignore"):
        m = re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d):\d+ (\d+) sec: \d+ operations; ([\d.]+) current", l)
        if m and (f, m.group(2)) not in seen:
            seen.add((f, m.group(2)))
            t = datetime.datetime.strptime(m.group(1) + " +0000", "%Y-%m-%d %H:%M:%S %z").timestamp()
            tot[int((t - k0) // 1)] += float(m.group(3))

# ---- table ---------------------------------------------------------------------------------------
print("== %s  kills: %s" % (sc, ", ".join("%s at %+.2f" % (h, k - k0) for k, h in kills)))
for name, a, b in rows:
    if a is None or b is None:
        print("  %-78s   n/a" % name)
    else:
        print("  %-78s %+6.2f -> %+6.2f  = %5.2f s" % (name, a, b, b - a))

# every round of every pair: copy (donor), replication to F1 and index update (recipient leader)
rounds_out = []
for (g, h, i), e in sorted(tr.items(), key=lambda kv: kv[1].get("start", 0)):
    if "TRANSFER" not in e:
        # a round the crash cut during its setup (S8: the donor leader died after the block
        # registration, before the transfer was announced): no byte moved, but the round had
        # started on the donor
        if (int(i) < 800000000000000000 and "DONE" not in e and e.get("start") is not None
                and e["start"] < 0 and any(0 <= k - k0 < 5 for k, _h in kills)):
            rounds_out.append({"pair": PAIR_OF[g], "reship": False, "ok": False, "setup": True,
                               "copy": [e["start"], None], "repl": [None, None], "idx": [None, None],
                               "async": [None, None], "async_wait": None, "done": None,
                               "adopted": None, "held": None})
        continue
    rg = PAIRS[PAIR_OF[g]][1]
    cend = e.get("BACKPATCH", e.get("FAILED"))
    # (only before the donor's next transfer: a re-ship carries the same sess= in its payload)
    nxt = min([e2["start"] for (g2, h2, i2), e2 in tr.items() if g2 == g and e2.get("start", 0) > e.get("start", 0)]
              or [1e9])
    # replication starts when the chain is up: up to the round's end, not 0.5 s after the copy (C7 on
    # 10M keys: a new leader's chain waited 1 s on the dead old leader, round 2 showed no replication)
    fp = first(r"forward FIRST-POST", [rg], after=e["TRANSFER"] - 0.05, before=min(e.get("DONE", nxt), nxt))
    wr = first(r"wrote \d+ bytes .*leader → F1", [rg], after=fp) if fp is not None else None
    md = first(r"bg-merge: session mig_id=%s merge_done" % i, [rg], after=e["TRANSFER"]) if "BACKPATCH" in e else None
    # The leader's background merge runs alongside the forward, gated per BLOCK (not per announced
    # chunk), so it starts when the forward starts: merge_done's own "wall_since_first" puts the
    # first merge at the forward's first post, both with 57 chunks per round (merge 505 ms over a
    # 507 ms copy) and with one chunk per round (496 ms over a 499 ms forward). So fp is the start.
    # Do NOT use the first "PERCHUNK INDEX ... done" as the start: that is the first chunk's
    # COMPLETION, which is ~10 ms late when a round has many chunks and collapses the bar to zero
    # width when a round is announced as one chunk.
    # done: the donor's worker finished, or the recipient committed the session durable (C3: the
    # donor died after that, before its own DONE; the round is not lost, nothing is sent again)
    dur = first(r'MGN_RECP_DURABLE applied: payload="sess=%s ' % i, [rg], after=e["TRANSFER"], before=nxt)
    # A round whose copy finished before the crash and that a NEW recipient leader then committed
    # durable from a follower's copy (MGN_RECP_DURABLE ... adopted=1 under its recovery session,
    # S12/C6: F1 held the round, F2 was elected): not lost, nothing sent again. Its last steps are
    # F1's forward to the new leader ("CHAIN recipe ... wrote N bytes to X (wired link, M ms)")
    # and the index update of the round on the new leader.
    adopted = None
    # (event times here are already relative to the first kill: the crash is t = 0)
    if not ("DONE" in e or dur is not None) and cend is not None and cend < 0:
        # up to the donor's next transfer that SENT data (a resume that skipped every slot did not)
        nxt_sent = min([e2["TRANSFER"] for (g2, h2, i2), e2 in tr.items()
                        if g2 == g and "TRANSFER" in e2 and e2["TRANSFER"] > e["TRANSFER"]] or [1e9])
        dm = firstm(r'MGN_RECP_DURABLE applied: payload="sess=7\d+ slots=\d+-\d+ .*adopted=1', [rg],
                    after=0, before=nxt_sent)
        if dm is not None:
            dur = dm[0]
            rw = firstm(r"CHAIN recipe: sess=\S+ attempt=\d+ wrote \d+ bytes to \S+ \(wired link, (\d+) ms\)",
                        [rg], after=0, before=dur + 0.01)
            if rw is not None:
                we = rw[0]; ws = we - int(rw[1].group(1)) / 1000.0
                ie = max([t for t, h_, g_, x in ev if g_ == rg and ws <= t <= dur + 0.1 and "INDEX-UPDATE done" in x]
                         or [we])
                adopted = {"repl": [ws, we], "idx": [ws, ie]}
    # Async chain replication: the rest of the chain after the majority hop -- F1 forwarding the
    # round to F2. From F1 having the whole batch ("CHAIN-FORWARDED: sess=S ... tail=0") to F2
    # having it ("... tail=1"); when F1 sent it through a recipe (a re-form / a held forward),
    # its own "wrote N bytes to X (wired link, M ms)" window is the data transfer.
    asy = [None, None]
    asy_wait = None
    cs = firstm(r"mig_id=%s -> chain_sess=(\d+)" % i, [rg], after=e["TRANSFER"] - 0.2, before=nxt)
    if cs is not None:
        S = cs[1].group(1)
        a0 = first(r"RDMA CHAIN-FORWARDED: sess=%s .*tail=0" % S, [rg], after=e["TRANSFER"])
        b0 = first(r"RDMA CHAIN-FORWARDED: sess=%s .*tail=1" % S, [rg], after=a0 if a0 is not None else e["TRANSFER"])
        rc_ = firstm(r"CHAIN recipe: sess=%s attempt=\d+ wrote \d+ bytes to \S+ \(wired link, (\d+) ms\)" % S,
                     [rg], after=e["TRANSFER"])
        if rc_ is not None and (b0 is None or rc_[0] <= b0 + 0.05):
            asy = [rc_[0] - int(rc_[1].group(1)) / 1000.0, rc_[0]]
        elif a0 is not None and b0 is not None:
            asy = [a0, b0]
        # F1 had the whole round (a0) but its forward started only later: the hop waited (S12: a
        # test hook held F1's forward 6 s, across the crash)
        if a0 is not None and asy[0] is not None and asy[0] - a0 > 0.1:
            asy_wait = [a0, asy[0]]
    if adopted is not None:
        adopted.pop("repl", None)     # F1 -> F2 is the async chain replication row now
    # A round that landed on the recipient leader before the crash but whose chain replication to
    # F1 had not started when the leader died (S10/C5: a test hook held the forward): the leader
    # was holding the round, waiting to forward it -- the phase the crash cut.
    held = None
    if not ("DONE" in e or dur is not None) and cend is not None and cend < 0 and fp is None:
        hk = first(r"TEST HOOK before_forward", [rg], after=e["TRANSFER"], before=0)
        held = [hk if hk is not None else cend, 0.0]
    rounds_out.append({"pair": PAIR_OF[g], "reship": int(i) >= 800000000000000000,
                       "ok": "DONE" in e or dur is not None,
                       "copy": [e["TRANSFER"], cend], "repl": [fp, wr], "idx": [fp, md],
                       "async": asy, "async_wait": asy_wait, "done": e.get("DONE", dur), "adopted": adopted,
                       "held": held})

# phases as JSON next to the figure (for compact paper figures: paper_recovery.py)
# What the refetch found, per pair: where the interrupted round was after the crash.
#  self    -- the new recipient leader holds it (it was a follower that had the round)
#  peer    -- another live replica holds it: the new leader fetches it from that holder
#  none    -- no live replica holds it: the donor sends it again
#  durable -- already committed on the recipient group: nothing is sent again
refetch = {}
for P, (dg, rg, _lo, _hi) in PAIRS.items():
    res = None
    m = firstm(r"recipient-recover: .*I (hold|lack) the range, (\d+)/(\d+) peers hold all of it", [rg], after=0)
    if m is not None:
        holder = None
        if m[1].group(1) == "hold":
            kind = "self"
        elif int(m[1].group(2)) > 0:
            kind = "peer"
            hm = firstm(r"asked holder (redis\d)", [rg], after=0)
            holder = hm[1].group(1) if hm else None
        else:
            kind = "none"
        nl = next((h for t, h, g, x in ev if g == rg and t > 0 and "now a leader" in x), None)
        res = {"kind": kind, "t": m[0], "leader": nl, "holder": holder}
    rp = firstm(r"resume-plan: .*recipient reports durable=(\d+) pending=(\d+) missing=(\d+)", [dg], after=0)
    if rp is not None:
        du, pe, mi = (int(rp[1].group(k)) for k in (1, 2, 3))
        if res is None:
            res = {"kind": "durable" if mi == 0 and pe == 0 and du > 0 else ("none" if du == 0 else "partial"),
                   "t": rp[0], "leader": None, "holder": None}
        res.update({"durable": du, "missing": mi})
    if res is not None:
        refetch[P] = res

json.dump({"scenario": sc, "kills": [[h, k - k0] for k, h in kills], "refetch": refetch,
           "faults": [dict(f, t=f["t"] - k0) for f in sorted(faults, key=lambda f: f["t"])],
           # every leader election of every group: [sg, host, t] (a kill of the current leader is a
           # no-leader interval until the next one, also for a second kill, C11)
           "leaders": [[g, h, t] for t, h, g, x in ev if "now a leader" in x],
           "rows": [[name, a, b] for name, a, b in rows],
           "ycsb_kops": [[sec + 0.5, tot[sec] / 1e3] for sec in sorted(tot)],   # 1-s samples, at mid-second
           # every donor transfer (incl. re-ships): pair, start, end (DONE / FAILED / last state seen)
           "rounds": rounds_out,
           "transfers": [[PAIR_OF[g], e["start"], e.get("DONE", e.get("FAILED", e.get("BACKPATCH", e["start"])))]
                         for (g, h, i), e in sorted(tr.items(), key=lambda kv: kv[1].get("start", 0)) if "start" in e]},
          open(os.path.splitext(out)[0] + ".json", "w"), indent=1)

# ---- figure --------------------------------------------------------------------------------------
INK, MUTED, GRID, KILL = "#0b0b0b", "#52514e", "#e4e3df", "#d03b3b"
COL = {"client": "#6250d6", "nolead": "#eda100", "ctrl": "#1baf7a", "copy": "#2a78d6",
       "repl": "#eb6834", "commit": "#9a3f17", "chain": "#e87ba4", "idx": "#008300",
       "bgA": "#c9c8c2", "bgB": "#b3b2ab", "bgC": "#9d9c95"}
lanes = [(L_CLIENT, "Client (probe):\nkeys unavailable"), (L_RAFT, "Raft: group\nwithout leader"),
         (L_CTRL, "Recovery control\n(probe, re-home, plan)"), (L_COPY, "Re-ship: copy\ndonor -> recipient"),
         (L_REPL, "Replication to F1\n+ commit"), (L_IDX, "Index update\n(leader merge)"), (L_CHAIN, "Chain rebuild /\nre-form around dead peer"),
         (L_BG, "Other transfers\n(all pairs)")]
used = {b[0] for b in bars} | {m[0] for m in marks}
lanes = [l for l in lanes if l[0] in used or l[0] == L_BG]
ypos = {l: len(lanes) - 1 - i for i, (l, _) in enumerate(lanes)}
x0, x1 = -1.0, max([b[2] for b in bars if b[0] != L_BG and b[2] is not None] + [3.0]) + 1.5
fig, (ax, ay) = plt.subplots(2, 1, figsize=(12, 0.62 * len(lanes) + 3.2), sharex=True,
                             gridspec_kw={"height_ratios": [0.62 * len(lanes) + 0.8, 1.6]})
stack = collections.Counter()
for lane, a, b, cat, lab in bars:
    if a is None or b is None:
        continue
    yy = ypos[lane]
    if lane in (L_CLIENT, L_RAFT):
        off = stack[lane] * 0.3; stack[lane] += 1
        ax.barh(yy + 0.3 - off, b - a, left=a, height=0.24, color=COL[cat], edgecolor="white", linewidth=1)
        txt = lab if lane == L_CLIENT else "%s  %.2f s" % (lab, b - a)
        ax.text(b + 0.04, yy + 0.3 - off, txt, va="center", fontsize=7.5, color=INK)
        continue
    h = 0.4 if lane != L_BG else 0.34
    ax.barh(yy, max(b - a, 0.012), left=a, height=h, color=COL[cat], edgecolor="white", linewidth=1)
    if lab and lane != L_BG:
        # label outside the bar with its duration, alternating above / below within a lane
        k = stack[lane]; stack[lane] += 1
        ax.text(a, yy + (0.27 if k % 2 == 0 else -0.27), "%s  %.2f s" % (lab, b - a),
                va="center", ha="left", fontsize=7, color=INK)
for lane, t, mk, lab in marks:
    if mk == "|":    # first key of a class served again
        ax.plot([t, t], [ypos[lane] - 0.45, ypos[lane] + 0.45], color=INK, lw=1.5)
        ax.text(t + 0.03, ypos[lane] - 0.42, "first served %+.2f s" % t, fontsize=7, color=INK)
        continue
    ax.plot([t], [ypos[lane]], marker=mk, color=KILL, ms=8, mew=2)
    ax.text(t + 0.05, ypos[lane] + 0.28, lab, fontsize=7, color=KILL)
for i, (k, h) in enumerate(kills):
    for a_ in (ax, ay):
        a_.axvline(k - k0, color=KILL, ls="--", lw=1.2)
    ax.text(k - k0 + 0.03, len(lanes) - 0.45 + 0.22 * i, "kill %s" % h, color=KILL, fontsize=8, va="bottom")
for i, g in enumerate(sorted(DEAD, key=lambda g: kill_of[g])):
    tl = first(r"now a leader", [g], after=0)
    if tl is not None:
        ax.plot([tl], [ypos[L_RAFT] + 0.3 - 0.3 * i], marker="v", color=INK, ms=6)
ax.set_yticks([ypos[l] for l, _ in lanes]); ax.set_yticklabels([n for _, n in lanes], fontsize=8, color=INK)
ax.set_ylim(-0.6, len(lanes) - 0.1)
ax.grid(axis="x", color=GRID, lw=0.8); ax.set_axisbelow(True)
for s in ("top", "right"):
    ax.spines[s].set_visible(False)
ax.set_title("%s: recovery timeline (0 = first kill)" % {"S1": "S1 recipient leader (sg4)", "S2": "S2 donor leader (sg1)",
             "S3": "S3 donor follower (sg1 on redis1)", "S4": "S4 recipient follower (sg4 on redis4)",
             "S5": "S5 donor + recipient leader of pair A", "S9": "S9 donor leader A (sg1) + recipient leader B (sg5)",
             "S8": "S8 donor leader (sg1), between registration and DONE-SLOTS-INIT"}.get(sc, sc),
             loc="left", fontsize=11, color=INK)
secs = [s for s in sorted(tot) if x0 - 1 <= s <= x1]
# one YCSB status line per second: plot each 1-s sample at the middle of its second
ay.plot([s + 0.5 for s in secs], [tot[s] / 1e3 for s in secs], color=INK, lw=2, marker="o", ms=4)
ay.set_ylabel("YCSB throughput\n(kops/s)", fontsize=8, color=INK); ay.set_ylim(0, 340)
ay.grid(axis="y", color=GRID, lw=0.8); ay.set_axisbelow(True)
for s in ("top", "right"):
    ay.spines[s].set_visible(False)
ay.set_xlabel("seconds after the first kill (YCSB: total throughput, one sample per second)", fontsize=9, color=INK)
ay.set_xlim(x0, x1)
leg = [Patch(color=COL[c], label=n) for c, n in (("client", "client outage (probe keys)"), ("nolead", "no leader"),
       ("ctrl", "recovery control"), ("copy", "re-ship copy"), ("repl", "replication to F1"),
       ("commit", "commit (durable -> TXN_DONE)"), ("idx", "index update"), ("chain", "chain rebuild"), ("bgA", "other transfers A/B/C"))
       if any(b[3] == c or (c == "bgA" and b[3].startswith("bg")) for b in bars)]
leg.append(Line2D([0], [0], color=KILL, ls="--", label="kill"))
fig.legend(handles=leg, loc="lower center", ncol=5, fontsize=8, frameon=False)
fig.tight_layout(rect=(0, 0.06, 1, 1))
fig.savefig(out, dpi=150)
print("wrote", out)
