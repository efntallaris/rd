#!/usr/bin/env python3
"""Downtime of one scale-out run, and what it is made of.

usage: downtime.py <3to4|3to6> <lincheck run dir> <ycsb run dir> [--json]
  <lincheck run dir>  e.g. /tmp/lincheck/big/3to4/S2_qyes_...  (run.log, history.jsonl, logs/)
  <ycsb run dir>      e.g. /tmp/experiments/crash_s2            (ycsb/<client>/tmp/ycsb_output_<client>)

Three views of the same event:
  probe   the linearizability client (rate-limited, keys only in the migrating ranges): the longest
          stretch in which NO operation on a range completed successfully, per range. This is the
          unavailability a client of those keys sees, in milliseconds.
  ycsb    total closed-loop throughput per second: seconds below 10% / 50% of the pre-event level
          and when it is back at 90%.
  servers kill time, leader elections, and the migration's own timeline (per transfer: wait,
          setup, copy, commit; recovery actions) from the server logs.
Times are seconds after the first kill (or after the migration start for a fault-free run)."""
import sys, re, json, glob, os, datetime, binascii, collections

setup, d, y = sys.argv[1], sys.argv[2].rstrip("/"), sys.argv[3].rstrip("/")
RANGES = {"3to4": [("A sg1->sg4", 0, 1364), ("B sg2->sg4", 5461, 6825), ("C sg3->sg4", 10922, 12286)],
          "3to6": [("A sg1->sg4", 0, 2729), ("B sg2->sg5", 5461, 8190), ("C sg3->sg6", 10922, 13651)]}[setup]
DONORS = {"sg1": "A", "sg2": "B", "sg3": "C"}
LOGT = re.compile(r"\d+:\w (\d\d \w\w\w \d{4} \d\d:\d\d:\d\d\.\d+) [*#] (.*)")


def ts(s):
    return datetime.datetime.strptime(s, "%d %b %Y %H:%M:%S.%f").timestamp()   # server logs are local time


kills = [(float(m.group(2)), m.group(1)) for l in open(d + "/run.log", errors="ignore")
         for m in [re.search(r"KILLED (\S+) .*t_kill=([\d.]+)", l)] if m]

# ---- server logs -----------------------------------------------------------------------------
ev, tr = [], {}
for f in glob.glob(d + "/logs/redis*/*.log"):
    host, sg = os.path.basename(f)[:-4].split("_")
    for l in open(f, errors="ignore"):
        m = LOGT.match(l)
        if not m:
            continue
        x = m.group(2)
        if "now a leader" in x or "MGN-RECOVER: role=" in x or "re-shipped slots" in x or "resume-plan" in x \
                or "RE-FORM —" in x or "pipelined forward failed" in x or "excluding from chain" in x \
                or "abandoned after" in x or "unresponsive" in x:
            ev.append((ts(m.group(1)), sg, host, x))
        if sg in DONORS:
            mm = re.search(r"RDMA MIGRATE worker: (started id=(\d+)|id=(\d+) state=(TRANSFER|BACKPATCH)|id=(\d+) (DONE|FAILED))", x)
            if mm:
                i = mm.group(2) or mm.group(3) or mm.group(5)
                k = "start" if mm.group(2) else (mm.group(4) or mm.group(6))
                tr.setdefault((sg, host, i), {"sg": sg, "id": i})[k] = ts(m.group(1))
starts = sorted(e["start"] for e in tr.values() if "start" in e)
t0 = min(k for k, _ in kills) if kills else (starts[0] if starts else 0)
mig_end = max([e.get("DONE", 0) for e in tr.values()] + [0])

# ---- probe client ----------------------------------------------------------------------------
slot = lambda k: binascii.crc_hqx(k.encode(), 0) % 16384
ok = collections.defaultdict(list); bad = collections.Counter(); slow = collections.defaultdict(list)
hist = d + "/history.jsonl"
if os.path.exists(hist):
    for l in open(hist, errors="ignore"):
        try:
            r = json.loads(l)
        except ValueError:
            continue
        if r.get("phase") != "run":
            continue
        s = slot(r["key"])
        name = next((n for n, lo, hi in RANGES if lo <= s <= hi), None)
        if name is None:
            continue
        if r["status"] == "ok":
            ok[name].append((r["ret"], r["inv"]))
            if r["ret"] - r["inv"] > 100e6:
                slow[name].append((r["ret"] - r["inv"]) / 1e6)
        else:
            bad[(name, r["status"])] += 1
probe = {}
for name, _, _ in RANGES:
    v = sorted(ok[name])
    if len(v) < 2:
        continue
    gaps = [(v[i + 1][0] - v[i][0], v[i][0]) for i in range(len(v) - 1)]
    g, at = max(gaps)
    probe[name] = {"longest_gap_ms": g / 1e6, "ops": len(v), "slow_ops_over_100ms": len(slow[name]),
                   "slowest_op_ms": max(slow[name]) if slow[name] else 0,
                   "not_ok": {s: c for (n, s), c in bad.items() if n == name}}

# ---- YCSB ------------------------------------------------------------------------------------
tot = collections.Counter(); seen = set()
for f in glob.glob(y + "/ycsb/*/tmp/ycsb_output_ycsb*"):
    if "load" in os.path.basename(f):
        continue
    for l in open(f, errors="ignore"):
        m = re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d):\d+ (\d+) sec: \d+ operations; ([\d.]+) current", l)
        if m and (f, m.group(2)) not in seen:     # every status line is printed twice
            seen.add((f, m.group(2)))
            t = datetime.datetime.strptime(m.group(1) + " +0000", "%Y-%m-%d %H:%M:%S %z").timestamp()
            tot[int((t - t0) // 1)] += float(m.group(3))     # floor: int() would fold -1..0 and 0..1 together
pre = [tot[s] for s in range(-12, -1) if s in tot]
pre = sum(pre) / len(pre) if pre else 0
span = range(0, 60)
below10 = [s for s in span if s in tot and tot[s] < 0.10 * pre]
below50 = [s for s in span if s in tot and tot[s] < 0.50 * pre]
last_low = max([s for s in span if s in tot and tot[s] < 0.90 * pre], default=None)
low = min(span, key=lambda s: tot.get(s, 1e18))

# ---- report ----------------------------------------------------------------------------------
out = {"kills": [(round(k - t0, 2), h) for k, h in kills],
       "elections": [(round(t - t0, 2), sg, h) for t, sg, h, x in sorted(ev) if "now a leader" in x and t > t0 - 0.3],
       "probe": probe,
       "ycsb": {"pre_kops": round(pre / 1e3), "min_kops": round(tot.get(low, 0) / 1e3), "min_at_s": low,
                "seconds_below_10pct": len(below10), "seconds_below_50pct": len(below50),
                "back_to_90pct_at_s": (last_low + 1) if last_low is not None else 0},
       "migration_end_s": round(mig_end - t0, 2) if mig_end else None}
if "--json" in sys.argv:
    print(json.dumps(out)); sys.exit(0)
print("kills      :", ", ".join("%s at +%.2f" % (h, k) for k, h in out["kills"]) or "none (times are from the migration start)")
print("elections  :", "; ".join("%s -> %s at +%.2f s" % (sg, h, t) for t, sg, h in out["elections"]) or "none")
for name, p in probe.items():
    print("probe %-11s longest gap without a successful op %7.0f ms | ops slower than 100 ms: %4d (slowest %6.0f ms) | not ok: %s"
          % (name, p["longest_gap_ms"], p["slow_ops_over_100ms"], p["slowest_op_ms"], p["not_ok"] or "none"))
yv = out["ycsb"]
print("ycsb       : %dk before; minimum %dk at +%d s; %d s below 10%%, %d s below 50%%; back at 90%% at +%d s"
      % (yv["pre_kops"], yv["min_kops"], yv["min_at_s"], yv["seconds_below_10pct"], yv["seconds_below_50pct"], yv["back_to_90pct_at_s"]))
print("ycsb/s     :", " ".join("%+d:%.0fk" % (s, tot[s] / 1e3) for s in range(-2, 16) if s in tot))
print("migration  : ends at +%s s" % out["migration_end_s"])
print("transfers  :")
prev = None
for e in sorted((e for e in tr.values() if "start" in e), key=lambda e: e["start"]):
    end = e.get("DONE", e.get("FAILED"))
    f = lambda a, b: ("%5.2f" % (e[b] - e[a])) if a in e and b in e else "  -  "
    print("  %+7.2f pair %s id=%-19s wait %s setup %s copy %s commit %s  %s" % (
        e["start"] - t0, DONORS[e["sg"]], e["id"], ("%5.2f" % (e["start"] - prev)) if prev else "  -  ",
        f("start", "TRANSFER"), f("TRANSFER", "BACKPATCH"),
        ("%5.2f" % (end - e["BACKPATCH"])) if end and "BACKPATCH" in e else "  -  ",
        "DONE" if "DONE" in e else ("FAILED" if "FAILED" in e else "no end (leader died / superseded)")))
    if end:
        prev = end
print("recovery   :")
seen_x = set()
for t, sg, h, x in sorted(ev):
    if "now a leader" in x or t < t0 - 0.3:
        continue
    key = (sg, h, x[:48])
    if key in seen_x:
        continue
    seen_x.add(key)
    print("  %+7.2f %s on %s: %s" % (t - t0, sg, h, x[:150]))
