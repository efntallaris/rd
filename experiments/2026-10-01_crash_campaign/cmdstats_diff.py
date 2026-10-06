#!/usr/bin/env python3
"""Per-leader command mix and cost before vs after the migration.

Usage: cmdstats_diff.py <campaign dir> <run>
Reads logs/final_<run>/cmdstats.txt (snapshots of INFO commandstats + errorstats of the four
leaders every ~10 s, written by run_campaign.sh) and the donor logs for the migration start.
For each leader: calls/s and usec/call per command in a window before the migration and one
after it, and error replies/s by type.
"""
import collections
import datetime as dt
import os
import re
import sys

here, run = sys.argv[1], sys.argv[2]
sys.argv = [sys.argv[0], here]
exec(open(os.path.join(here, "breakdown.py")).read().split("res = {}")[0])

NAMES = {"redis0:8000": "sg1 leader", "redis1:8001": "sg2 leader", "redis2:8002": "sg3 leader",
         "redis3:8000": "sg4 leader"}
donors = logs(run, "redis[012]_sg[123]")
m0 = grep(donors, r"MIGRATE worker: id=\S+ state=PREP")[0][1]
m1 = grep(donors, r"RAFT.MGN-LOG TXN_DONE logged")[-1][1]

snaps = collections.defaultdict(list)   # inst -> [(rel_s, {cmd: (calls, usec)}, {err: count})]
cur = None
for ln in open(f"{here}/logs/final_{run}/cmdstats.txt"):
    ln = ln.strip()
    if ln.startswith("###"):
        _, ts, inst = ln.split()
        t = dt.datetime.utcfromtimestamp(float(ts))
        cur = ((t - m0).total_seconds(), {}, {})
        snaps[inst].append(cur)
    elif cur is not None and ln.startswith("cmdstat_"):
        name, rest = ln.split(":", 1)
        kv = dict(x.split("=") for x in rest.split(","))
        cur[1][name[8:]] = (int(kv["calls"]), int(kv["usec"]))
    elif cur is not None and ln.startswith("errorstat_"):
        name, rest = ln.split(":", 1)
        cur[2][name[10:]] = int(rest.split("=")[1])


def window(s, lo, hi):
    """First and last snapshot inside [lo, hi] seconds (relative to the migration start)."""
    w = [x for x in s if lo <= x[0] <= hi]
    return (w[0], w[-1]) if len(w) >= 2 else None


print(f"run {run}: migration {(m1 - m0).total_seconds():.2f} s; windows: before = -40..-5 s, after = +30..+110 s\n")
for inst, name in NAMES.items():
    s = snaps.get(inst, [])
    pre, post = window(s, -40, -5), window(s, 30, 110)
    print(f"== {name} ({inst})")
    if not post:
        print("   no snapshots")
        continue
    rows = []
    cmds = set(post[1][1]) | (set(pre[1][1]) if pre else set())
    tot = {"pre": [0, 0], "post": [0, 0]}
    for c in cmds:
        r = {}
        for tag, w in (("pre", pre), ("post", post)):
            if not w:
                r[tag] = (0.0, 0.0)
                continue
            (t0, a, _), (t1, b, _) = w
            dc = b.get(c, (0, 0))[0] - a.get(c, (0, 0))[0]
            du = b.get(c, (0, 0))[1] - a.get(c, (0, 0))[1]
            r[tag] = (dc / (t1 - t0), (du / dc) if dc else 0.0)
            tot[tag][0] += dc / (t1 - t0)
            tot[tag][1] += du / (t1 - t0)
        rows.append((c, r))
    rows.sort(key=lambda x: -(x[1]["pre"][0] * x[1]["pre"][1] + x[1]["post"][0] * x[1]["post"][1]))
    print(f"   {'command':24s} {'calls/s pre':>12s} {'us/call':>8s} {'calls/s post':>13s} {'us/call':>8s}")
    for c, r in rows[:9]:
        if max(r["pre"][0], r["post"][0]) < 5:
            continue
        print(f"   {c:24s} {r['pre'][0]:12.0f} {r['pre'][1]:8.2f} {r['post'][0]:13.0f} {r['post'][1]:8.2f}")
    print(f"   {'TOTAL':24s} {tot['pre'][0]:12.0f} {'':8s} {tot['post'][0]:13.0f}"
          f"      (cpu in commands: pre {tot['pre'][1] / 1e4:.1f}%  post {tot['post'][1] / 1e4:.1f}% of one core)")
    errs = []
    for e in set(post[1][2]) | (set(pre[1][2]) if pre else set()):
        a = ((pre[1][2].get(e, 0) - pre[0][2].get(e, 0)) / (pre[1][0] - pre[0][0])) if pre else 0.0
        b = (post[1][2].get(e, 0) - post[0][2].get(e, 0)) / (post[1][0] - post[0][0])
        if max(a, b) >= 1:
            errs.append(f"{e}: {a:.0f} -> {b:.0f}/s")
    print("   error replies/s (pre -> post):", ", ".join(sorted(errs)) or "none")
    print()
