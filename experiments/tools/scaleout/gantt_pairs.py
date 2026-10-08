#!/usr/bin/env python3
"""Gantt of a scale-out run from the server logs, one block of lanes per donor -> recipient pair.

usage: gantt_pairs.py <3to4|3to6> <lincheck run dir> <out.png> [title]
Lanes per pair: donor setup + copy, replication to the first follower, leader merge, commit.
Crashes (red dashed) and leader elections (triangles) are marked. Bars are read from every
replica's log, so they continue after a failover. Recipient-side bars are matched to rounds by
time order, which can be off by one round right after a failover."""
import sys, re, glob, os, datetime
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.lines import Line2D

setup, d, out = sys.argv[1], sys.argv[2].rstrip("/"), sys.argv[3]
title = sys.argv[4] if len(sys.argv) > 4 else os.path.basename(d)
PAIRS = {"3to4": [("A", "sg1", "sg4"), ("B", "sg2", "sg4"), ("C", "sg3", "sg4")],
         "3to6": [("A", "sg1", "sg4"), ("B", "sg2", "sg5"), ("C", "sg3", "sg6")]}[setup]
LOGT = re.compile(r"\d+:\w (\d\d \w\w\w \d{4} \d\d:\d\d:\d\d\.\d+) [*#] (.*)")
ts = lambda s: datetime.datetime.strptime(s, "%d %b %Y %H:%M:%S.%f").timestamp()

kills = [float(m.group(1)) for l in open(d + "/run.log", errors="ignore") for m in [re.search(r"t_kill=([\d.]+)", l)] if m]
tr, rec, elect = {}, {}, []
for f in glob.glob(d + "/logs/redis*/*.log"):
    host, sg = os.path.basename(f)[:-4].split("_")
    for l in open(f, errors="ignore"):
        m = LOGT.match(l)
        if not m:
            continue
        x = m.group(2)
        if "now a leader" in x:
            elect.append((ts(m.group(1)), sg))
            continue
        mm = re.search(r"RDMA MIGRATE worker: (started id=(\d+) addr=([\d.]+):(\d+)|id=(\d+) state=(TRANSFER|BACKPATCH)|id=(\d+) (DONE|FAILED))", x)
        if mm:
            i = mm.group(2) or mm.group(5) or mm.group(7)
            k = "start" if mm.group(2) else (mm.group(6) or mm.group(8))
            tr.setdefault((sg, host, i), {})[k] = ts(m.group(1))
            continue
        if "slots released earlier" in x and " 0 slots released earlier" not in x:
            rec.setdefault((sg, host), {}).setdefault("early", set()).add(round(ts(m.group(1)), 3))
        for key, pat in (("fp", "forward FIRST-POST"), ("fw", "leader → F1 ("), ("gate", "forward gate open"),
                         ("merged", "merge_done"), ("commit", ("MGN_RECP_DURABLE applied", "MGN_INDX_UPD applied"))):  # old name: saved runs
            if any(p in x for p in ((pat,) if isinstance(pat, str) else pat)):
                # a recipient group shared by several donors (3 -> 4): tell the donor from the slots when present
                rec.setdefault((sg, host), {}).setdefault(key, set()).add(round(ts(m.group(1)), 3))
t0 = min(e["start"] for e in tr.values() if "start" in e)


def pairs_of(a, b):
    """each start in a with the first end in b after it"""
    b = sorted(b); res = []
    for s in sorted(a):
        e = next((x for x in b if x >= s), None)
        if e is not None:
            res.append((s, e))
    return res


C = {"setup": "#9aa5b1", "copy": "#1f2933", "fwd": "#2f6fab", "merge": "#c96a12", "commit": "#3a8a4f"}
LANES = ["donor: setup + copy", "replicate to follower", "leader merge", "commit"]
fig, ax = plt.subplots(figsize=(13, 0.42 * len(PAIRS) * len(LANES) + 1.6))
yl, yt = [], []
shared = len({r for _, _, r in PAIRS}) == 1
for pi, (pn, dg, rg) in enumerate(PAIRS):
    base = pi * (len(LANES) + 0.6)
    for li, ln in enumerate(LANES):
        yt.append(base + li); yl.append("%s  %s" % (pn if li == 0 else " ", ln))
    for (sg, _h, i), e in tr.items():
        if sg != dg or "start" not in e:
            continue
        if "TRANSFER" in e:
            ax.barh(base, e["TRANSFER"] - e["start"], left=e["start"] - t0, height=0.62, color=C["setup"])
            end = e.get("BACKPATCH", e.get("FAILED"))
            if end:
                ax.barh(base, end - e["TRANSFER"], left=e["TRANSFER"] - t0, height=0.62, color=C["copy"])
        if "DONE" in e:
            ax.plot([e["DONE"] - t0], [base + 3], marker="s", ms=5, color=C["commit"])
    for (g, _h), r in rec.items():     # per replica: a bar never joins two leaders' events
        if g != rg or (shared and pi != 0):   # one recipient for all donors: its lanes once, under pair A
            continue
        for s, e in pairs_of(r.get("fp", ()), r.get("fw", ())):
            ax.barh(base + 1, e - s, left=s - t0, height=0.62, color=C["fwd"])
        # per-slot gate (2026-10-05): slots are merged as their blocks reach the follower, so the
        # merge runs from the first forwarded block to merge_done, not from the gate opening
        early = r.get("early", ())
        for s, e in pairs_of(r.get("fp", ()) if early else r.get("gate", ()), r.get("merged", ())):
            ax.barh(base + 2, e - s, left=s - t0, height=0.62, color=C["merge"])
    for t, sg in elect:
        if sg in (dg, rg) and t > t0 - 0.5:
            ax.plot([t - t0], [base - 0.55], marker="v", ms=7, color="#7b2cbf", clip_on=False)
            ax.text(t - t0, base - 0.72, " %s leader" % sg, fontsize=7, color="#7b2cbf", va="bottom")
for k in kills:
    ax.axvline(k - t0, color="#c0392b", ls=(0, (4, 3)), lw=1.3)
end = max([e.get("DONE", 0) for e in tr.values()] + [t0]) - t0
ax.set_yticks(yt); ax.set_yticklabels(yl, fontsize=8); ax.invert_yaxis()
ax.set_xlabel("Time since the first round started (s)")
ax.set_xlim(-0.3, max(end * 1.04, 1))
ax.grid(axis="x", color="#dddddd", lw=0.6); ax.set_axisbelow(True)
for s in ("top", "right"): ax.spines[s].set_visible(False)
note = " (recipient lanes shown once: one recipient group)" if shared else ""
ax.set_title("%s — migration %.2f s%s" % (title, end, note), fontsize=10, loc="left")
ax.legend(handles=[Patch(color=C["setup"], label="setup"), Patch(color=C["copy"], label="copy donor -> recipient leader"),
                   Patch(color=C["fwd"], label="replicate to follower"), Patch(color=C["merge"], label="leader merge"),
                   Line2D([], [], marker="s", ls="", color=C["commit"], label="round done"),
                   Line2D([], [], color="#c0392b", ls=(0, (4, 3)), label="crash"),
                   Line2D([], [], marker="v", ls="", color="#7b2cbf", label="new leader")],
          fontsize=7.5, ncol=7, loc="upper center", bbox_to_anchor=(0.5, -0.16), frameon=False)
fig.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
print("wrote", out)
