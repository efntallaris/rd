#!/usr/bin/env python3
"""3 vs 6 redis-server processes: total YCSB throughput and average latency over time
(all clients; latency weighted by each client's operations in that second).
usage: plot_3v6.py <logs-dir with 3procs/ 6procs/> <out.png>"""
import re, sys, datetime, pathlib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

logs = pathlib.Path(sys.argv[1]); out = sys.argv[2]
pat = re.compile(r'^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d):\d+ \d+ sec: \d+ operations; ([\d.]+) current ops/sec')
lat = re.compile(r'\[(READ|UPDATE) AverageLatency\(us\)=([\d.]+)\]')

def load(run):
    clients = []
    for f in sorted(run.glob("ycsb/*/tmp/ycsb_output_ycsb*")):
        if "load" in f.name: continue
        d = {}
        for l in open(f, errors="ignore"):
            m = pat.match(l)
            if not m: continue
            ls = [float(v) for _, v in lat.findall(l)]
            d[datetime.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")] = (float(m.group(2)), sum(ls) / len(ls) if ls else None)
        clients.append(d)
    ts = sorted(set().union(*[set(d) for d in clients]))
    t0 = next(t for t in ts if any(d.get(t, (0, None))[0] > 0 for d in clients))
    rows = []
    for t in ts:
        if t < t0: continue
        ops = sum(d[t][0] for d in clients if t in d)
        w = [(d[t][0], d[t][1]) for d in clients if t in d and d[t][1] is not None and d[t][0] > 0]
        l = sum(a * b for a, b in w) / sum(a for a, _ in w) if w else None
        rows.append(((t - t0).total_seconds(), ops / 1000, l))
    mid = sorted(r[1] for r in rows)[len(rows) // 2]
    rows = [r for r in rows if r[2] is not None]   # whole run, from the first operation
    return rows

INK, AXIS = "#222222", "#555555"
plt.rcParams.update({"font.size": 15, "axes.edgecolor": AXIS, "axes.linewidth": 0.9,
                     "xtick.color": AXIS, "ytick.color": AXIS, "xtick.labelcolor": INK,
                     "ytick.labelcolor": INK, "pdf.fonttype": 42})
fig, (a1, a2) = plt.subplots(1, 2, figsize=(13, 3.6), layout="constrained")
for name, label, color in (("3procs", "3 processes", "#8a8a8a"), ("6procs", "6 processes", "#111111")):
    rows = load(logs / name)
    x = [r[0] for r in rows]; thr = [r[1] for r in rows]; l = [r[2] / 1000 for r in rows]
    for ax, y, unit, fmt in ((a1, thr, "Kops/s", "%.0f"), (a2, l, "ms", "%.2f")):
        ax.plot(x, y, color=color, lw=2.2, solid_capstyle="round")
        steady = y[5:-3]; avg = sum(steady) / len(steady)   # label: steady-state mean
        ax.annotate(f"{label}  ({fmt % avg} {unit})", (x[-1], avg), xytext=(-4, -9), textcoords="offset points",
                    ha="right", va="top", color=INK, fontsize=13,
                    bbox=dict(facecolor="white", edgecolor="none", pad=1.5))
a1.set_ylim(0, 760); a1.set_yticks([0, 200, 400, 600]); a1.set_ylabel("YCSB th/put (Kops/s)")
a2.set_ylim(0, 1.5); a2.set_yticks([0, 0.5, 1.0, 1.5]); a2.set_ylabel("Avg latency (ms)")
for ax in (a1, a2):
    ax.set_xlim(0, 180); ax.set_xticks([0, 60, 120, 180]); ax.set_xlabel("Time (s)")
    ax.grid(axis="y", color="#2b2b2b", lw=1.0, ls=(0, (1, 4))); ax.set_axisbelow(True)
    for s in ("top", "right"): ax.spines[s].set_visible(False)
fig.savefig(out, dpi=140)
if out.endswith(".png"): fig.savefig(out[:-4] + ".pdf")
print("wrote", out)
