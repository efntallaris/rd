#!/usr/bin/env python3
"""3 vs 6 redis-server processes: throughput and average latency over the WHOLE run
(YCSB's end-of-run summary; throughput summed over clients, latency weighted by operations).
usage: plot_3v6_overall.py <logs-dir with 3procs/ 6procs/> <out.png>"""
import re, sys, pathlib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

logs = pathlib.Path(sys.argv[1]); out = sys.argv[2]

def overall(run):
    thr = 0.0; num = 0.0; den = 0.0
    for f in sorted(run.glob("ycsb/*/tmp/ycsb_output_ycsb*")):
        if "load" in f.name: continue
        t = open(f, errors="ignore").read()
        thr += float(re.search(r"\[OVERALL\], Throughput\(ops/sec\), ([\d.]+)", t).group(1))
        for op in ("READ", "UPDATE"):
            n = float(re.search(r"\[%s\], Operations, ([\d.]+)" % op, t).group(1))
            a = float(re.search(r"\[%s\], AverageLatency\(us\), ([\d.]+)" % op, t).group(1))
            num += n * a; den += n
    return thr / 1000, num / den / 1000

INK, AXIS = "#222222", "#555555"
plt.rcParams.update({"font.size": 15, "axes.edgecolor": AXIS, "axes.linewidth": 0.9,
                     "xtick.color": AXIS, "ytick.color": AXIS, "xtick.labelcolor": INK,
                     "ytick.labelcolor": INK, "pdf.fonttype": 42})
names = ["3 procs", "6 procs"]; colors = ["#8a8a8a", "#111111"]
vals = [overall(logs / "3procs"), overall(logs / "6procs")]
fig, axes = plt.subplots(1, 2, figsize=(9, 3.4), layout="constrained")
fig.get_layout_engine().set(wspace=0.12)
for ax, i, ylabel, fmt, top, ticks in ((axes[0], 0, "YCSB th/put (Kops/s)", "%.0f", 760, [0, 200, 400, 600]),
                                       (axes[1], 1, "Avg latency (ms)", "%.2f", 1.4, [0, 0.5, 1.0])):
    y = [v[i] for v in vals]
    bars = ax.bar(names, y, width=0.55, color=colors)
    for b, v in zip(bars, y):
        ax.annotate(fmt % v, (b.get_x() + b.get_width() / 2, v), xytext=(0, 4), textcoords="offset points",
                    ha="center", va="bottom", color=INK, fontsize=14)
    ax.set_ylim(0, top); ax.set_yticks(ticks); ax.set_ylabel(ylabel)
    ax.tick_params(axis="x", length=0)
    for s in ("top", "right"): ax.spines[s].set_visible(False)
fig.savefig(out, dpi=140)
if out.endswith(".png"): fig.savefig(out[:-4] + ".pdf")
print("wrote", out, vals)
