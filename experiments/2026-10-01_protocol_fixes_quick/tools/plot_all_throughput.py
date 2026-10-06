#!/usr/bin/env python3
"""One figure, one panel per scenario: total YCSB throughput (all clients summed)
over time, with the migration window shaded and the crash marked.

usage: plot_all_throughput.py <data-dir> <out.png> [<lin-log-dir>]
  <data-dir>     has lin_healthy/, crash_s1/ ... (each with ycsb/ and migration_window.txt)
  <lin-log-dir>  has tf_S1.log ... (run_lin_scenario output; gives the kill time)
"""
import re, sys, datetime, pathlib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

data = pathlib.Path(sys.argv[1]); out = sys.argv[2]
logdir = pathlib.Path(sys.argv[3]) if len(sys.argv) > 3 else None
PANELS = [
    ("lin_healthy", None, "No fault"),
    ("crash_s1", "S1", "S1: recipient leader"),
    ("crash_s2", "S2", "S2: donor leader (during)"),
    ("crash_s3", "S3", "S3: donor follower"),
    ("crash_s4", "S4", "S4: recipient follower"),
    ("crash_s5", "S5", "S5: donor leader (after)"),
    ("crash_s8_keep", "S8keep", "S8: both leaders"),
]
INK, MUTED, CRASH = "#1a1a1a", "#555555", "#c0392b"
LINE, BAND = "#000000", "#e9e9e9"
pat = re.compile(r'^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d):\d+ \d+ sec: \d+ operations; ([\d.]+) current ops/sec')

def load(run):
    clients = []
    for f in sorted(run.glob("ycsb/*/tmp/ycsb_output_ycsb*")):
        if "load" in f.name: continue
        d = {}
        for l in open(f, errors="ignore"):
            m = pat.match(l)
            if m: d[datetime.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")] = float(m.group(2))
        clients.append(d)
    ts = sorted(set().union(*[set(d) for d in clients]))
    t0 = next(t for t in ts if any(d.get(t, 0) > 0 for d in clients))
    ts = [t for t in ts if t >= t0]
    total = [sum(d.get(t, 0) for d in clients) / 1000 for t in ts]
    return t0, [(t - t0).total_seconds() for t in ts], total, len(clients)

def near(t0, hms, off_h=0):
    h, mi, se = map(int, hms.split(":"))
    t = t0.replace(hour=h, minute=mi, second=se) + datetime.timedelta(hours=off_h)
    while t < t0 - datetime.timedelta(hours=12): t += datetime.timedelta(days=1)
    while t > t0 + datetime.timedelta(hours=12): t -= datetime.timedelta(days=1)
    return (t - t0).total_seconds()

tz = round((datetime.datetime.utcnow() - datetime.datetime.now()).total_seconds() / 3600)
# Drawn at print size: 7 in is the USENIX text width (figure*), so 7-8 pt here is 7-8 pt on paper.
plt.rcParams.update({"font.family": ["Times New Roman", "Times", "STIXGeneral"], "mathtext.fontset": "stix", "font.size": 9,
                     "axes.edgecolor": MUTED, "axes.linewidth": 0.5,
                     "xtick.color": MUTED, "ytick.color": MUTED, "xtick.labelcolor": INK,
                     "ytick.labelcolor": INK, "xtick.labelsize": 8.5, "ytick.labelsize": 8.5,
                     "pdf.fonttype": 42, "ps.fonttype": 42})
fig, axes = plt.subplots(2, 4, figsize=(7.0, 1.95), sharex=True, sharey=True, layout="constrained")
ymax = 0
for ax, (name, sc, title) in zip(axes.flat, PANELS):
    run = data / name
    if not run.exists():
        ax.set_title(title + " (no data)", fontsize=9, loc="left", color=MUTED); continue
    t0, x, total, ncli = load(run)
    # The last samples are the clients shutting down, not the system: drop them.
    tail = sorted(total[-12:-2])[5] if len(total) > 14 else 0
    while len(total) > 2 and total[-1] < 0.9 * tail: x.pop(); total.pop()
    ymax = max(ymax, max(total))
    m = re.search(r"FULL.*\[(\d\d:\d\d:\d\d)\.\d+ -> (\d\d:\d\d:\d\d)\.\d+\]", (run / "migration_window.txt").read_text())
    if m:
        w0, w1 = near(t0, m.group(1), tz), near(t0, m.group(2), tz)   # redis logs: local time
        ax.axvspan(w0, w1, color=BAND, lw=0, zorder=0)
    if sc and logdir is not None and (logdir / f"tf_{sc}.log").exists():
        k = re.findall(r"\[crash_inject (\d\d:\d\d:\d\d)\] KILLED (\S+)", (logdir / f"tf_{sc}.log").read_text(errors="ignore"))
        if k:
            kx = near(t0, k[-1][0])                                    # injector: UTC, like YCSB
            ax.axvline(kx, color=CRASH, lw=0.9, ls=(0, (3, 2)), zorder=2)
    ax.plot(x, total, color=LINE, lw=1.2, solid_capstyle="round", solid_joinstyle="round", zorder=3)
    ax.set_title(title, fontsize=9, loc="left", color=INK, pad=3)
    ax.set_xlim(10, 60); ax.set_xticks([10, 20, 30, 40, 50, 60]); ax.set_yticks([0, 100, 200])
    ax.tick_params(length=2, width=0.5, pad=2)
    for s in ("top", "right"): ax.spines[s].set_visible(False)
axes[0][0].set_ylim(0, ymax * 1.08)
spare = list(axes.flat)[len(PANELS):]
for ax in spare: ax.set_axis_off()
for ax in axes[0][len(PANELS) - 4:]: ax.tick_params(labelbottom=True)   # no panel below: show its x ticks
# The empty grid cell holds the legend.
(spare[0] if spare else fig).legend(
    handles=[Line2D([], [], color=LINE, lw=1.2, label="Throughput"),
             Patch(facecolor=BAND, edgecolor="none", label="Migration"),
             Line2D([], [], color=CRASH, lw=0.9, ls=(0, (3, 2)), label="Crash injected")],
    loc="center left", frameon=False, fontsize=9, handlelength=1.8, labelspacing=0.5, borderaxespad=0)
fig.supxlabel("Time (s)", fontsize=9, color=INK)
fig.supylabel("YCSB throughput (kops/s)", fontsize=9, color=INK)
fig.get_layout_engine().set(w_pad=0.03, h_pad=0.03, wspace=0.03, hspace=0.05)
fig.savefig(out, dpi=400)
if out.endswith(".png"): fig.savefig(out[:-4] + ".pdf")
print("wrote", out)
