#!/usr/bin/env python3
"""One figure, one panel pair per scenario: total YCSB throughput (all clients summed)
and, below it, the mean read and update latency over time, with the migration
window shaded and the crash marked.

usage: plot_all_throughput_latency.py <data-dir> <out.png> [<lin-log-dir>]
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
READ, UPDATE = "#0072B2", "#D55E00"
pat = re.compile(r'^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d):\d+ \d+ sec: \d+ operations; ([\d.]+) current ops/sec')
lat_pat = {op: re.compile(r'\[%s AverageLatency\(us\)=([\d.]+)\]' % op) for op in ("READ", "UPDATE")}

def load(run):
    clients, lats = [], {"READ": {}, "UPDATE": {}}
    for f in sorted(run.glob("ycsb/*/tmp/ycsb_output_ycsb*")):
        if "load" in f.name: continue
        d = {}
        for l in open(f, errors="ignore"):
            m = pat.match(l)
            if not m: continue
            t = datetime.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
            d[t] = float(m.group(2))
            for op, p in lat_pat.items():
                lm = p.search(l)
                if lm: lats[op].setdefault(t, []).append((float(lm.group(1)) / 1000, d[t]))
        clients.append(d)
    ts = sorted(set().union(*[set(d) for d in clients]))
    t0 = next(t for t in ts if any(d.get(t, 0) > 0 for d in clients))
    ts = [t for t in ts if t >= t0]
    total = [sum(d.get(t, 0) for d in clients) / 1000 for t in ts]
    # Mean latency (ms) across clients, weighted by each client's throughput in that second;
    # a second in which no client completed an operation of that type has no value.
    def mean(v):
        w = sum(k for _, k in v)
        return sum(a * k for a, k in v) / w if w else float("nan")
    lat = {op: [mean(lats[op].get(t, [])) for t in ts] for op in lats}
    return t0, [(t - t0).total_seconds() for t in ts], total, lat

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
# Rows alternate throughput / latency, so each scenario is a throughput panel with its latency below.
fig, axes = plt.subplots(4, 4, figsize=(7.0, 3.2), sharex=True, layout="constrained",
                         gridspec_kw={"height_ratios": [1.15, 1, 1.15, 1]})
pairs = [(axes[r][c], axes[r + 1][c]) for r in (0, 2) for c in range(4)]
for top, bot in pairs[1:]: top.sharey(pairs[0][0])
# The latency axis is broken: steady state is 2-3 ms, the spikes after a crash reach 100+ ms.
LAT_LO, LAT_HI = (0, 12), (20, 160)
SPIKE_MS = 5
YLABEL_X = -0.3
ymax = 0
for i, ((ax, lax), (name, sc, title)) in enumerate(zip(pairs, PANELS)):
    run = data / name
    if not run.exists():
        ax.set_title(title + " (no data)", fontsize=9, loc="left", color=MUTED); lax.set_axis_off(); continue
    t0, x, total, lat = load(run)
    # lax only reserves the space: the two parts of the broken latency axis are drawn inside it.
    lo, hi = lax.inset_axes([0, 0, 1, 0.56]), lax.inset_axes([0, 0.64, 1, 0.36])
    # The last samples are the clients shutting down, not the system: drop them.
    tail = sorted(total[-12:-2])[5] if len(total) > 14 else 0
    while len(total) > 2 and total[-1] < 0.9 * tail:
        x.pop(); total.pop(); lat["READ"].pop(); lat["UPDATE"].pop()
    ymax = max(ymax, max(total))
    m = re.search(r"FULL.*\[(\d\d:\d\d:\d\d)\.\d+ -> (\d\d:\d\d:\d\d)\.\d+\]", (run / "migration_window.txt").read_text())
    if m:
        w0, w1 = near(t0, m.group(1), tz), near(t0, m.group(2), tz)   # redis logs: local time
        for a in (ax, lo, hi): a.axvspan(w0, w1, color=BAND, lw=0, zorder=0)
    if sc and logdir is not None and (logdir / f"tf_{sc}.log").exists():
        k = re.findall(r"\[crash_inject (\d\d:\d\d:\d\d)\] KILLED (\S+)", (logdir / f"tf_{sc}.log").read_text(errors="ignore"))
        if k:
            kx = near(t0, k[-1][0])                                    # injector: UTC, like YCSB
            for a in (ax, lo, hi): a.axvline(kx, color=CRASH, lw=0.9, ls=(0, (3, 2)), zorder=2)
    ax.plot(x, total, color=LINE, lw=1.2, solid_capstyle="round", solid_joinstyle="round", zorder=3)
    ax.set_title(title, fontsize=9, loc="left", color=INK, pad=3)
    ax.set_xlim(10, 60); ax.set_xticks([10, 20, 30, 40, 50, 60]); ax.set_yticks([0, 100, 200])
    for a in (lo, hi):
        a.plot(x, lat["UPDATE"], color=UPDATE, lw=1.1, solid_capstyle="round", solid_joinstyle="round", zorder=3)
        a.plot(x, lat["READ"], color=READ, lw=1.1, solid_capstyle="round", solid_joinstyle="round", zorder=4)
        # Mark the individual samples only where latency leaves its steady state (the spikes).
        for op, c in (("UPDATE", UPDATE), ("READ", READ)):
            pts = [(t, v) for t, v in zip(x, lat[op]) if v > SPIKE_MS]
            if pts: a.plot(*zip(*pts), ls="none", marker="o", ms=2.6, mfc=c, mec="white", mew=0.4, zorder=5)
        a.set_xlim(10, 60); a.set_xticks([10, 20, 30, 40, 50, 60])
    lo.set_ylim(*LAT_LO); lo.set_yticks([0, 10]); hi.set_ylim(*LAT_HI); hi.set_yticks([50, 150])
    col, below = i % 4, i + 4 < len(PANELS)
    for a in (ax, lo, hi):
        a.tick_params(length=2, width=0.5, pad=2, labelleft=col == 0, labelbottom=False)
        for s in ("top", "right"): a.spines[s].set_visible(False)
    lo.tick_params(labelbottom=not below)                       # no panel below: show its x ticks
    hi.spines["bottom"].set_visible(False); hi.tick_params(bottom=False)
    brk = dict(marker=[(-1, -0.6), (1, 0.6)], markersize=5, ls="none", color=MUTED, mec=MUTED, mew=0.6, clip_on=False)
    hi.plot([0], [0], transform=hi.transAxes, **brk); lo.plot([0], [1], transform=lo.transAxes, **brk)
    lax.set_frame_on(False); lax.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)
    if col == 0:
        ax.set_ylabel("YCSB throughput\n(kops/s)", fontsize=7.5, color=INK)
        lax.set_ylabel("Latency\n(ms)", fontsize=7.5, color=INK)
        for a in (ax, lax): a.yaxis.set_label_coords(YLABEL_X, 0.5)     # same x for both: labels line up
pairs[0][0].set_ylim(0, ymax * 1.08)
spare = pairs[len(PANELS):]
for p in spare:
    for a in p: a.set_axis_off()
# The empty grid cell holds the legend.
(spare[0][0] if spare else fig).legend(
    handles=[Line2D([], [], color=READ, lw=1.1, label="Mean read latency"),
             Line2D([], [], color=UPDATE, lw=1.1, label="Mean update latency"),
             Patch(facecolor=BAND, edgecolor="none", label="Migration"),
             Line2D([], [], color=CRASH, lw=0.9, ls=(0, (3, 2)), label="Crash injected")],
    loc="upper left", frameon=False, fontsize=9, handlelength=1.8, labelspacing=0.5, borderaxespad=0).set_in_layout(False)   # keep the legend from stretching its row
fig.supxlabel("Time (s)", fontsize=9, color=INK)
fig.get_layout_engine().set(w_pad=0.02, h_pad=0.015, wspace=0.02, hspace=0.01)
fig.savefig(out, dpi=400)
if out.endswith(".png"): fig.savefig(out[:-4] + ".pdf")
print("wrote", out)
