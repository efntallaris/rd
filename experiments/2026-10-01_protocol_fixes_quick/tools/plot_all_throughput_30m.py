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

data = pathlib.Path(sys.argv[1]); out = sys.argv[2]
logdir = pathlib.Path(sys.argv[3]) if len(sys.argv) > 3 else None
PANELS = [
    ("lin_healthy", None, "No fault"),
    ("crash_s1", "S1", "S1: recipient leader crashes"),
    ("crash_s2", "S2", "S2: donor leader crashes mid-transfer"),
    ("crash_s3", "S3", "S3: donor follower crashes"),
    ("crash_s4", "S4", "S4: recipient follower crashes"),
    ("crash_s5", "S5", "S5: donor leader crashes after its transfer"),
    ("crash_s8", "S8", "S8: donor and recipient leaders crash"),
]
INK, MUTED, CRASH = "#222222", "#6b6b6b", "#c0392b"
LINE, BAND, AXIS = "#111111", "#e2e2e2", "#555555"
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
plt.rcParams.update({"font.size": 15, "axes.edgecolor": AXIS, "axes.linewidth": 0.9,
                     "xtick.color": AXIS, "ytick.color": AXIS, "xtick.labelcolor": INK,
                     "ytick.labelcolor": INK, "pdf.fonttype": 42})
fig, axes = plt.subplots(2, 4, figsize=(20, 5.2), sharex=True, sharey=True, layout="constrained")
ymax = 0
for ax, (name, sc, title) in zip(axes.flat, PANELS):
    run = data / name
    if not run.exists():
        ax.set_title(title + "\n(no data)", fontsize=11, color=MUTED); continue
    t0, x, total, ncli = load(run)
    # The last samples are the clients shutting down, not the system: drop them.
    tail = sorted(total[-12:-2])[5] if len(total) > 14 else 0
    while len(total) > 2 and total[-1] < 0.9 * tail: x.pop(); total.pop()
    ymax = max(ymax, max(total))
    m = re.search(r"FULL.*\[(\d\d:\d\d:\d\d)\.\d+ -> (\d\d:\d\d:\d\d)\.\d+\]", (run / "migration_window.txt").read_text())
    if m:
        w0, w1 = near(t0, m.group(1), tz), near(t0, m.group(2), tz)   # redis logs: local time
        ax.axvspan(w0, w1, color=BAND, lw=0, zorder=0)
    if sc and logdir is not None and (logdir / f"t30_{sc}.log").exists():
        k = re.findall(r"\[crash_inject (\d\d:\d\d:\d\d)\] KILLED (\S+)", (logdir / f"t30_{sc}.log").read_text(errors="ignore"))
        if k:
            kx = near(t0, k[-1][0])                                    # injector: UTC, like YCSB
            ax.axvline(kx, color=CRASH, lw=1.4, ls=(0, (4, 3)), zorder=2)
    ax.plot(x, total, color=LINE, lw=2.2, solid_capstyle="round", solid_joinstyle="round", zorder=3)
    ax.set_title(title, fontsize=14, loc="center", color=INK, pad=7)
    ax.grid(axis="y", color="#2b2b2b", lw=1.0, ls=(0, (1, 4)), zorder=1); ax.set_axisbelow(True)
    ax.set_xlim(40, 120); ax.set_xticks([40, 60, 80, 100, 120]); ax.set_yticks([0, 100, 200])
    ax.tick_params(length=3.5, width=0.9)
    for s in ("top", "right"): ax.spines[s].set_visible(False)
axes[0][0].set_ylim(0, ymax * 1.08)
for ax in list(axes.flat)[len(PANELS):]: ax.set_visible(False)
for ax in axes[0][len(PANELS) - 4:]: ax.tick_params(labelbottom=True)   # no panel below: show its x ticks
fig.supxlabel("Time (s)", fontsize=15, color=INK)
fig.supylabel("YCSB th/put (Kops/s)", fontsize=15, color=INK)
fig.get_layout_engine().set(w_pad=0.08, h_pad=0.06, wspace=0.04, hspace=0.06)
fig.savefig(out, dpi=140)
if out.endswith(".png"): fig.savefig(out[:-4] + ".pdf")
print("wrote", out)
