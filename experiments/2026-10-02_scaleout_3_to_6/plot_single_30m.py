#!/usr/bin/env python3
"""3 -> 6 scale-out, one panel pair per scenario: total YCSB throughput (all clients summed)
and, below it, latency over time, with the migration window shaded and the crash marked.

Latency has two sources:
  - mean read / update latency per second, from the YCSB status lines (the runs use
    measurementtype=timeseries, which records no percentiles);
  - p99 read / write latency per second, from the per-operation history of the
    linearizability client (32 threads x 50 ops/s, running next to YCSB on ycsb0).

usage: plot_scaleout_ycsb.py <ycsb-root> <lincheck-root> <out.png> [<scenarios>]
  <ycsb-root>      has HEALTHY/, S1/ ... (this experiment's logs/) or scaleout3to6_lin_healthy/,
                   scaleout3to6_crash_s1/ ... (/tmp/experiments, overwritten by every new run);
                   each with ycsb/ and migration_window.txt
  <lincheck-root>  has HEALTHY_q*_*/, S1_q*_*/ ... at any depth (run.log with the kill time, history.jsonl)
  <scenarios>      e.g. HEALTHY,S6,S7: compact single-column version (3.33 in wide) with just these
                   scenarios side by side, throughput on top and latency below
"""
import re, sys, json, datetime, pathlib, collections
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

ycsb_root, lin_root, out = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]
PANELS = [
    ("scaleout3to6_lin_healthy", "HEALTHY", "No fault"),
    ("scaleout3to6_crash_s1", "S1", "S1: recipient leader"),
    ("scaleout3to6_crash_s2", "S2", "S2: donor leader (during)"),
    ("scaleout3to6_crash_s3", "S3", "S3: donor follower"),
    ("scaleout3to6_crash_s4", "S4", "S4: recipient follower"),
    # ("scaleout3to6_crash_s6", "S6", "S6: recipient host"),   # not shown since 2026-10-04; uncomment to bring it back
    # ("scaleout3to6_crash_s7", "S7", "S7: donor host"),   # not shown since 2026-10-04; uncomment to bring it back
    ("scaleout3to6_crash_s8", "S8", "S8: two leaders, one pair"),
    ("scaleout3to6_crash_s9", "S9", "S9: two leaders, two pairs"),
]
def run_dir(name, sc): return ycsb_root / name if (ycsb_root / name).exists() else ycsb_root / sc
PANELS = [p for p in PANELS if run_dir(p[0], p[1]).exists()]        # only the scenarios that were run
COMPACT = sys.argv[4].split(",") if len(sys.argv) > 4 else None
if COMPACT:
    SHORT = {"HEALTHY": "No fault", "S1": "S1: recip. leader", "S2": "S2: donor leader", "S3": "S3: donor follower",
             "S4": "S4: recip. follower", "S5": "S5: donor leader", "S6": "S6: recipient host", "S7": "S7: donor host",
             "S8": "S8: two leaders", "S9": "S9: two leaders"}
    PANELS = [(n, sc, SHORT[sc]) for want in COMPACT for n, sc, _ in PANELS if sc == want]
    NCOL, WIDTH = len(PANELS), 3.33                                  # USENIX column width
else:
    NCOL, WIDTH = (3, 6.0) if len(PANELS) <= 6 else (4, 7.0)
# Type sizes (pt): panel titles, axis titles and legend, tick labels.
FS_TITLE, FS_LABEL, FS_TICK = (7.5, 7, 6.5) if COMPACT else (8.5, 8, 7.5)
INK, MUTED, AXIS, CRASH = "#1a1a1a", "#555555", "#333333", "#c0392b"
LINE, BAND = "#000000", "#e9e9e9"
READ, WRITE = "#0072B2", "#D55E00"
P99_LS = (0, (1, 1.2))
# Stroke widths (pt), one hierarchy: data > p99 > crash marker > axes.
LW_DATA, LW_P99, LW_CRASH, LW_AXIS = (0.9, 0.8, 0.7, 0.5) if COMPACT else (1.1, 0.9, 0.8, 0.6)
XLIM = (30, 110)
pat = re.compile(r'^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d):\d+ \d+ sec: \d+ operations; ([\d.]+) current ops/sec')
lat_pat = {op: re.compile(r'\[%s AverageLatency\(us\)=([\d.]+)\]' % op) for op in ("READ", "UPDATE")}
FMT = "%Y-%m-%d %H:%M:%S"

def load(run):
    clients, lats = [], {"READ": {}, "UPDATE": {}}
    for f in sorted(run.glob("ycsb/*/tmp/ycsb_output_ycsb*")):
        if "load" in f.name: continue
        d = {}
        for l in open(f, errors="ignore"):
            m = pat.match(l)
            if not m: continue
            t = datetime.datetime.strptime(m.group(1), FMT)
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

def p99_series(lin, t0):
    """Per-second p99 (ms) of the linearizability client's completed operations.
    Its timestamps are ns since the client started; run.log gives that start to the second,
    so these series are aligned with YCSB to about +-1 s."""
    m = re.search(r"\[lin (\d\d:\d\d:\d\d)\] history client started", (lin / "run.log").read_text(errors="ignore"))
    if not m or not (lin / "history.jsonl").exists(): return {}
    off = near(t0, m.group(1))
    b = {"read": collections.defaultdict(list), "write": collections.defaultdict(list)}
    for l in open(lin / "history.jsonl", errors="ignore"):
        try: r = json.loads(l)
        except ValueError: continue                      # a line cut short when the client was stopped
        if r.get("phase") != "run" or r.get("status") != "ok" or not r.get("ret"): continue
        sec = int(r["ret"] // 10**9 + off)
        b["read" if r["op"] == "get" else "write"][sec].append((r["ret"] - r["inv"]) / 1e6)
    res = {}
    for op, d in b.items():
        xs = sorted(d)
        res[op] = (xs, [sorted(d[s])[min(len(d[s]) - 1, int(0.99 * len(d[s])))] for s in xs])
    return res

tz = round((datetime.datetime.utcnow() - datetime.datetime.now()).total_seconds() / 3600)
# Drawn at print size (6 or 7 in wide; the USENIX text width is 7 in), so 7-9 pt here is 7-9 pt on paper.
plt.rcParams.update({"font.family": ["Times New Roman", "Times", "STIXGeneral"], "mathtext.fontset": "stix", "font.size": FS_LABEL,
                     "axes.edgecolor": AXIS, "axes.linewidth": LW_AXIS,
                     "xtick.color": AXIS, "ytick.color": AXIS, "xtick.labelcolor": INK,
                     "ytick.labelcolor": INK, "xtick.labelsize": FS_TICK, "ytick.labelsize": FS_TICK,
                     "pdf.fonttype": 42, "ps.fonttype": 42})
nrow = -(-len(PANELS) // NCOL)
# Rows alternate throughput / latency, so each scenario is a throughput panel with its latency below.
spare = nrow * NCOL - len(PANELS)             # empty grid cells: room for the legend
fig, axes = plt.subplots(2 * nrow, NCOL, figsize=(WIDTH, 1.75 if COMPACT else 1.8 * nrow + (0.2 if spare else 0.5)), sharex=True,
                         layout="constrained", gridspec_kw={"height_ratios": [1.2, 1] * nrow}, squeeze=False)
pairs = [(axes[2 * r][c], axes[2 * r + 1][c]) for r in range(nrow) for c in range(NCOL)]
for top, bot in pairs[1:]: top.sharey(pairs[0][0]); bot.sharey(pairs[0][1])
ymax = xmax = 0
for i, ((ax, lax), (name, sc, title)) in enumerate(zip(pairs, PANELS)):
    run = run_dir(name, sc)
    if not run.exists():
        ax.set_title(title + " (no data)", fontsize=FS_TITLE, loc="center", color=MUTED); lax.set_axis_off(); continue
    t0, x, total, lat = load(run)
    # The last samples are the clients shutting down, not the system: drop them.
    tail = sorted(total[-12:-2])[5] if len(total) > 14 else 0
    while len(total) > 2 and total[-1] < 0.9 * tail:
        x.pop(); total.pop(); lat["READ"].pop(); lat["UPDATE"].pop()
    ymax, xmax = max(ymax, max(total)), max(xmax, x[-1])
    m = re.search(r"FULL.*\[(\d\d:\d\d:\d\d)\.\d+ -> (\d\d:\d\d:\d\d)\.\d+\]", (run / "migration_window.txt").read_text())
    if m:
        w0, w1 = near(t0, m.group(1), tz), near(t0, m.group(2), tz)   # redis logs: local time
        for a in (ax, lax): a.axvspan(w0, w1, color=BAND, lw=0, zorder=0)
    # The lincheck run of this YCSB run: the latest one of this scenario started before it (within 15 min).
    lins = [(datetime.datetime.strptime(d.name[-15:], "%Y%m%d_%H%M%S"), d) for d in lin_root.glob(f"**/{sc}_q*_*") if d.is_dir()]
    lins = sorted((t, d) for t, d in lins if 0 <= (t0 - t).total_seconds() < 900)
    lin = lins[-1][1] if lins else None
    if lin is not None:
        for hms, _ in re.findall(r"\[crash_inject (\d\d:\d\d:\d\d)\] KILLED (\S+)", (lin / "run.log").read_text(errors="ignore")):
            for a in (ax, lax): a.axvline(near(t0, hms), color=CRASH, lw=LW_CRASH, ls=(0, (3, 2)), zorder=2)   # injector: UTC, like YCSB
        for op, c in (("write", WRITE), ("read", READ)):
            px, py = p99_series(lin, t0).get(op, ([], []))
            keep = [(a, b) for a, b in zip(px, py) if a <= x[-1]]
            if keep: lax.plot(*zip(*keep), color=c, lw=LW_P99, ls=P99_LS, zorder=3)
    ax.plot(x, total, color=LINE, lw=LW_DATA, solid_capstyle="round", solid_joinstyle="round", zorder=3)
    lax.plot(x, lat["UPDATE"], color=WRITE, lw=LW_DATA, solid_capstyle="round", solid_joinstyle="round", zorder=4)
    lax.plot(x, lat["READ"], color=READ, lw=LW_DATA, solid_capstyle="round", solid_joinstyle="round", zorder=5)
    ax.set_title(title, fontsize=FS_TITLE, loc="center", color=INK, pad=3 if COMPACT else 4)
    col = i % NCOL
    for a in (ax, lax):
        a.tick_params(length=2 if COMPACT else 2.5, width=LW_AXIS, pad=1.5 if COMPACT else 2, labelleft=col == 0, labelbottom=False)
        for s in ("top", "right"): a.spines[s].set_visible(False)
    lax.tick_params(labelbottom=True)                           # under every scenario: all rows get the same gap
    ax.tick_params(labelbottom=False)
    if col == 0:
        ax.set_ylabel("Tput (kops/s)" if COMPACT else "YCSB throughput\n(kops/s)", color=INK)
        lax.set_ylabel("Latency (ms)" if COMPACT else "Latency\n(ms)", color=INK)
        for a in (ax, lax): a.yaxis.set_label_coords(-0.36 if COMPACT else -0.22 if NCOL == 3 else -0.27, 0.5)        # same x for both: labels line up
# Latency goes from under 1 ms to seconds after a crash: log scale.
lax0 = pairs[0][1]
lax0.set_yscale("log"); lax0.set_ylim(0.3, 3000); lax0.set_yticks([1, 10, 100, 1000])
lax0.yaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter("%g"))
for _, a in pairs: a.minorticks_off()
pairs[0][0].set_ylim(0, ymax * 1.08); pairs[0][0].yaxis.set_major_locator(matplotlib.ticker.MaxNLocator(3))
pairs[0][0].set_xlim(*XLIM); pairs[0][0].xaxis.set_major_locator(matplotlib.ticker.MultipleLocator(20))
for p in pairs[len(PANELS):]:
    for a in p: a.set_axis_off()
handles = [Line2D([], [], color=READ, lw=LW_DATA, label="Mean read (YCSB)"),
           Line2D([], [], color=WRITE, lw=LW_DATA, label="Mean update (YCSB)"),
           Line2D([], [], color=READ, lw=LW_P99, ls=P99_LS, label="p99 read (probe client)"),
           Line2D([], [], color=WRITE, lw=LW_P99, ls=P99_LS, label="p99 write (probe client)"),
           Patch(facecolor=BAND, edgecolor="none", label="Migration"),
           Line2D([], [], color=CRASH, lw=LW_CRASH, ls=(0, (3, 2)), label="Crash injected")]
kw = dict(frameon=False, fontsize=FS_LABEL, handlelength=2.0, borderaxespad=0)
fig.supxlabel("Time (s)", fontsize=FS_LABEL, color=INK)
fig.get_layout_engine().set(w_pad=0.03, h_pad=0.03, wspace=0.03, hspace=0.02)
if COMPACT:
    # No room for a legend row: small keys inside the first and last panels, in the area that is
    # empty after the migration (a long migration window in those two would collide with them).
    fig.get_layout_engine().set(w_pad=0.015, h_pad=0.015, wspace=0.02, hspace=0.03)
    GREY = "#444444"
    ikw = dict(frameon=False, fontsize=FS_TICK, handlelength=1.5, handletextpad=0.4, labelspacing=0.2, borderaxespad=0.2, borderpad=0)
    last = len(PANELS) - 1
    keys = [(0, 0, "lower right", [Patch(facecolor=BAND, edgecolor=MUTED, lw=0.4, label="Migration")]),   # edged: it can sit on the band
            (last, 0, "lower right", [Line2D([], [], color=CRASH, lw=LW_CRASH, ls=(0, (3, 2)), label="Crash")]),
            (0, 1, "upper right", [Line2D([], [], color=READ, lw=LW_DATA, label="Read"), Line2D([], [], color=WRITE, lw=LW_DATA, label="Write")]),
            (last, 1, "upper right", [Line2D([], [], color=GREY, lw=LW_DATA, label="Mean"), Line2D([], [], color=GREY, lw=LW_P99, ls=P99_LS, label="p99")])]
    for i, row, loc, h in keys:
        pairs[i][row].legend(handles=h, loc=loc, **ikw)
elif spare:
    # Centre the legend in the empty cells (their position is known only after the layout has run).
    fig.canvas.draw()
    boxes = [a.get_position() for p in pairs[len(PANELS):] for a in p]
    cx = (min(b.x0 for b in boxes) + max(b.x1 for b in boxes)) / 2
    cy = (min(b.y0 for b in boxes) + max(b.y1 for b in boxes)) / 2
    fig.legend(handles=handles, loc="center", bbox_to_anchor=(cx, cy), ncol=1 if spare == 1 else 2,
               columnspacing=1.6, labelspacing=0.7, **kw).set_in_layout(False)
else:
    fig.legend(handles=handles, loc="outside upper center", ncol=3, columnspacing=1.6, labelspacing=0.35, **kw)
fig.savefig(out, dpi=400)
if out.endswith(".png"): fig.savefig(out[:-4] + ".pdf")
print("wrote", out)
