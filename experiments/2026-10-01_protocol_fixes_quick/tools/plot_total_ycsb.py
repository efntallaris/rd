#!/usr/bin/env python3
"""Throughput and latency over time, SUMMED over all YCSB clients of a run.

plot_full_run.py / plot_ycsb_timeseries.py read ycsb0 only, which misleads when
the clients start at different times (one client's share halves when the other
joins). This plots the total, each client's share, and the op-weighted latency.

usage: plot_total_ycsb.py <run-dir> <out.png> [title]
  <run-dir> has ycsb/<host>/tmp/ycsb_output_<host> and migration_window.txt
"""
import re, sys, datetime, pathlib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

run = pathlib.Path(sys.argv[1]); out = sys.argv[2]
title = sys.argv[3] if len(sys.argv) > 3 else run.name
pat = re.compile(r'^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d):\d+ \d+ sec: \d+ operations; ([\d.]+) current ops/sec'
                 r'(?:.*?READ AverageLatency\(us\)=([\d.]+))?(?:.*?UPDATE AverageLatency\(us\)=([\d.]+))?')
clients = {}
for f in sorted(run.glob("ycsb/*/tmp/ycsb_output_ycsb*")):
    if "load" in f.name: continue
    d = {}
    for l in open(f, errors="ignore"):
        m = pat.match(l)
        if not m: continue
        t = datetime.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
        d[t] = (float(m.group(2)), float(m.group(3) or 0), float(m.group(4) or 0))
    clients[f.parent.parent.name] = d
ts = sorted(set().union(*[set(d) for d in clients.values()]))
# time zero = first second in which any client completed operations
t0 = next(t for t in ts if any(d.get(t, (0,))[0] > 0 for d in clients.values()))
ts = [t for t in ts if t >= t0]
x = [(t - t0).total_seconds() for t in ts]
total = [sum(d.get(t, (0, 0, 0))[0] for d in clients.values()) for t in ts]
def wlat(i):
    out_ = []
    for t in ts:
        num = sum(d[t][0] * d[t][i] for d in clients.values() if t in d and d[t][i] > 0)
        den = sum(d[t][0] for d in clients.values() if t in d and d[t][i] > 0)
        out_.append(num / den if den else float("nan"))
    return out_
rd, up = wlat(1), wlat(2)

# migration window: redis logs are local time, YCSB stamps are UTC
win = None
wf = run / "migration_window.txt"
if wf.exists():
    m = re.search(r"FULL.*\[(\d\d:\d\d:\d\d)\.\d+ -> (\d\d:\d\d:\d\d)\.\d+\]", wf.read_text())
    if m:
        off = round((datetime.datetime.utcnow() - datetime.datetime.now()).total_seconds() / 3600)
        def conv(s):
            h, mi, se = map(int, s.split(":"))
            t = t0.replace(hour=h, minute=mi, second=se) + datetime.timedelta(hours=off)
            while t < t0 - datetime.timedelta(hours=12): t += datetime.timedelta(days=1)
            while t > t0 + datetime.timedelta(hours=12): t -= datetime.timedelta(days=1)
            return (t - t0).total_seconds()
        win = (conv(m.group(1)), conv(m.group(2)))

def avg(lo, hi):
    v = [y for xx, y in zip(x, total) if lo <= xx < hi]
    return sum(v) / len(v) if v else 0
fig, (a1, a2) = plt.subplots(2, 1, figsize=(11, 7), sharex=True)
for name, d in clients.items():
    a1.plot(x, [d.get(t, (0,))[0] / 1000 for t in ts], lw=1, alpha=.6, label=f"{name} only")
a1.plot(x, [v / 1000 for v in total], color="black", lw=2, label="total (all clients)")
a1.set_ylabel("Throughput (Kops/s)"); a1.set_ylim(0, None); a1.grid(alpha=.3)
a2.plot(x, rd, label="READ"); a2.plot(x, up, label="UPDATE")
a2.set_ylabel("Avg latency (µs), op-weighted"); a2.set_xlabel("seconds since first YCSB operation")
a2.grid(alpha=.3)
note = ""
if win:
    for a in (a1, a2): a.axvspan(win[0], win[1], color="grey", alpha=.3)
    # compare only seconds in which EVERY client was running
    allrun = [xx for xx, t in zip(x, ts) if all(d.get(t, (0,))[0] > 0 for d in clients.values())]
    # "before" = the last seconds before the migration, once the slowest
    # client has finished ramping up (clients start their threads gradually)
    # It stops 5 s short of the logged window: throughput already dips a few
    # seconds earlier (pre-migration warm-up steps), and that dip is not "before".
    b1 = win[0] - 5
    b0 = max(min(allrun) + 10, b1 - 12) if allrun else b1 - 12
    before, after = avg(b0, b1), avg(win[1] + 5, win[1] + 65)
    if before > 0:
        note = (f"all clients running: {before/1000:.0f}K before -> {after/1000:.0f}K after "
                f"({(after/before-1)*100:+.0f}%)   migration {win[1]-win[0]:.1f}s")
        a1.hlines(before / 1000, b0, b1, colors="tab:red", linestyles="--", lw=1)
        a1.hlines(after / 1000, win[1] + 5, min(win[1] + 65, x[-1]), colors="tab:red", linestyles="--", lw=1)
    lat = [v for xx, v in zip(x, up) if win[0] - 5 <= xx <= win[1] + 5 and v == v]
    base = sorted(v for v in up if v == v)
    if base: a2.set_ylim(0, max(base[int(len(base) * .98)] * 1.3, 1))
a1.set_title(f"{title} — {len(clients)} YCSB clients\n{note}", fontsize=11)
a1.legend(loc="lower right", fontsize=8); a2.legend(loc="upper right", fontsize=8)
fig.tight_layout(); fig.savefig(out, dpi=140)
print("wrote", out, "|", note)
