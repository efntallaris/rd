#!/usr/bin/env python3
"""Before/after for the follower-stall fix: logs/final_{healthy,s3,s4} (before) vs
logs/stall_{healthy,s3,s4} (follower pools registered at startup on the shared PD).
Writes figures/{OUT}/{compare_70s.png, compare_full.png, stall_metrics.json}."""
import glob, json, re, sys, datetime as dt
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
here = sys.argv[1] if len(sys.argv) > 1 else "."
TZ = dt.timedelta(hours=6)            # redis logs are UTC-6, YCSB is UTC
RUNS = [("healthy", "HEALTHY · no failure"), ("s3", "S3 · donor follower killed"), ("s4", "S4 · recipient follower killed")]
AFTER, OUT = "stall", "stall"
if len(sys.argv) > 2 and sys.argv[2] == "recov":
    RUNS = [("s1", "S1 · recipient leader killed"), ("s4", "S4 · recipient follower killed"),
            ("s6", "S6 · donor + recipient leaders killed")]
    AFTER, OUT = "recov", "recov"

def ycsb(d):
    per = {}
    for f in glob.glob(f"{d}/ycsb/*/tmp/ycsb_output_ycsb[01]"):
        lines = open(f, errors="ignore").read().splitlines()
        st = [i for i, l in enumerate(lines) if l.startswith("Command line:")]
        seen = set()
        for l in lines[st[-1] if st else 0:]:
            m = re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d):\d+ (\d+) sec: \d+ operations; ([\d.]+) current", l)
            if m and m.group(2) not in seen:
                seen.add(m.group(2)); t = dt.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                per[t] = per.get(t, 0) + float(m.group(3)) / 1000
    return per

def mig_window(d, day):
    s = open(f"{d}/migration_window.txt", errors="ignore").read()
    m = re.search(r"FULL.*?\[(\d\d:\d\d:\d\d\.\d+) -> (\d\d:\d\d:\d\d\.\d+)\]", s)
    f = lambda x: dt.datetime.combine(day, dt.datetime.strptime(x, "%H:%M:%S.%f").time()) + TZ
    return f(m.group(1)), f(m.group(2))

def kill_time(run, d, day):
    if run == "healthy": return None
    try:
        e = float(re.search(r"t_kill=([\d.]+)", open(f"{d}/kill.txt").read()).group(1))
        return dt.datetime.utcfromtimestamp(e)
    except Exception:
        k = json.load(open(f"{here}/figures/final/metrics_final.json"))[run]["kills"][0]
        return dt.datetime.combine(day, dt.datetime.strptime(k.split("@ ")[1].split()[0], "%H:%M:%S.%f").time())

def load(run, which):
    d = f"{here}/logs/{which}_{run}"
    per = ycsb(d); ts = sorted(per); t0 = next(t for t in ts if per[t] > 0)
    day = (ts[0] - TZ).date(); m0, m1 = mig_window(d, day); k = kill_time(run, d, ts[0].date())
    rel = lambda t: (t - t0).total_seconds()
    pre = [per[t] for t in ts if m0 - dt.timedelta(seconds=30) <= t <= m0 - dt.timedelta(seconds=5)]
    base = sum(pre) / len(pre)
    win = [t for t in ts if m0 - dt.timedelta(seconds=3) <= t <= m1 + dt.timedelta(seconds=20)]
    below = sum(1 for t in win if per[t] < 0.5 * base)
    post = [t for t in ts if m1 + dt.timedelta(seconds=5) <= t]
    below_post = sum(1 for t in post if per[t] < 0.8 * sum(per[x] for x in post) / len(post))
    return dict(x=[rel(t) for t in ts], y=[per[t] for t in ts], m=(rel(m0), rel(m1)), k=rel(k) if k else None,
                base=round(base, 1), min_mig=round(min(per[t] for t in win), 1), below_half=below,
                post_dips=below_post, mig_s=round((m1 - m0).total_seconds(), 2))

res = {r: {w: load(r, w) for w in ("final", AFTER)} for r, _ in RUNS}
json.dump({r: {w: {k: v for k, v in res[r][w].items() if k not in ("x", "y")} for w in res[r]} for r in res},
          open(f"{here}/figures/{OUT}/stall_metrics.json", "w"), indent=1)
for r in res:
    for w in ("final", AFTER):
        v = res[r][w]; print(f"{r:8s} {w:6s} base={v['base']} min_in_mig={v['min_mig']} below_half={v['below_half']} post_dips={v['post_dips']} mig={v['mig_s']}s")

COL = {"final": "#9aa5b1", AFTER: "#1f6fb2"}
LAB = {"final": "before (pools registered per connection, on the main thread)", "stall": "after (pools registered at startup, off the main thread)"} if AFTER == "stall" else {"final": "before (campaign build)", "recov": "after (recovery fixes)"}
for name, xmax in (("compare_70s", 75), ("compare_full", 330)):
    fig, axs = plt.subplots(3, 1, figsize=(8, 7.2), sharex=True)
    for ax, (r, title) in zip(axs, RUNS):
        for w in ("final", AFTER):
            v = res[r][w]
            ax.plot(v["x"], v["y"], color=COL[w], lw=1.4 if w == AFTER else 1.1, label=LAB[w])
        v = res[r][AFTER]; ax.axvspan(*v["m"], color="#1f6fb2", alpha=0.08, lw=0)
        for w in ("final", AFTER):
            if res[r][w]["k"] is not None: ax.axvline(res[r][w]["k"], color="#c0392b", ls="--" if w == AFTER else ":", lw=1)
        ax.set_xlim(0, xmax); ax.set_ylim(0, 215); ax.grid(alpha=.25)
        ax.set_title(f"{title}   ·   below half: {res[r]['final']['below_half']} s → {res[r][AFTER]['below_half']} s", fontsize=9.5, loc="left")
    if xmax <= 80: axs[-1].set_xticks(range(0, 76, 5))
    axs[0].legend(fontsize=8, loc="lower right", frameon=False)
    axs[-1].set_xlabel("seconds since YCSB start (runs aligned on their first operation)")
    fig.text(0.01, 0.5, "Kops/s (both clients)", rotation=90, va="center")
    fig.tight_layout(rect=(0.03, 0, 1, 1)); fig.savefig(f"{here}/figures/{OUT}/{name}.png", dpi=150); plt.close(fig)
