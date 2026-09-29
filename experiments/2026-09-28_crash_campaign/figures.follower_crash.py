#!/usr/bin/env python3
"""Follower crashes vs client throughput: for each run, both clients' throughput around the
kill (top) and the surviving follower's AppendEntries round trip, avg and max (bottom).
Reads logs/fc_* (collected runs) and logs/follower_crash_raw/<S>/ (kill times). Log times are UTC-6."""
import re, glob, datetime as dt, os, sys
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
here = sys.argv[1] if len(sys.argv) > 1 else "."
RUNS = [("fc_s4_quiet", "S4quiet", "S4 · recipient follower, before migration", "redis3", "sg4"),
        ("fc_s3_quiet", "S3quiet", "S3 · donor follower, before migration", "redis0", "sg1"),
        ("fc_s4_migration", "S4", "S4 · recipient follower, during migration", "redis3", "sg4"),
        ("fc_s3_migration", "S3", "S3 · donor follower, during migration", "redis0", "sg1")]
def kill_time(raw):
    k = os.path.join(here, "logs/follower_crash_raw", raw, "kill.txt")
    if os.path.exists(k):
        m = re.search(r"at (\d\d:\d\d:\d\d)", open(k).read()); return m.group(1)
    tks = re.findall(r"t_kill=([\d.]+)", open(os.path.join(here, "logs/follower_crash_raw", raw, "run.log"), errors="ignore").read())
    return dt.datetime.utcfromtimestamp(float(tks[-1])).strftime("%H:%M:%S")
def tput(run):
    per = {}
    for f in glob.glob(f"{here}/logs/{run}/ycsb/*/tmp/ycsb_output_ycsb[01]"):
        seen = set()
        for ln in open(f, errors="ignore"):
            m = re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d):\d+ (\d+) sec: \d+ operations; ([\d.]+) current", ln)
            if not m or (f, m.group(2)) in seen: continue
            seen.add((f, m.group(2))); t = dt.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
            per[t] = per.get(t, 0) + float(m.group(3))
    return per
def rtt(run, host, sg):
    out = {}
    for ln in open(f"{here}/logs/{run}/logs/{host}/tmp/redis_logs/{host}_{sg}.log", errors="ignore"):
        m = re.search(r"(\d+ \w+ \d{4} \d\d:\d\d:\d\d)\.\d+ .*AE-RTT node=3 n=\d+ avg=([\d.]+)ms max=([\d.]+)ms", ln)
        if m: out[dt.datetime.strptime(m.group(1), "%d %b %Y %H:%M:%S") + dt.timedelta(hours=6)] = (float(m.group(2)), float(m.group(3)))
    return out
fig, axes = plt.subplots(2, 4, figsize=(16, 5.6), sharex=True, gridspec_kw={"height_ratios": [3, 2]})
for c, (run, raw, title, host, sg) in enumerate(RUNS):
    per, r = tput(run), rtt(run, host, sg)
    day = min(per).date(); k = dt.datetime.combine(day, dt.datetime.strptime(kill_time(raw), "%H:%M:%S").time())
    xs = [t for t in sorted(per) if -8 <= (t - k).total_seconds() <= 12]
    ax = axes[0][c]
    ax.plot([(t - k).total_seconds() for t in xs], [per[t] / 1000 for t in xs], color="#1f6fb2", lw=2, marker="o", ms=3)
    ax.axvline(0, color="#c0392b", ls="--", lw=1); ax.set_ylim(0, 200); ax.set_title(title, fontsize=10)
    ax.grid(alpha=.25)
    if c == 0: ax.set_ylabel("throughput, both clients\n(Kops/s)")
    rx = [t for t in sorted(r) if -8 <= (t - k).total_seconds() <= 12]
    ax2 = axes[1][c]
    ax2.plot([(t - k).total_seconds() for t in rx], [r[t][1] for t in rx], color="#e67e22", lw=1.5, marker="^", ms=3, label="max")
    ax2.plot([(t - k).total_seconds() for t in rx], [r[t][0] for t in rx], color="#555", lw=1.5, marker="o", ms=3, label="avg")
    ax2.axvline(0, color="#c0392b", ls="--", lw=1); ax2.set_yscale("log"); ax2.set_ylim(0.1, 1000); ax2.grid(alpha=.25, which="both")
    ax2.set_xlabel("seconds from kill")
    if c == 0: ax2.set_ylabel("surviving follower\nAppendEntries RTT (ms)"); ax2.legend(loc="upper left", fontsize=8, frameon=False)
fig.suptitle("Follower crash: client throughput and the surviving follower's AppendEntries round trip (red line = kill)", fontsize=11)
fig.tight_layout(); fig.savefig(f"{here}/figures/follower_crash/follower_crash_compare.png", dpi=150)
print("wrote figures/follower_crash/follower_crash_compare.png")
