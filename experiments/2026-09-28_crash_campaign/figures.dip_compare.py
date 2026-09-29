#!/usr/bin/env python3
"""Pre-migration throughput dip: last night's HEALTHY vs startup pre-registration.
Sums both YCSB clients per second and aligns t=0 at the warm-up (MIGRATE-WARM on the
sg2 leader, redis logs are UTC-6)."""
import re, glob, datetime as dt, sys
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
here = sys.argv[1] if len(sys.argv) > 1 else "."
RUNS = [("campaign_heal", "before: lazy registration (5.7 GB while serving)", "#8a8f98"),
        ("prereg_heal",   "after: pool registered at startup", "#1f6fb2")]
def tput(run):
    per = {}
    for f in glob.glob(f"{here}/logs/{run}/ycsb/*/tmp/ycsb_output_ycsb[01]"):
        seen = set()
        for ln in open(f, errors="ignore"):
            m = re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d):\d+ (\d+) sec: \d+ operations; ([\d.]+) current", ln)
            if not m or (f, m.group(2)) in seen: continue
            seen.add((f, m.group(2)))
            t = dt.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
            per[t] = per.get(t, 0) + float(m.group(3))
    return per
def warm(run):
    for f in glob.glob(f"{here}/logs/{run}/logs/redis1/tmp/redis_logs/redis1_sg2.log"):
        for ln in open(f, errors="ignore"):
            if "MIGRATE-WARM: target" in ln:
                m = re.search(r"(\d+ \w+ \d{4} \d\d:\d\d:\d\d)", ln)
                return dt.datetime.strptime(m.group(1), "%d %b %Y %H:%M:%S") + dt.timedelta(hours=6)
fig, ax = plt.subplots(figsize=(9, 3.4))
for run, label, col in RUNS:
    p, w = tput(run), warm(run)
    xs = sorted(t for t in p if -20 <= (t - w).total_seconds() <= 40)
    ax.plot([(t - w).total_seconds() for t in xs], [p[t] / 1000 for t in xs], color=col, lw=2, label=label)
ax.axvline(0, color="#444", lw=1, ls="--"); ax.text(0.5, 8, "warm-up starts", fontsize=9, color="#444")
ax.set_xlabel("seconds from warm-up (MIGRATE-WARM)"); ax.set_ylabel("throughput, both clients (Kops/s)")
ax.set_ylim(0, None); ax.grid(alpha=.25); ax.legend(loc="lower right", frameon=False, fontsize=9)
ax.set_title("Pre-migration dip, 30M HEALTHY", fontsize=11)
fig.tight_layout(); fig.savefig(f"{here}/figures/fix/dip_compare.png", dpi=150)
print("wrote figures/fix/dip_compare.png")
