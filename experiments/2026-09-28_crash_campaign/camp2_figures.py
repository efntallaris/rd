#!/usr/bin/env python3
"""Campaign figures (build: reverts + landing-pool fix + hostname fix + raft 300/50 ms, 3-min
workload), from logs/camp2_*: client throughput per run, and a per-range recovery breakdown for
the leader-crash runs. Phase times are seconds after the (first) kill, read from the redis logs
(see the README section for the log lines each one comes from)."""
import glob, os, re, sys, datetime as dt
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
from matplotlib.patches import Patch
here = sys.argv[1] if len(sys.argv) > 1 else "."
OUT = os.path.join(here, "figures", "camp2")
RUNS = [("s2", "S2 · donor leader killed", 16.4), ("s3", "S3 · donor follower killed", None),
        ("s4", "S4 · recipient follower killed", None), ("s5", "S5 · donor leader killed after its transfer", 2.9),
        ("s6", "S6 · donor + recipient leaders killed", 8.7)]

def ycsb(d):
    per = {}
    for f in glob.glob(f"{d}/ycsb/*/tmp/ycsb_output_ycsb[01]"):
        L = open(f, errors="ignore").read().splitlines()
        st = [i for i, l in enumerate(L) if l.startswith("Command line:")]; seen = set()
        for l in L[st[-1] if st else 0:]:
            m = re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d):\d+ (\d+) sec: \d+ operations; ([\d.]+) current", l)
            if m and m.group(2) not in seen:
                seen.add(m.group(2)); t = dt.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                per[t] = per.get(t, 0) + float(m.group(3)) / 1000
    return per

fig, axs = plt.subplots(len(RUNS), 1, figsize=(8, 1.9 * len(RUNS)), sharex=True)
for ax, (r, title, rec) in zip(axs, RUNS):
    d = f"{here}/logs/camp2_{r}"; per = ycsb(d); ts = sorted(per); t0 = next(t for t in ts if per[t] > 0)
    kills = [dt.datetime.utcfromtimestamp(float(re.search(r"t_kill=([\d.]+)", open(f"{d}/kill.txt").read()).group(1)))]
    if os.path.exists(f"{d}/second_kill.txt"):
        k = re.search(r"at (\d\d:\d\d:\d\d\.\d{6})", open(f"{d}/second_kill.txt").read())
        kills.append(dt.datetime.combine(ts[0].date(), dt.datetime.strptime(k.group(1), "%H:%M:%S.%f").time()))
    k0 = min(kills); rel = lambda t: (t - t0).total_seconds()
    if rec: ax.axvspan(rel(k0), rel(k0) + rec, color="#1f6fb2", alpha=0.10, lw=0)
    ax.plot([rel(t) for t in ts], [per[t] for t in ts], color="#1f6fb2", lw=1.3)
    for k in kills: ax.axvline(rel(k), color="#c0392b", ls="--", lw=1)
    ax.set_xlim(0, 75); ax.set_ylim(0, 215); ax.set_yticks([0, 100, 200]); ax.grid(alpha=.25)
    extra = f"all ranges durable {rec:.1f} s after the kill (shaded)" if rec else "no recovery needed"
    ax.set_title(f"{title}   ·   {extra}", fontsize=9, loc="left", pad=2)
axs[-1].set_xticks(range(0, 76, 5)); axs[-1].set_xlabel("seconds since YCSB start")
fig.text(0.01, 0.5, "Kops/s (both clients)", rotation=90, va="center")
fig.tight_layout(rect=(0.03, 0, 1, 1)); fig.savefig(os.path.join(OUT, "tput_campaign.png"), dpi=150); plt.close(fig)

# Per-range phases, seconds after the (first) kill.
C = {"detect": "#2C58A0", "merge": "#A0650F", "prep": "#8E7CC3", "xfer": "#217A3C",
     "commit": "#9BD1A8", "stall": "#c0392b", "wait": "#C9D0DA"}
LANES = [  # (label, [(start, end, kind)], done)
 ("S2 · detect (new sg1 leader)", [(0, 0.45, "detect")], None),
 ("S2 · sg1 resume", [(0.45, 0.52, "prep"), (0.52, 2.66, "xfer"), (2.66, 3.07, "commit"), (3.07, 14.30, "stall"), (14.30, 14.34, "commit")], 14.34),
 ("S2 · sg2 playbook re-drive", [(0, 1.85, "wait"), (1.85, 6.27, "prep"), (6.27, 10.23, "xfer"), (10.23, 10.66, "commit"), (10.66, 15.32, "stall"), (15.32, 15.36, "commit")], 15.36),
 ("S2 · sg3 playbook re-drive", [(0, 4.91, "wait"), (4.91, 7.27, "prep"), (7.27, 11.08, "xfer"), (11.08, 12.45, "commit"), (12.45, 16.35, "stall"), (16.35, 16.38, "commit")], 16.38),
 ("S5 · detect (new sg1 leader)", [(0, 0.30, "detect")], None),
 ("S5 · sg3 (in its commit at the kill)", [(0, 0.86, "xfer"), (0.86, 2.86, "commit")], 2.86),
 ("S6 · detect (new sg4 leader)", [(0, 0.64, "detect")], None),
 ("S6 · sg4 blocking merge", [(0.64, 4.78, "merge")], None),
 ("S6 · sg2 re-home", [(0, 4.79, "wait"), (4.79, 4.91, "prep"), (4.91, 5.98, "xfer"), (5.98, 6.96, "commit")], 6.96),
 ("S6 · sg3 playbook re-drive", [(0, 4.92, "wait"), (4.92, 6.53, "prep"), (6.53, 7.52, "xfer"), (7.52, 8.67, "commit")], 8.67),
]
fig, ax = plt.subplots(figsize=(9, 0.45 * len(LANES) + 1.4))
for i, (lab, segs, done) in enumerate(LANES):
    for a, b, k in segs: ax.barh(i, b - a, left=a, color=C[k], height=0.6)
    if done: ax.text(done + 0.15, i, f"{done:.1f} s", va="center", fontsize=8, fontweight="bold")
ax.set_yticks(range(len(LANES))); ax.set_yticklabels([l[0] for l in LANES], fontsize=8); ax.invert_yaxis()
ax.set_xlim(0, 18.5); ax.set_xticks(range(0, 19, 1)); ax.set_xlabel("seconds after the (first) kill"); ax.grid(axis="x", alpha=.3)
ax.legend(handles=[Patch(color=C["detect"], label="failure detection + election"),
                   Patch(color=C["merge"], label="new sg4 leader's blocking merge"),
                   Patch(color=C["wait"], label="not started yet"),
                   Patch(color=C["prep"], label="donor link / source-pool setup"),
                   Patch(color=C["xfer"], label="RDMA transfer"),
                   Patch(color=C["commit"], label="merge, forward, commit"),
                   Patch(color=C["stall"], label="waiting on the dead session's stalled forwarder")],
          fontsize=7, loc="lower right", frameon=False)
fig.tight_layout(); fig.savefig(os.path.join(OUT, "recovery_breakdown.png"), dpi=150); plt.close(fig)
print("ok")
