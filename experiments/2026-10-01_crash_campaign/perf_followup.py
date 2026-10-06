#!/usr/bin/env python3
"""Follow-up perf analysis (2026-10-01): warm-up dip with/without registration throttle,
and per-leader command rates before/after the migration.

Usage: perf_followup.py <campaign dir> <run>[=<label>] ...
  each run is logs/final_<run>/ (as assembled from /tmp/crash_inject/<O>/runs/HEALTHY) and
  may carry ops.txt (per-instance total_commands_processed, 1 s samples).
Writes figures/final/perf_followup.png and prints a summary.
"""
import collections
import datetime as dt
import os
import re
import sys

here = sys.argv[1]
runs = [a.split("=", 1) if "=" in a else (a, a) for a in sys.argv[2:]]
sys.argv = [sys.argv[0], here]
exec(open(os.path.join(here, "breakdown.py")).read().split("res = {}")[0])

LEADERS = {"redis0:8000": "sg1 leader", "redis1:8001": "sg2 leader", "redis2:8002": "sg3 leader",
           "redis3:8000": "sg4 leader"}


def ops_rates(run):
    """{instance: [(t_utc, ops/s)]} from ops.txt (cumulative counter -> rate)."""
    f = f"{here}/logs/final_{run}/ops.txt"
    if not os.path.exists(f):
        return {}
    series = collections.defaultdict(list)
    for ln in open(f):
        p = ln.split()
        if len(p) == 3:
            series[p[1]].append((float(p[0]), int(p[2])))
    out = {}
    for inst, s in series.items():
        s.sort()
        r = []
        for (t0, v0), (t1, v1) in zip(s, s[1:]):
            if t1 > t0 and v1 >= v0:
                r.append((dt.datetime.utcfromtimestamp(t1), (v1 - v0) / (t1 - t0)))
        out[inst] = r
    return out


fig, axs = plt.subplots(2, 1, figsize=(8, 6), sharex=True)
colors = ["#c0392b", "#1f6fb2", "#2e8b57", "#8e44ad"]
for i, (run, label) in enumerate(runs):
    tput, lat, errs, y0 = ycsb(run)
    donors = logs(run, "redis[012]_sg[123]")
    m0 = grep(donors, r"MIGRATE worker: id=\S+ state=PREP")[0][1]
    m1 = grep(donors, r"RAFT.MGN-LOG TXN_DONE logged")[-1][1]
    ts = sorted(tput)
    rel = [(t - m0).total_seconds() for t in ts]

    def w(a, b, d=tput):
        v = [d[t] for t in ts if t in d and a <= (t - m0).total_seconds() < b]
        return sum(v) / len(v) if v else 0.0

    base = w(-25, -8)
    pre = [(r, tput[t]) for r, t in zip(rel, ts) if -12 <= r < 0]
    dip = min(pre, key=lambda x: x[1]) if pre else (None, 0)
    below = sum(1 for r, v in pre if v < 0.8 * base)
    warm = grep(donors, r"MIGRATE-WARM\(async\): registered=.*in (\d+) ms")
    warm_ms = [re.search(r"in (\d+) ms", ln).group(1) + " ms (" + f + ")" for f, t, ln in warm]
    warm_start = grep(donors, r"MIGRATE-WARM: target=")
    print(f"== {label} ({run})")
    print(f"   migration {(m1 - m0).total_seconds():.2f} s; base {base / 1000:.1f} Kops/s; "
          f"pre-migration dip min {dip[1] / 1000:.1f} Kops/s at {dip[0]:+.0f} s; "
          f"{below} s below 80% of base in the 12 s before the migration")
    print(f"   warm-up starts {[round((t - m0).total_seconds(), 2) for f, t, ln in warm_start]} s; "
          f"registration took {warm_ms}")
    print(f"   after (+30..110 s) {w(30, 110) / 1000:.1f} Kops/s ({100 * (w(30, 110) / base - 1):+.1f}%)")
    rates = ops_rates(run)
    if rates:
        tot_pre = tot_post = 0.0
        rows = []
        for inst, name in LEADERS.items():
            s = rates.get(inst, [])
            a = [v for t, v in s if -25 <= (t - m0).total_seconds() < -8]
            b = [v for t, v in s if 30 <= (t - m0).total_seconds() < 110]
            pa = sum(a) / len(a) if a else 0.0
            pb = sum(b) / len(b) if b else 0.0
            tot_pre += pa
            tot_post += pb
            rows.append((name, pa, pb))
        for name, pa, pb in rows:
            print(f"   {name:11s} cmds/s pre {pa / 1000:7.1f}K ({100 * pa / max(tot_pre, 1):4.1f}%)  "
                  f"post {pb / 1000:7.1f}K ({100 * pb / max(tot_post, 1):4.1f}%)")
        print(f"   leaders total cmds/s pre {tot_pre / 1000:.1f}K post {tot_post / 1000:.1f}K "
              f"(client ops/s pre {base / 1000:.1f}K post {w(30, 110) / 1000:.1f}K)")
    axs[0].plot(rel, [tput[t] / 1000 for t in ts], color=colors[i % 4], lw=1.2, label=label)
    for inst, name in LEADERS.items():
        s = rates.get(inst, []) if rates else []
        if s and i == len(runs) - 1:
            axs[1].plot([(t - m0).total_seconds() for t, _ in s], [v / 1000 for _, v in s], lw=1.1, label=name)
axs[0].set_ylabel("client Kops/s")
axs[0].set_ylim(0, 210)
axs[0].legend(fontsize=8, loc="lower right")
axs[0].set_title("30M no-fault: warm-up throttle and per-leader load", fontsize=10, loc="left")
axs[1].set_ylabel(f"commands/s (K)\n({runs[-1][1]})")
axs[1].legend(fontsize=7, loc="upper left", ncol=4)
for ax in axs:
    ax.axvline(0, color="#888", ls=":", lw=1)
    ax.grid(alpha=.25)
axs[-1].set_xlim(-30, 110)
axs[-1].set_xlabel("seconds since migration start (first donor PREP)")
fig.tight_layout()
fig.savefig(f"{here}/figures/final/perf_followup.png", dpi=150)
print("figure:", f"{here}/figures/final/perf_followup.png")
