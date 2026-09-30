#!/usr/bin/env python3
"""After-fix results page: figures/recov/{recovery_phases.png, tput_after_70s.png,
tput_after_full.png, aqraft_recovery_fixes.html}. Runs: logs/recov_{s1,s4,s6} (recovery-fix
build) and logs/stall_{healthy,s3} (follower-pool build). Phase times are seconds after the
(first) kill, read from the redis logs of logs/recov_*."""
import base64, glob, json, os, re, sys, datetime as dt
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
from matplotlib.patches import Patch
here = sys.argv[1] if len(sys.argv) > 1 else "."
R = os.path.join(here, "figures", "recov")
TZ = dt.timedelta(hours=6)            # redis logs are UTC-6, YCSB is UTC

# ---- recovery phases (after the fixes) -------------------------------------
PH = [("S1 · recipient leader", 1.44, 5.58, 5.72, 20.59, [("sg3", 18.30), ("sg2", 20.59)]),
      ("S6 · donor + recipient leaders", 1.63, 5.77, 5.88, 12.93, [("sg3", 9.36), ("sg2", 12.93)])]
C = {"elect": "#2C58A0", "merge": "#A0650F", "send": "#217A3C"}
fig, ax = plt.subplots(figsize=(9, 2.4))
for i, (lab, el, me, s0, done, marks) in enumerate(PH):
    ax.barh(i, el, color=C["elect"], height=0.5)
    ax.barh(i, me - el, left=el, color=C["merge"], height=0.5)
    ax.barh(i, done - s0, left=s0, color=C["send"], height=0.5)
    for name, t in marks:
        ax.plot(t, i, marker="|", color="#17202E", ms=14, mew=1.5)
        ax.text(t, i - 0.36, name, ha="center", fontsize=8)
    ax.text(done + 0.3, i, f"{done:.1f} s", va="center", fontsize=10, fontweight="bold")
ax.set_yticks(range(len(PH))); ax.set_yticklabels([p[0] for p in PH]); ax.invert_yaxis()
ax.set_xlim(0, 25); ax.set_xticks(range(0, 26, 2)); ax.set_xlabel("seconds after the (first) kill")
ax.grid(axis="x", alpha=.3); ax.set_ylim(1.6, -0.7)
ax.legend(handles=[Patch(color=C["elect"], label="kill → new sg4 leader"),
                   Patch(color=C["merge"], label="blocking held-block merge"),
                   Patch(color=C["send"], label="donor re-send → durable")],
          fontsize=8, loc="lower right", frameon=False)
fig.tight_layout(); fig.savefig(os.path.join(R, "recovery_phases.png"), dpi=150); plt.close(fig)

# ---- throughput, after-fix runs only ----------------------------------------
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

def load(d):
    per = ycsb(d); ts = sorted(per); t0 = next(t for t in ts if per[t] > 0)
    day = (ts[0] - TZ).date()
    s = open(f"{d}/migration_window.txt", errors="ignore").read()
    m = re.search(r"FULL.*?\[(\d\d:\d\d:\d\d\.\d+) -> (\d\d:\d\d:\d\d\.\d+)\]", s)
    f = lambda x: dt.datetime.combine(day, dt.datetime.strptime(x, "%H:%M:%S.%f").time()) + TZ
    m0, m1 = f(m.group(1)), f(m.group(2))
    kills = []
    if os.path.exists(f"{d}/kill.txt"):
        k = re.search(r"t_kill=([\d.]+)", open(f"{d}/kill.txt").read())
        if k: kills.append(dt.datetime.utcfromtimestamp(float(k.group(1))))
    if os.path.exists(f"{d}/second_kill.txt"):
        k = re.search(r"at (\d\d:\d\d:\d\d\.\d{6})", open(f"{d}/second_kill.txt").read())
        if k: kills.append(dt.datetime.combine(ts[0].date(), dt.datetime.strptime(k.group(1), "%H:%M:%S.%f").time()))
    rel = lambda t: (t - t0).total_seconds()
    pre = [per[t] for t in ts if m0 - dt.timedelta(seconds=30) <= t <= m0 - dt.timedelta(seconds=5)]
    base = sum(pre) / len(pre)
    win = [t for t in ts if m0 - dt.timedelta(seconds=3) <= t <= m1 + dt.timedelta(seconds=20)]
    return dict(x=[rel(t) for t in ts], y=[per[t] for t in ts], m=(rel(m0), rel(m1)),
                k=[rel(k) for k in kills], below=sum(1 for t in win if per[t] < 0.5 * base),
                low=round(min(per[t] for t in win), 1), base=round(base, 1),
                post=round(sum(per[t] for t in ts if t > m1 + dt.timedelta(seconds=20)) /
                           max(1, sum(1 for t in ts if t > m1 + dt.timedelta(seconds=20))), 1))

RUNS = [("healthy", "stall", "HEALTHY · no failure"), ("s3", "stall", "S3 · donor follower killed"),
        ("s4", "recov", "S4 · recipient follower killed"), ("s1", "recov", "S1 · recipient leader killed"),
        ("s6", "recov", "S6 · donor + recipient leaders killed")]
res = {r: load(f"{here}/logs/{w}_{r}") for r, w, _ in RUNS}
json.dump({r: {k: v for k, v in res[r].items() if k not in ("x", "y")} for r in res},
          open(os.path.join(R, "after_metrics.json"), "w"), indent=1)
for r in res: print(r, {k: v for k, v in res[r].items() if k not in ("x", "y", "k", "m")})
for name, xmax in (("tput_after_70s", 75), ("tput_after_full", 330)):
    fig, axs = plt.subplots(len(RUNS), 1, figsize=(8, 1.75 * len(RUNS)), sharex=True)
    for ax, (r, w, title) in zip(axs, RUNS):
        v = res[r]
        ax.axvspan(*v["m"], color="#1f6fb2", alpha=0.10, lw=0)
        ax.plot(v["x"], v["y"], color="#1f6fb2", lw=1.3)
        for k in v["k"]: ax.axvline(k, color="#c0392b", ls="--", lw=1)
        ax.set_xlim(0, xmax); ax.set_ylim(0, 215); ax.set_yticks([0, 100, 200]); ax.grid(alpha=.25)
        ax.set_title(f"{title}   ·   below half: {v['below']} s   ·   lowest: {v['low']:.0f} Kops/s",
                     fontsize=9, loc="left", pad=2)
    if xmax <= 80: axs[-1].set_xticks(range(0, 76, 5))
    axs[-1].set_xlabel("seconds since YCSB start")
    fig.text(0.01, 0.5, "Kops/s (both clients)", rotation=90, va="center")
    fig.tight_layout(rect=(0.03, 0, 1, 1)); fig.savefig(os.path.join(R, name + ".png"), dpi=150); plt.close(fig)

def img(p, alt):
    b = base64.b64encode(open(p, "rb").read()).decode()
    return f'<img src="data:image/png;base64,{b}" alt="{alt}">'
page = open(os.path.join(here, "recovery_fixes_template.html")).read()
for k, fn, alt in (("PHASES", "recovery_phases.png", "Recovery phases"),
                   ("T70", "tput_after_70s.png", "Throughput, first 75 s"),
                   ("TFULL", "tput_after_full.png", "Throughput, whole run")):
    page = page.replace("{{" + k + "}}", img(os.path.join(R, fn), alt))
for r in res:
    page = page.replace("{{BELOW_%s}}" % r, str(res[r]["below"])).replace("{{LOW_%s}}" % r, f"{res[r]['low']:.0f}")
out = os.path.join(R, "aqraft_recovery_fixes.html"); open(out, "w").write(page)
print("wrote", out, f"{os.path.getsize(out)/1e6:.1f} MB")
