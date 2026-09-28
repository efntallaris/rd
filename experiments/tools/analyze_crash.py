#!/usr/bin/env python3
"""
analyze_crash.py — per-scenario metrics + figures for an AqRaft crash campaign.

Usage:
  python3 analyze_crash.py [RESULTS_DIR] [--pre LO HI] [--plateau LO HI] [--mig SEC]

RESULTS_DIR defaults to /tmp/crash_inject/campaign (the run_all_scenarios.sh output).
Reads each scenario subdir's ycsb_ycsb{0,1}.txt + run.log + redis*.log, writes:
  - <dir>/<S>/fig.png       per-scenario throughput timeline
  - <dir>/metrics.json      all metrics (consumed by build_artifact.py)
"""
import re, os, sys, json, glob
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt

args = [a for a in sys.argv[1:] if not a.startswith("--")]
RES = args[0] if args else "/tmp/crash_inject/campaign"
def opt(name, n, default):
    if name in sys.argv:
        i = sys.argv.index(name); return [type(default[k])(sys.argv[i+1+k]) for k in range(n)]
    return default
PRE     = tuple(opt("--pre", 2, [25, 55]))
PLATEAU = tuple(opt("--plateau", 2, [200, 550]))
MIG     = opt("--mig", 1, [60])[0]

META = {
 "HEALTHY": ("Healthy (no crash)",      "reference — clean reshard",              "#3b5bdb"),
 "S1": ("Recipient LEADER crash",       "kill sg4 leader redis3 mid-transfer",    "#d6336c"),
 "S2": ("Donor LEADER crash",           "kill sg1 leader redis0 mid-transfer",    "#7048e8"),
 "S3": ("Donor FOLLOWER crash",         "kill sg1 follower redis1 mid-transfer",  "#e8890c"),
 "S4": ("Recipient FOLLOWER crash",     "kill sg4 chain follower redis4",         "#2f9e44"),
 "S5": ("Donor LEADER crash AFTER xfer","kill sg1 leader redis0 after its DONE",  "#0ca678"),
}
LINE = re.compile(r"(\d+)\s+sec:\s+\d+\s+operations;\s+([0-9.]+)\s+current")

def series(d):
    a = {}
    for c in ("ycsb0", "ycsb1"):
        p = os.path.join(d, f"ycsb_{c}.txt")
        if not os.path.exists(p): continue
        for ln in open(p, errors="ignore"):
            m = LINE.search(ln)
            if m: a[int(m.group(1))] = a.get(int(m.group(1)), 0) + float(m.group(2))
    return dict(sorted(a.items()))

def mean(s, lo, hi):
    v = [o for t, o in s.items() if lo <= t <= hi and o > 0]; return sum(v)/len(v) if v else 0.0

def ttf(s, plat):
    thr = 0.95*plat; run = 0
    for t in sorted(s):
        if t < MIG: continue
        if s[t] >= thr:
            run += 1
            if run >= 5: return (t-4)-MIG
        else: run = 0
    return None

def readtxt(*paths): return "".join(open(p, errors="ignore").read() for p in paths if os.path.exists(p))

results = {}
for S, (name, target, color) in META.items():
    d = os.path.join(RES, S)
    if not os.path.isdir(d): continue
    s = series(d)
    if not s: continue
    runlog = readtxt(os.path.join(d, "run.log"))
    logs   = readtxt(*[os.path.join(d, f"{h}_sg4.log") for h in ("redis3", "redis4", "redis5")])
    pre, plat = mean(s, *PRE), mean(s, *PLATEAU)
    rise = (plat/pre - 1)*100 if pre else 0
    mw = re.search(r"FULL\s+\(first TXN_START -> last TXN_DONE\):\s+([0-9.]+)s", runlog)
    uerr = re.findall(r"UPDATE-err=(\d+)", runlog)
    crash = len(re.findall(r"SIGSEGV|Crashed by signal|ASSERTION FAILED|REDIS BUG", logs))
    reform = len(re.findall(r"RE-FORM", logs))
    elect  = len([t for t in re.findall(r"now a leader,? term (\d+)", logs) if int(t) >= 2])
    dbsz   = re.findall(r"DBSIZE[^\n]*?([0-9]{5,})", runlog)
    results[S] = dict(name=name, target=target, color=color, pre=pre, plateau=plat, rise=rise,
                      t2f=ttf(s, plat), migwin=(float(mw.group(1)) if mw else None),
                      upd_err=(sum(int(x) for x in uerr) if uerr else None),
                      crash=crash, reform=reform, elections=elect,
                      dbsize=(int(dbsz[-1]) if dbsz else None), end=(max(s) if s else 0))
    fig, ax = plt.subplots(figsize=(11, 3.4))
    ax.plot(list(s), list(s.values()), lw=1.1, color=color)
    ax.axvspan(*PRE, color="#3b5bdb", alpha=0.05); ax.axvspan(MIG, MIG+30, color="#e0891c", alpha=0.13)
    ax.axvspan(*PLATEAU, color="#2f9e44", alpha=0.05)
    ax.axhline(pre, ls="--", lw=0.8, color="#888"); ax.axhline(plat, ls="--", lw=0.8, color=color)
    ax.set_xlim(min(s, default=0), results[S]["end"]); ax.set_ylim(bottom=0)
    ax.set_xlabel("YCSB elapsed (s)"); ax.set_ylabel("ops/sec (2 clients)")
    t2 = results[S]["t2f"]
    ax.set_title(f"{S} — {name}   ({rise:+.0f}% rise" + (f", full in {t2}s)" if t2 is not None else ")"), fontsize=10)
    ax.grid(alpha=0.2); fig.tight_layout(); fig.savefig(os.path.join(d, "fig.png"), dpi=104); plt.close(fig)
    print(f"{S}: rise={rise:+.1f}% pre={pre:,.0f} plat={plat:,.0f} t2f={results[S]['t2f']} "
          f"crash={crash} upd_err={results[S]['upd_err']} dbsize={results[S]['dbsize']}")

json.dump(results, open(os.path.join(RES, "metrics.json"), "w"), indent=2)
print("wrote", os.path.join(RES, "metrics.json"), "for", list(results))
