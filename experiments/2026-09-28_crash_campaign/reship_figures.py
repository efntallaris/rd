#!/usr/bin/env python3
"""Run-1 figures for the re-ship build (logs/reship_{s1,s2,s6}): client throughput with the
kill and recovery time, and each donor transfer's time split into block registration vs
waiting for RDMA completions, marked by the network the link used."""
import glob, os, re, sys, datetime as dt
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
from matplotlib.patches import Patch
here = sys.argv[1] if len(sys.argv) > 1 else "."
OUT = os.path.join(here, "figures", "reship"); TZ = dt.timedelta(hours=6)
RUNS = [("s1", "S1 · recipient leader killed", 22.6), ("s2", "S2 · donor leader killed", 26.7),
        ("s6", "S6 · donor + recipient leaders killed", 12.7)]

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

fig, axs = plt.subplots(3, 1, figsize=(8, 6.2), sharex=True)
for ax, (r, title, rec) in zip(axs, RUNS):
    d = f"{here}/logs/reship_{r}"; per = ycsb(d); ts = sorted(per); t0 = next(t for t in ts if per[t] > 0)
    kills = [dt.datetime.utcfromtimestamp(float(re.search(r"t_kill=([\d.]+)", open(f"{d}/kill.txt").read()).group(1)))]
    if os.path.exists(f"{d}/second_kill.txt"):
        k = re.search(r"at (\d\d:\d\d:\d\d\.\d{6})", open(f"{d}/second_kill.txt").read())
        kills.append(dt.datetime.combine(ts[0].date(), dt.datetime.strptime(k.group(1), "%H:%M:%S.%f").time()))
    k0 = min(kills); rel = lambda t: (t - t0).total_seconds()
    ax.axvspan(rel(k0), rel(k0) + rec, color="#1f6fb2", alpha=0.10, lw=0)
    ax.plot([rel(t) for t in ts], [per[t] for t in ts], color="#1f6fb2", lw=1.3)
    for k in kills: ax.axvline(rel(k), color="#c0392b", ls="--", lw=1)
    ax.set_xlim(0, 75); ax.set_ylim(0, 215); ax.set_yticks([0, 100, 200]); ax.grid(alpha=.25)
    ax.set_title(f"{title}   ·   all ranges durable {rec:.1f} s after the kill (shaded)", fontsize=9, loc="left", pad=2)
axs[-1].set_xticks(range(0, 76, 5)); axs[-1].set_xlabel("seconds since YCSB start")
fig.text(0.01, 0.5, "Kops/s (both clients)", rotation=90, va="center")
fig.tight_layout(rect=(0.03, 0, 1, 1)); fig.savefig(os.path.join(OUT, "tput_reship.png"), dpi=150); plt.close(fig)

# per-transfer breakdown
rows = []   # (run, label, reg_s, reap_s, total_s, network)
for r, title, _ in RUNS:
    for f in sorted(glob.glob(f"{here}/logs/reship_{r}/logs/redis[012]/tmp/redis_logs/redis?_sg?.log")):
        sg = re.search(r"_(sg\d)\.log", f).group(1); link = None; cur = None
        for l in open(f, errors="ignore"):
            m = re.search(r"new outbound link to (\S+) ", l)
            if m: link = m.group(1)
            m = re.search(r"TRANSFER chunk seq=(\d+) slots=\d+ wall=(\d+)ms reg=(\d+)ms \((\d+) new MRs\) reap=(\d+)ms", l)
            if m:
                seq, wall, reg, nmr, reap = map(int, m.groups())
                if seq == 0: cur = [0, 0, 0, 0, link]
                if cur is None: continue
                cur[0] += reg; cur[1] += reap; cur[2] += wall; cur[3] += nmr
            m = re.search(r"TRANSFER \(overlap\) finished .*transfer_ms=(\d+)", l)
            if m and cur is not None:
                net = "control network" if (cur[4] and ".entall" in cur[4]) else "experiment network"
                rows.append((r.upper(), f"{r.upper()} {sg} → {(cur[4] or '?') if re.match(r'^[0-9.]+$', cur[4] or '') else (cur[4] or '?').split('.')[0]}", cur[0] / 1000, cur[1] / 1000,
                             int(m.group(1)) / 1000, net, cur[3])); cur = None
fig, ax = plt.subplots(figsize=(8.5, 0.42 * len(rows) + 1.2))
for i, (run, lab, reg, reap, tot, net, nmr) in enumerate(rows):
    ax.barh(i, reg, color="#A0650F", height=0.6)
    ax.barh(i, reap, left=reg, color="#2C58A0" if net == "experiment network" else "#c0392b", height=0.6)
    ax.text(tot + 0.2, i, f"{tot:.1f} s · {nmr} MRs registered in-window", va="center", fontsize=8)
ax.set_yticks(range(len(rows))); ax.set_yticklabels([r[1] for r in rows], fontsize=8); ax.invert_yaxis()
ax.set_xlabel("donor TRANSFER time (s)"); ax.grid(axis="x", alpha=.3); ax.set_xlim(0, max(r[4] for r in rows) * 1.45)
ax.legend(handles=[Patch(color="#A0650F", label="registering blocks"),
                   Patch(color="#2C58A0", label="waiting for RDMA completions (experiment network)"),
                   Patch(color="#c0392b", label="waiting for RDMA completions (control network: hostname bug)")],
          fontsize=7.5, loc="lower right", frameon=False)
fig.tight_layout(); fig.savefig(os.path.join(OUT, "transfer_breakdown.png"), dpi=150); plt.close(fig)
for r in rows: print(r)
