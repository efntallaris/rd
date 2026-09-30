#!/usr/bin/env python3
"""Per leader-crash run: kill -> new leader elected, -> first recovery action, -> all ranges
durable (last donor TXN_DONE), plus client impact. Writes figures/final/recovery_times.{json,png}."""
import glob, json, os, re, sys, datetime as dt
here = sys.argv[1] if len(sys.argv) > 1 else "."
M = json.load(open(f"{here}/figures/final/metrics_final.json"))
TZ = dt.timedelta(hours=6)                       # redis logs are UTC-6
RUNS = {"s1": ("S1", "recipient leader", ["sg4"]), "s2": ("S2", "donor leader, mid-transfer", ["sg1"]),
        "s5": ("S5", "donor leader, after transfer", ["sg1"]), "s6": ("S6", "donor + recipient leaders", ["sg1", "sg4"])}
def ts(line, day):
    m = re.search(r"(\d+ \w{3} \d{4} \d\d:\d\d:\d\d\.\d+)", line)
    return dt.datetime.strptime(m.group(1), "%d %b %Y %H:%M:%S.%f") + TZ if m else None
def events(run, pat, files="*"):
    out = []
    for f in glob.glob(f"{here}/logs/final_{run}/logs/*/tmp/redis_logs/{files}.log"):
        for ln in open(f, errors="ignore"):
            if re.search(pat, ln):
                t = ts(ln, None)
                if t: out.append((t, os.path.basename(f), ln.strip()))
    return sorted(out)
res = {}
for run, (tag, what, groups) in RUNS.items():
    m = M[run]; day = None
    kills = []
    for k in m["kills"]:
        h, t = re.match(r"(\S+) @ (\S+)", k).groups()
        kills.append(t)
    # kill date from the first log line
    any_ev = events(run, r"Node is now a leader")
    d0 = any_ev[0][0].date()
    kt = [dt.datetime.combine(d0, dt.datetime.strptime(t, "%H:%M:%S.%f").time()) for t in kills]
    k0, k1 = min(kt), max(kt)
    elect = {}
    for g in groups:
        ev = [e for e in events(run, r"Node is now a leader", f"*_{g}") if e[0] > k0]
        if ev: elect[g] = round((ev[0][0] - k0).total_seconds(), 2)
    rec = [e for e in events(run, r"MGN-RECOVER: role=|DONOR-REHOME recorded|MGN-RESUME-STATUS") if e[0] > k0]
    done = [e for e in events(run, r"MGN-LOG TXN_DONE logged: sess=\d+$", "redis[012]_sg[123]") if e[0] > k0]
    res[run] = dict(tag=tag, what=what, kills=kills,
        new_leader_s=elect,
        first_recovery_s=round((rec[0][0] - k0).total_seconds(), 2) if rec else None,
        all_durable_s=round((done[-1][0] - k0).total_seconds(), 2) if done else None,
        seconds_below_half=m["seconds_below_half"], update_errors=m["update_errors"])
    print(run, res[run])
json.dump(res, open(f"{here}/figures/final/recovery_times.json", "w"), indent=1)

import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
fig, ax = plt.subplots(figsize=(8, 3.2))
names = [f"{r['tag']} · {r['what']}" for r in res.values()]
for i, r in enumerate(res.values()):
    el = max(r["new_leader_s"].values()) if r["new_leader_s"] else 0
    fr = r["first_recovery_s"] or el; ad = r["all_durable_s"] or fr
    ax.barh(i, el, color="#3b5bdb", height=0.5, label="kill → new leader" if i == 0 else None)
    ax.barh(i, max(fr - el, 0), left=el, color="#e8a33d", height=0.5, label="→ first recovery action" if i == 0 else None)
    ax.barh(i, max(ad - fr, 0), left=max(fr, el), color="#9aa5b1", height=0.5, label="→ all ranges durable" if i == 0 else None)
    ax.text(ad + 0.8, i, f"{ad:.1f} s", va="center", fontsize=9)
ax.set_yticks(range(len(names))); ax.set_yticklabels(names, fontsize=9); ax.invert_yaxis()
ax.set_xlabel("seconds after the (first) kill"); ax.legend(fontsize=8, loc="lower right", frameon=False)
ax.set_xlim(0, max(r["all_durable_s"] or 0 for r in res.values()) * 1.15); ax.grid(axis="x", alpha=.3)
fig.tight_layout(); fig.savefig(f"{here}/figures/final/recovery_times.png", dpi=150)
