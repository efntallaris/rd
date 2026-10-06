#!/usr/bin/env python3
"""Final failure campaign (logs/final_*): per-run metrics (metrics_final.json), recovery
evidence (recovery_final.txt) and a throughput grid (figures/final/throughput_grid.png).
Redis log times are UTC-6; YCSB times are UTC."""
import re, glob, json, os, datetime as dt, sys
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
here = sys.argv[1] if len(sys.argv) > 1 else "."
RUNS = ["healthy", "s1", "s2", "s3", "s4", "s5", "s6"]
TS = re.compile(r"(\d+ \w+ \d{4} \d\d:\d\d:\d\d)\.(\d+)")
def rtime(line):
    m = TS.search(line)
    return dt.datetime.strptime(m.group(1), "%d %b %Y %H:%M:%S") + dt.timedelta(hours=6, milliseconds=int(m.group(2)[:3]))
def logs(run, pat="*"):
    return sorted(glob.glob(f"{here}/logs/final_{run}/logs/*/tmp/redis_logs/{pat}.log"))
def grep(files, rx):
    out = []
    for f in files:
        for ln in open(f, errors="ignore"):
            if re.search(rx, ln): out.append((f, ln.rstrip()))
    return out
def ycsb(run):
    per = {}; errs = 0
    for f in glob.glob(f"{here}/logs/final_{run}/ycsb/*/tmp/ycsb_output_ycsb[01]"):
        seen = set()
        lines = open(f, errors="ignore").read().splitlines()
        # A client whose previous run was not cleaned up (failed playbook) appends to the same
        # file: keep only the last run.
        starts = [i for i, ln in enumerate(lines) if ln.startswith("Command line:")]
        for ln in lines[starts[-1] if starts else 0:]:
            m = re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d):\d+ (\d+) sec: \d+ operations; ([\d.]+) current", ln)
            if m and (f, m.group(2)) not in seen:
                seen.add((f, m.group(2))); t = dt.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                per[t] = per.get(t, 0) + float(m.group(3))
            m = re.match(r"\[UPDATE\], Return=ERROR, (\d+)", ln)
            if m: errs += int(m.group(1))
    return per, errs
def kills(run):
    ks = []
    rl = f"{here}/logs/final_{run}/run.log"
    if os.path.exists(rl):
        for m in re.finditer(r"label=(\S+) target=(\S+) pid=\d+ t_arm=[\d.]+ t_kill=([\d.]+)", open(rl, errors="ignore").read()):
            ks.append((m.group(2), dt.datetime.utcfromtimestamp(float(m.group(3)))))
        ks = ks[-1:]
    sk = f"{here}/logs/final_{run}/second_kill.txt"
    if os.path.exists(sk):
        m = re.search(r"(redis\d) .* at (\d\d:\d\d:\d\d\.\d+)", open(sk).read())
        day = ks[0][1].date() if ks else dt.date.today()
        ks.append((m.group(1), dt.datetime.combine(day, dt.datetime.strptime(m.group(2)[:15], "%H:%M:%S.%f").time())))
    return sorted(ks, key=lambda k: k[1])
def playbook_rc(run):
    """The playbook's own result: crash runs print '[run] playbook exited rc=N'; the
    HEALTHY run is judged by the ansible PLAY RECAP (any failed=N>0 means failure)."""
    rl = f"{here}/logs/final_{run}/run.log"
    if not os.path.exists(rl): return None
    txt = open(rl, errors="ignore").read()
    m = re.findall(r"playbook exited rc=(\d+)", txt)
    if m: return int(m[-1])
    return 1 if re.search(r"failed=[1-9]", txt) else 0
rc = {run: playbook_rc(run) for run in RUNS}
DONOR_OF = lambda lo: "sg1" if lo < 5461 else ("sg2" if lo < 10922 else "sg3")
metrics, evidence = {}, []
for run in RUNS:
    sg4 = logs(run, "redis[345]_sg4"); donors = logs(run, "redis[012]_sg[123]"); allf = logs(run)
    committed = sorted({DONOR_OF(int(m.group(1))) for _, l in grep(sg4, r"MGN_INDX_UPD applied")
                        for m in [re.search(r"slots=(\d+)-", l)] if m})
    faked = len(grep(sg4, r"firing MGN_INDX_UPD (anyway|immediately)"))
    crash = len(grep(allf, r"ASSERTION FAILED|REDIS BUG|Crashed by signal|SIGSEGV"))
    flips = [rtime(l) for _, l in grep(donors, r"MIGRATE worker: id=\S+ state=FLIPPING")]
    preps = [rtime(l) for _, l in grep(donors, r"MIGRATE worker: id=\S+ state=PREP")]
    dones = [rtime(l) for _, l in grep(donors, r"RAFT.MGN-LOG TXN_DONE logged")]
    per, errs = ycsb(run); ts = sorted(per)
    if not ts:
        print(f"{run}: no YCSB samples, skipped", file=sys.stderr); continue
    t0 = ts[0]
    _m0 = min(preps) if preps else None
    pre = [per[t] for t in ts if _m0 and -12 <= (t - _m0).total_seconds() <= -2] or \
          [per[t] for t in ts if 25 <= (t - t0).total_seconds() <= 55]
    tail = [per[t] for t in ts if (t - t0).total_seconds() >= 300]
    ks = kills(run)
    mig0 = min(preps) if preps else None; mig1 = max(dones) if dones else None
    lo = min([k[1] for k in ks] + ([mig0] if mig0 else [])) - dt.timedelta(seconds=2)
    hi = max([k[1] for k in ks] + ([mig1] if mig1 else [])) + dt.timedelta(seconds=30)
    prem = sum(pre) / len(pre) if pre else 0
    low = [t for t in ts if lo <= t <= hi and per[t] < 0.5 * prem]
    win = [per[t] for t in ts if lo <= t <= hi]
    rec = {
        "rc": rc.get(run), "kills": [f"{h} @ {t:%H:%M:%S.%f}"[:-3] + " UTC" for h, t in ks],
        "ranges_committed": committed, "faked_indx_upd": faked, "crash_signatures": crash,
        "update_errors": errs, "tput_pre_kops": round(prem / 1000, 1),
        "tput_after_kops": round(sum(tail) / len(tail) / 1000, 1) if tail else None,
        "tput_min_kops": round(min(win) / 1000, 1) if win else None,
        "seconds_below_half": len(low),
        "migration_s": round((mig1 - min(flips)).total_seconds(), 2) if flips and mig1 else None,
        "prep_to_done_s": round((mig1 - mig0).total_seconds(), 2) if mig0 and mig1 else None,
        "become_leader": len(grep(allf, r"AqRaft become-leader: in-flight")),
        "redrives": len({m.group(1) for _, l in grep(donors, r"MGN-RECOVER: role=donor sess=(\d+)")
                         for m in [re.search(r"sess=(\d+)", l)] if m and int(m.group(1)) >= 700}),
        "resumes": len(grep(donors, r"MGN-RECOVER: sess=\d+ RESUMED")),
        "rehomes": len(grep(allf, r"DONOR-REHOME recorded|MGN-DONOR-REHOME")),
        "chain_reforms": len(grep(sg4, r"RE-FORM —")),
        "ae_piggyback_acks": len(grep(sg4, r"AE-piggyback")),
    }
    metrics[run] = rec
    evidence.append(f"===== {run.upper()}  {rec['kills']}")
    for f, l in grep(allf, r"AqRaft become-leader: in-flight|MGN-RECOVER: role=|RESUMED|resume-plan|MGN-RESUME-STATUS:|DONOR-REHOME|RE-FORM —|dropped \d+ dead follower|abandoned after|State change: Node is now a leader"):
        evidence.append(f"{os.path.basename(f)}: {l[20:230]}")
    # throughput panel data
    rec["_series"] = [((t - t0).total_seconds(), per[t] / 1000) for t in ts]
    rec["_kills_rel"] = [(k[1] - t0).total_seconds() for k in ks]
    rec["_mig_rel"] = [((mig0 - t0).total_seconds() if mig0 else None), ((mig1 - t0).total_seconds() if mig1 else None)]
json.dump({k: {a: b for a, b in v.items() if not a.startswith("_")} for k, v in metrics.items()},
          open(f"{here}/figures/final/metrics_final.json", "w"), indent=1)
open(f"{here}/figures/final/recovery_final.txt", "w").write("\n".join(evidence))
TITLES = {"healthy": "HEALTHY · no failure", "s1": "S1 · recipient leader", "s2": "S2 · donor leader (orchestrator)",
          "s3": "S3 · donor follower", "s4": "S4 · recipient follower", "s5": "S5 · donor leader after its transfer",
          "s6": "S6 · donor + recipient leaders"}
for run in [x for x in RUNS if x in metrics]:
    r = metrics[run]; fig, ax = plt.subplots(figsize=(7.2, 2.6))
    xs = [x for x, _ in r["_series"]]; ys = [y for _, y in r["_series"]]
    m0, m1 = r["_mig_rel"]
    if m0 is not None and m1 is not None: ax.axvspan(m0, m1, color="#1f6fb2", alpha=0.10, lw=0)
    ax.plot(xs, ys, color="#1f6fb2", lw=1.3)
    for k in r["_kills_rel"]: ax.axvline(k, color="#c0392b", ls="--", lw=1.1)
    ax.set_xlim(0, 330); ax.set_ylim(0, 210); ax.grid(alpha=.25)
    ax.set_xlabel("seconds since YCSB start"); ax.set_ylabel("Kops/s (both clients)")
    ax.set_title(TITLES[run], fontsize=10, loc="left")
    fig.tight_layout(); fig.savefig(f"{here}/figures/final/tput_{run}.png", dpi=150)
    # zoom on the first 70 s (warm-up, migration, kill) with 5 s ticks
    ax.set_xlim(0, 70); ax.set_xticks(range(0, 71, 5)); ax.set_title(TITLES[run] + " · first 70 s", fontsize=10, loc="left")
    fig.savefig(f"{here}/figures/final/tput_{run}_70s.png", dpi=150); plt.close(fig)
runs_ok = [x for x in RUNS if x in metrics]
fig, axs = plt.subplots(len(runs_ok), 1, figsize=(7.2, 1.7 * len(runs_ok)), sharex=True)
for ax, run in zip(axs, runs_ok):
    r = metrics[run]; m0, m1 = r["_mig_rel"]
    if m0 is not None and m1 is not None: ax.axvspan(m0, m1, color="#1f6fb2", alpha=0.10, lw=0)
    ax.plot([x for x, _ in r["_series"]], [y for _, y in r["_series"]], color="#1f6fb2", lw=1.2)
    for k in r["_kills_rel"]: ax.axvline(k, color="#c0392b", ls="--", lw=1.0)
    ax.set_xlim(0, 70); ax.set_ylim(0, 210); ax.set_yticks([0, 100, 200]); ax.grid(alpha=.25)
    ax.set_title(TITLES[run], fontsize=9, loc="left", pad=2)
axs[-1].set_xticks(range(0, 71, 5)); axs[-1].set_xlabel("seconds since YCSB start")
fig.text(0.01, 0.5, "Kops/s (both clients)", rotation=90, va="center")
fig.tight_layout(rect=(0.03, 0, 1, 1)); fig.savefig(f"{here}/figures/final/tput_all_70s.png", dpi=150); plt.close(fig)
print("ok")
import subprocess
for run in RUNS:
    subprocess.run([sys.executable, os.path.join(here, "..", "tools", "plot_phase_gantt.py"),
                    f"{here}/logs/final_{run}", f"{here}/figures/final/gantt_{run}.png"],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
