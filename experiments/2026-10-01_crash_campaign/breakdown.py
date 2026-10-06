#!/usr/bin/env python3
"""Client-performance + time breakdown for the 2026-10-01 crash campaign.

Usage: breakdown.py <campaign dir>     (reads logs/final_<run>/, writes figures/final/ + breakdown.md)

Per run:
  client   throughput before / min / after, seconds below 50% and 90% of the baseline,
           time back to 90%, ops lost vs. baseline, READ/UPDATE avg latency (before,
           worst second, after), YCSB errors
  phases   warm-up (MIGRATE-WARM / CHAIN-WARM), then per donor TXN_START -> FLIPPING ->
           TRANSFER -> transfer finished -> TXN_DONE, recipient chain ack and range commit
           (MGN_RECP_TXN_DONE), first PREP -> last TXN_DONE
Redis log times are UTC-6, YCSB times UTC; everything is reported in seconds relative to
the first donor PREP (the start of the migration) unless said otherwise.
"""
import datetime as dt
import glob
import json
import os
import re
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

here = sys.argv[1] if len(sys.argv) > 1 else "."
RUNS = ["healthy", "s1", "s2", "s3", "s4", "s5"]
TITLES = {"healthy": "No fault", "s1": "S1 recipient leader", "s2": "S2 donor leader (mid-transfer)",
          "s3": "S3 donor follower", "s4": "S4 recipient follower", "s5": "S5 donor leader after its transfer"}
TZ = dt.timedelta(hours=6)
TS = re.compile(r"(\d+ \w{3} \d{4} \d\d:\d\d:\d\d)\.(\d+)")


def rtime(line):
    m = TS.search(line)
    if not m:
        return None
    return dt.datetime.strptime(m.group(1), "%d %b %Y %H:%M:%S") + TZ + dt.timedelta(milliseconds=int(m.group(2)[:3]))


def logs(run, pat="*"):
    return sorted(glob.glob(f"{here}/logs/final_{run}/logs/*/tmp/redis_logs/{pat}.log"))


def grep(files, rx):
    r = re.compile(rx)
    out = []
    for f in files:
        for ln in open(f, errors="ignore"):
            if r.search(ln):
                t = rtime(ln)
                if t:
                    out.append((os.path.basename(f)[:-4], t, ln.rstrip()))
    return sorted(out, key=lambda x: x[1])


def in_run(events, m0, m1):
    """Drop lines from a previous run's log tail (the grabber starts before teardown)."""
    if m0 is None:
        return events
    hi = (m1 or m0) + dt.timedelta(seconds=180)
    return [e for e in events if m0 - dt.timedelta(seconds=60) <= e[1] <= hi]


def ycsb(run):
    tput, lat = {}, {"READ": {}, "UPDATE": {}}
    errs = {"READ": 0, "UPDATE": 0}
    starts = []
    for f in sorted(glob.glob(f"{here}/logs/final_{run}/ycsb/*/tmp/ycsb_output_ycsb[01]")):
        lines = open(f, errors="ignore").read().splitlines()
        cmd = [i for i, ln in enumerate(lines) if ln.startswith("Command line:")]
        lines = lines[cmd[-1] if cmd else 0:]
        t0 = None
        seen = set()
        for ln in lines:
            m = re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d):\d+ (\d+) sec: \d+ operations; ([\d.]+) current", ln)
            if m and m.group(2) not in seen:
                seen.add(m.group(2))
                t = dt.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                if t0 is None:
                    t0 = t - dt.timedelta(seconds=int(m.group(2)))
                tput[t] = tput.get(t, 0.0) + float(m.group(3))
                # per-interval latency printed on the (wall-clock) status line; the
                # "[READ], <ms>, <us>" timeseries rows are offset from wall clock
                for k, v in re.findall(r"\[(READ|UPDATE) AverageLatency\(us\)=([\d.]+)\]", ln):
                    lat[k].setdefault(t, []).append(float(v))
            m = re.match(r"\[(READ|UPDATE)\], Return=ERROR, (\d+)", ln)
            if m:
                errs[m.group(1)] += int(m.group(2))
        if t0:
            starts.append(t0)
    lat = {k: {t: sum(v) / len(v) for t, v in d.items()} for k, d in lat.items()}
    return tput, lat, errs, (min(starts) if starts else None)


def kill_time(run):
    rl = f"{here}/logs/final_{run}/run.log"
    if not os.path.exists(rl):
        return None, None
    m = re.search(r"target=(\S+) pid=\d+ t_arm=[\d.]+ t_kill=([\d.]+)", open(rl, errors="ignore").read())
    if not m:
        return None, None
    return m.group(1), dt.datetime.utcfromtimestamp(float(m.group(2)))


def avg(xs):
    return sum(xs) / len(xs) if xs else None


def rel(t, t0):
    return None if t is None or t0 is None else round((t - t0).total_seconds(), 2)


res = {}
for run in RUNS:
    donors = logs(run, "redis[012]_sg[123]")
    sg4 = logs(run, "redis[345]_sg4")
    allf = logs(run)
    tput, lat, errs, y0 = ycsb(run)
    if not tput:
        print(f"{run}: no YCSB data", file=sys.stderr)
        continue
    preps = grep(donors, r"MIGRATE worker: id=\S+ state=PREP")
    m0 = preps[0][1] if preps else None
    dones = grep(donors, r"RAFT.MGN-LOG TXN_DONE logged")
    m1 = dones[-1][1] if dones else None
    warm = in_run(grep(allf, r"MIGRATE-WARM|CHAIN-WARM|warm-pin: pinned|prereg|REGISTER-BLOCK-SLOTS: aqueduct big-MR pool"), m0, m1)
    killed, tk = kill_time(run)

    ts = sorted(tput)
    base = avg([tput[t] for t in ts if m0 and -25 <= (t - m0).total_seconds() <= -8])
    # warm-up dip: lowest second in the 10 s before the migration starts
    pre_win = [t for t in ts if m0 and -10 <= (t - m0).total_seconds() < 0]
    warm_dip = min((tput[t] for t in pre_win), default=None)
    win = [t for t in ts if m0 and 0 <= (t - m0).total_seconds() <= (rel(m1, m0) or 0) + 15]
    post = [tput[t] for t in ts if m1 and 20 <= (t - m1).total_seconds() <= 100]
    below50 = sum(1 for t in win if tput[t] < 0.5 * base)
    below90 = sum(1 for t in win if tput[t] < 0.9 * base)
    back90 = None
    if tk:
        after = [t for t in ts if t >= tk]
        low = [t for t in after if tput[t] < 0.9 * base and (t - tk).total_seconds() < 60]
        back90 = round((max(low) - tk).total_seconds() + 1, 1) if low else 0.0
    lost = sum(max(0.0, base - tput[t]) for t in win)

    def latwin(kind, lo, hi):
        d = lat[kind]
        return [v for t, v in d.items() if m0 and lo <= (t - m0).total_seconds() <= hi]

    L = {}
    for kind in ("READ", "UPDATE"):
        L[kind] = {
            "pre_us": round(avg(latwin(kind, -25, -8)) or 0),
            "worst_us": round(max(latwin(kind, 0, (rel(m1, m0) or 0) + 15), default=0)),
            "post_us": round(avg([v for t, v in lat[kind].items() if m1 and 20 <= (t - m1).total_seconds() <= 100]) or 0),
        }

    # per-donor phase timeline (relative to m0)
    phases = {}
    last_id = {}   # per log file: the worker that logged last (transfer-finished lines carry no id)
    for f, t, ln in in_run(grep(donors, r"MIGRATE worker: id=(\S+) state=(PREP|FLIPPING|TRANSFER|BACKPATCH)|TRANSFER \(overlap\) finished|TXN_DONE logged|TXN_START logged"), m0, m1):
        sg = f.split("_")[-1]
        m = re.search(r"id=(\S+) state=(\w+)", ln)
        if m:
            key, mid = m.group(2), m.group(1)
            last_id[f] = mid
        elif "finished" in ln:
            key, mid = "transfer_done", last_id.get(f, "")
        else:
            key = "TXN_DONE" if "TXN_DONE" in ln else "TXN_START"
            ms = re.search(r"sess=(\d+)", ln)
            mid = ms.group(1) if ms else ""
        tag = f"{sg}{'(re-ship)' if mid.startswith('8') else ''}"
        phases.setdefault(tag, {}).setdefault(key, rel(t, m0))
    rec = {}
    for f, t, ln in in_run(grep(sg4, r"chain-ack observed|MGN_RECP_TXN_DONE applied|RAFT.MGN-LOG RECP_TXN_DONE logged"), m0, m1):
        m = re.search(r"slots=(\d+)-(\d+)", ln)
        if "chain-ack" in ln:
            rec.setdefault("chain_acks", []).append(rel(t, m0))
        elif m and ("logged" in ln or "adopted=1" in ln):
            # first commit of each range (a promoted leader's adopt commits via the log too)
            rec.setdefault("commits", {}).setdefault(f"{m.group(1)}-{m.group(2)}", rel(t, m0))

    res[run] = {
        "killed": killed, "kill_s": rel(tk, m0),
        "migration_s": rel(m1, m0),
        "warmup_first_s": rel(warm[0][1], m0) if warm else None,
        "tput_base_kops": round(base / 1000, 1) if base else None,
        "warmup_dip_kops": round(warm_dip / 1000, 1) if warm_dip is not None else None,
        "tput_min_kops": round(min((tput[t] for t in win), default=0) / 1000, 1),
        "tput_after_kops": round(avg(post) / 1000, 1) if post else None,
        "s_below_50": below50, "s_below_90": below90, "back_to_90_s": back90,
        "ops_lost_k": round(lost / 1000, 1),
        "latency": L, "errors": errs,
        "phases": phases, "recipient": rec,
        "_tput": [((t - m0).total_seconds(), tput[t] / 1000) for t in ts] if m0 else [],
        "_lat": {k: sorted(((t - m0).total_seconds(), v / 1000) for t, v in lat[k].items()) for k in lat} if m0 else {},
    }

os.makedirs(f"{here}/figures/final", exist_ok=True)
json.dump({k: {a: b for a, b in v.items() if not a.startswith("_")} for k, v in res.items()},
          open(f"{here}/figures/final/breakdown.json", "w"), indent=1)

# ---- figure: throughput + READ/UPDATE latency per run, around the migration ----
runs = [r for r in RUNS if r in res]
fig, axs = plt.subplots(len(runs), 2, figsize=(11, 1.9 * len(runs)), sharex=True)
for i, run in enumerate(runs):
    r = res[run]
    a, b = axs[i][0], axs[i][1]
    xs = [x for x, _ in r["_tput"]]
    a.plot(xs, [y for _, y in r["_tput"]], color="#1f6fb2", lw=1.2)
    for k, col in (("READ", "#1f6fb2"), ("UPDATE", "#d9822b")):
        b.plot([x for x, _ in r["_lat"][k]], [y for _, y in r["_lat"][k]], color=col, lw=1.1, label=k.lower())
    for ax in (a, b):
        if r["migration_s"] is not None:
            ax.axvspan(0, r["migration_s"], color="#1f6fb2", alpha=0.08, lw=0)
        if r["kill_s"] is not None:
            ax.axvline(r["kill_s"], color="#c0392b", ls="--", lw=1)
        ax.grid(alpha=.25)
    a.set_xlim(-20, 40)
    a.set_ylim(0, 210)
    b.set_ylim(0, None)
    a.set_title(TITLES[run], fontsize=9, loc="left", pad=2)
    if i == 0:
        b.legend(fontsize=7, loc="upper right")
        a.set_ylabel("Kops/s")
        b.set_ylabel("avg latency (ms)")
axs[-1][0].set_xlabel("seconds since migration start (first donor PREP)")
axs[-1][1].set_xlabel("seconds since migration start (first donor PREP)")
fig.tight_layout()
fig.savefig(f"{here}/figures/final/client_breakdown.png", dpi=150)
plt.close(fig)

# ---- markdown summary ----
o = ["# 30M crash campaign — client performance and time breakdown\n",
     "Seconds are relative to the migration start (first donor `PREP`). Baseline = mean "
     "throughput 25–8 s before the migration. \"after\" = 20–100 s after the last `TXN_DONE`.\n",
     "## Client impact\n",
     "| run | kill (s) | migration (s) | base Kops/s | min | after | s <50% | s <90% | back to 90% after kill (s) | Kops lost | READ avg µs pre / worst / after | UPDATE avg µs pre / worst / after | errors R/U |",
     "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
for run in runs:
    r = res[run]
    L = r["latency"]
    o.append("| %s | %s | %s | %s | %s | %s | %d | %d | %s | %s | %d / %d / %d | %d / %d / %d | %d / %d |" % (
        TITLES[run], "—" if r["kill_s"] is None else f"{r['kill_s']:+.2f} ({r['killed']})",
        r["migration_s"], r["tput_base_kops"], r["tput_min_kops"], r["tput_after_kops"],
        r["s_below_50"], r["s_below_90"], "—" if r["back_to_90_s"] is None else r["back_to_90_s"],
        r["ops_lost_k"],
        L["READ"]["pre_us"], L["READ"]["worst_us"], L["READ"]["post_us"],
        L["UPDATE"]["pre_us"], L["UPDATE"]["worst_us"], L["UPDATE"]["post_us"],
        r["errors"]["READ"], r["errors"]["UPDATE"]))
o.append("\n## Warm-up dip before the migration\n")
o.append("| run | first warm-up event (s) | lowest Kops/s in the 10 s before the migration |")
o.append("|---|---|---|")
for run in runs:
    r = res[run]
    o.append(f"| {TITLES[run]} | {r['warmup_first_s']} | {r['warmup_dip_kops']} |")
o.append("\n## Phase timeline per donor (s since migration start)\n")
for run in runs:
    r = res[run]
    o.append(f"**{TITLES[run]}**" + (f" — {r['killed']} killed at {r['kill_s']:+.2f} s" if r["kill_s"] is not None else ""))
    o.append("\n| donor | TXN_START | FLIPPING | TRANSFER | transfer finished | BACKPATCH | TXN_DONE |")
    o.append("|---|---|---|---|---|---|---|")
    for sg, p in sorted(r["phases"].items()):
        o.append("| %s | %s | %s | %s | %s | %s | %s |" % (sg, p.get("TXN_START", "—"), p.get("FLIPPING", "—"),
                 p.get("TRANSFER", "—"), p.get("transfer_done", "—"), p.get("BACKPATCH", "—"), p.get("TXN_DONE", "—")))
    rc = r["recipient"]
    o.append("\nrecipient: chain acks at %s; range commits (`RECP_TXN_DONE`) %s\n" % (
        rc.get("chain_acks", []), rc.get("commits", {})))
open(f"{here}/breakdown.md", "w").write("\n".join(o) + "\n")
print("\n".join(o))
