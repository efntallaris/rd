#!/usr/bin/env python3
"""One table over the crash suite: a row per scenario, the metrics of the evaluation.

usage: suite_table.py <experiment dir> [scenario ...]
Reads <dir>/data/<S>/{lincheck,exp} and <dir>/figures/<S>_recovery.json (figures.sh), writes
<dir>/figures/suite_table.{md,csv} and prints the markdown.

Columns
  correct     OVERALL of the run: lincheck (linearizable probe history) AND postcheck
              (migration durable on the sg4 leader, live replicas identical, no key lost)
  fault       what the injector did (run.log)
  no leader   per group that lost its leader: its leader's kill -> new leader (s)
  unavail     clients: first fault -> the last pair-A key class served again (its median key; s,
              from the probe)
  migration   first TXN_START -> last TXN_DONE (s), and the difference to HEALTHY
  re-sent     slots a recovery shipped again (the resume plans' "re-shipping N"), and their
              landing blocks (2 MiB per slot)
  worst lat   the worst 1-s mean YCSB latency of the run (ms; YCSB reports per-second means)
  errors      YCSB operations that returned an error
  plan/chain  recovery steps (s): plan (probe / re-home / resume plan), chain rebuild
  path        recovery paths seen in the server logs (see PATHS)
"""
import sys, os, re, glob, json, gzip, csv

base = sys.argv[1].rstrip("/")
scs = sys.argv[2:] or sorted((s for s in os.listdir(base + "/data") if re.match(r"^(HEALTHY|[SC]\d+)$", s)),
                             key=lambda s: (s != "HEALTHY", s[0] != "C", int(re.sub(r"\D", "", s) or 0)))
SLOT_BYTES = 1430257664 // 682
# recovery path -> log pattern, counted over every server log of the run
PATHS = [("hook", r"TEST HOOK \w+: id="),
         ("resume", r"AqRaft MGN-RECOVER: sess=\d+ RESUMED"),
         ("all-durable", r"every slot already durable"),
         ("re-home", r"donor re-home"),
         ("fetch", r"fetched from \S+ and merged here"),
         ("missing", r"0/\d+ peers hold all of it"),
         ("reform", r"RE-FORM"),
         ("repair", r"repairing the chain"),
         ("dispatch-failed", r"resume dispatch FAILED"),
         ("rehome-timeout", r"no re-home endpoint recorded")]


def lines(path):
    op = gzip.open if path.endswith(".gz") else open
    try:
        with op(path, "rt", errors="ignore") as f:
            yield from f
    except OSError:
        return


def migration_s(d):
    for l in lines(d + "/exp/migration_window.txt"):
        m = re.search(r"FULL .*:\s+([\d.]+)s", l)
        if m:
            return float(m.group(1))
    return None


def ycsb(d):
    worst, errs = 0.0, 0
    for f in glob.glob(d + "/exp/ycsb/*/tmp/ycsb_output_ycsb*"):
        if "load" in f:
            continue
        for l in lines(f):
            if " sec: " in l:
                for v in re.findall(r"(?:READ|UPDATE) AverageLatency\(us\)=([\d.]+)", l):
                    worst = max(worst, float(v) / 1e3)
            m = re.match(r"\[(READ|UPDATE)\], Return=ERROR, (\d+)", l)
            if m:
                errs += int(m.group(2))
    return worst, errs


rows, health = [], None
for sc in scs:
    d = "%s/data/%s" % (base, sc)
    run = list(lines(d + "/lincheck/run.log"))
    ov = next((m.group(1) for l in reversed(run) for m in [re.search(r"OVERALL: (\w+)", l)] if m), "?")
    fault = "; ".join(re.sub(r"\s+t_\w+=.*|\(verified alive\)", "", l.split("] ", 1)[1]).strip()
                      for l in run if re.search(r"\] (KILLED|RESTARTED|STOPPED|PARTITIONED)", l)) or "-"
    js = "%s/figures/%s_recovery.json" % (base, sc)
    jr = json.load(open(js))["rows"] if os.path.exists(js) else []
    down = {n.split(":")[0]: a for n, a, b in jr if "failure detection" in n and a is not None}
    noleader = ", ".join("%s %.2f" % (g, b - down.get(g, 0.0)) for n, a, b in jr
                         for g in [n.split(":")[0]] if n.endswith("-> new leader") and b is not None)
    served = [b for n, a, b in jr if re.match(r"CLIENT pair A .*(served again \(median key\)|first key served again)", n)
              and b is not None]
    plan = next(("%.2f" % (b - a) for n, a, b in jr if re.search(r"probe peers|roll-forward", n) and a is not None and b is not None), "")
    chain = next(("%.2f" % (b - a) for n, a, b in jr if "rebuild replication chain" in n and a is not None and b is not None), "")
    mig = migration_s(d)
    if sc == "HEALTHY":
        health = mig
    resent = 0
    seen = {k: 0 for k, _ in PATHS}
    for f in glob.glob(d + "/lincheck/logs/redis*/*.log"):
        donor = re.search(r"_sg[123]\.log$", f)
        for l in lines(f):
            if donor:
                m = re.search(r"resume-plan: .*re-shipping (\d+)", l)
                if m:
                    resent += int(m.group(1))
            for k, p in PATHS:
                if re.search(p, l):
                    seen[k] += 1
    worst, errs = ycsb(d)
    rows.append({
        "scenario": sc, "correct": ov, "fault": fault, "no leader (s)": noleader,
        "unavail (s)": "%.2f" % max(served) if served else ("-" if not jr else ""),
        "migration (s)": "" if mig is None else ("%.2f" % mig + ("" if health is None or sc == "HEALTHY" else " (%+.2f)" % (mig - health))),
        "re-sent slots": str(resent), "re-sent GiB": "%.1f" % (resent * SLOT_BYTES / 2**30),
        "worst lat (ms)": "%.1f" % worst, "errors": str(errs), "plan (s)": plan, "chain rebuild (s)": chain,
        "path": " ".join("%s%s" % (k, "" if v == 1 else "x%d" % v) for k, v in seen.items() if v)})

cols = list(rows[0].keys()) if rows else []
os.makedirs(base + "/figures", exist_ok=True)
with open(base + "/figures/suite_table.csv", "w", newline="") as f:
    w = csv.DictWriter(f, cols); w.writeheader(); w.writerows(rows)
md = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
md += ["| " + " | ".join(r[c].replace("|", "/") for c in cols) + " |" for r in rows]
open(base + "/figures/suite_table.md", "w").write("\n".join(md) + "\n")
print("\n".join(md))
