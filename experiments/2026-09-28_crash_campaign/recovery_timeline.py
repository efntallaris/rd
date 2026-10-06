#!/usr/bin/env python3
"""Recovery timeline per crash run: every recovery event in seconds after the (first) kill.

Usage: recovery_timeline.py <run dir> [<run dir> ...]
A run dir holds run.log (kill record), optional second_kill.txt, and
logs/<host>/tmp/redis_logs/*.log. Redis logs are UTC-6; kill times are UTC.
"""
import datetime as dt
import glob
import os
import re
import sys

TZ = dt.timedelta(hours=6)
TS = re.compile(r"(\d+ \w{3} \d{4} \d\d:\d\d:\d\d\.\d+)")
EVENTS = [
    (r"Node is now a leader", "elected leader"),
    (r"become-leader: in-flight migration sess=(\d+)", "promotion finds unfinished session {0}"),
    (r"held-block merge staged in (\d+) ms", "held-block merge done ({0} ms)"),
    (r"adopt: .*range=(\S+) (not adoptable|adopted)", "adopt check {0}: {1}"),
    (r"donor timeout: recipient leader unresponsive", "donor: recipient declared dead"),
    (r"TRANSFER failed .* waiting for its successor", "donor: transfer failed, waiting for re-home"),
    (r"DONOR-REHOME recorded: slot_lo=(\d+)", "donor: re-home recorded (slot {0})"),
    (r"donor re-home \((\w+)\): .*re-shipped slots \(lo=(\d+)", "donor: re-ship started from {0} (slot {1})"),
    (r"MGN-RECOVER: role=donor sess=(\d+)", "donor: MGN-RECOVER sess={0} (resume / re-drive)"),
    (r"worker: id=(8\d+) state=(PREP|REGISTERING|FLIPPING|TRANSFER|BACKPATCH)", "re-ship {0}: {1}"),
    (r"TRANSFER \(overlap\) finished .*transfer_ms=(\d+)", "transfer finished ({0} ms)"),
    (r"RE-FORM|dropped \d+ dead follower", "chain re-formed around dead node"),
    (r"chain-ack observed", "chain ack (majority holds batch)"),
    (r"RECP_TXN_DONE logged: sess=(\d+) slots=(\S+)", "recipient commits range {1}"),
    (r"MGN-LOG TXN_DONE logged: sess=(\d+)$", "donor closes session {0}"),
]


def kills(run):
    ks = []
    rl = os.path.join(run, "run.log")
    m = re.findall(r"target=(\S+) pid=\d+ t_arm=[\d.]+ t_kill=([\d.]+)", open(rl, errors="ignore").read())
    if m:
        h, t = m[-1]
        ks.append((dt.datetime.utcfromtimestamp(float(t)), h))
    sk = os.path.join(run, "second_kill.txt")
    if os.path.exists(sk):
        m = re.search(r"(redis\d) .* at (\d\d:\d\d:\d\d\.\d+)", open(sk).read())
        t = dt.datetime.combine(ks[0][0].date(), dt.datetime.strptime(m.group(2)[:15], "%H:%M:%S.%f").time())
        ks.append((t, m.group(1)))
    return sorted(ks)


def timeline(run, window=90):
    ks = kills(run)
    k0 = ks[0][0]
    rows = []
    for f in glob.glob(os.path.join(run, "logs", "*", "tmp", "redis_logs", "*.log")):
        name = os.path.basename(f)[:-4]
        for ln in open(f, errors="ignore"):
            m = TS.search(ln)
            if not m:
                continue
            for rx, label in EVENTS:
                e = re.search(rx, ln.rstrip())
                if not e:
                    continue
                t = dt.datetime.strptime(m.group(1), "%d %b %Y %H:%M:%S.%f") + TZ
                s = (t - k0).total_seconds()
                if -1 <= s <= window:
                    rows.append((s, name, label.format(*e.groups())))
                break
    for t, h in ks:
        rows.append(((t - k0).total_seconds(), h, "KILLED"))
    return sorted(rows)


if __name__ == "__main__":
    for run in sys.argv[1:]:
        print(f"===== {os.path.basename(run.rstrip('/'))}")
        seen = set()
        for s, host, ev in timeline(run):
            key = (host, ev)
            if key in seen and not ev.startswith(("re-ship", "chain", "transfer")):
                continue
            seen.add(key)
            print(f"  {s:+7.2f}  {host:<11} {ev}")
