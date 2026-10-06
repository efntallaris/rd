#!/usr/bin/env python3
"""Throughput and latency of two collected runs, side by side.

Usage: compare_runs.py <run-dir-a> <run-dir-b>

Reads the YCSB end-of-run summary of every client under <run-dir>/ycsb/.
Throughput is summed over the clients; latency is the operation-weighted mean
of the clients' averages (the runs use measurementtype=timeseries, which
reports averages only, no percentiles).
"""
import re
import sys
from pathlib import Path

LINE_RE = re.compile(r"^\[(OVERALL|READ|UPDATE)\], ([A-Za-z]+)(?:\([a-z/]+\))?, ([\d.]+)\s*$")


def load(run_dir):
    files = sorted(f for f in Path(run_dir, "ycsb").rglob("ycsb_output_*")
                   if f.is_file() and "_load_" not in f.name)
    if not files:
        sys.exit(f"no YCSB run output under {run_dir}/ycsb")
    tput = 0.0
    ops = {"READ": 0.0, "UPDATE": 0.0}
    lat_sum = {"READ": 0.0, "UPDATE": 0.0}
    for f in files:
        vals = {}
        for line in f.read_text(errors="replace").splitlines():
            m = LINE_RE.match(line)
            if m:
                vals[(m.group(1), m.group(2))] = float(m.group(3))
        if ("OVERALL", "Throughput") not in vals:
            sys.exit(f"{f}: no [OVERALL] Throughput line (run did not finish?)")
        tput += vals[("OVERALL", "Throughput")]
        for op in ops:
            n = vals.get((op, "Operations"), 0.0)
            ops[op] += n
            lat_sum[op] += n * vals.get((op, "AverageLatency"), 0.0)
    lat = {op: lat_sum[op] / ops[op] if ops[op] else float("nan") for op in ops}
    return {"clients": len(files), "tput": tput, "read": lat["READ"], "update": lat["UPDATE"]}


def main():
    dirs = sys.argv[1:]
    if len(dirs) != 2:
        sys.exit(__doc__)
    a, b = (load(d) for d in dirs)
    na, nb = (Path(d).name for d in dirs)
    rows = [("throughput (ops/s)", "tput"),
            ("READ avg latency (us)", "read"),
            ("UPDATE avg latency (us)", "update")]
    print(f"| metric | {na} | {nb} | change |")
    print("|---|---:|---:|---:|")
    for label, key in rows:
        change = (b[key] / a[key] - 1) * 100 if a[key] else float("nan")
        print(f"| {label} | {a[key]:,.0f} | {b[key]:,.0f} | {change:+.1f}% |")
    print(f"\nYCSB clients: {na}={a['clients']}, {nb}={b['clients']}")


if __name__ == "__main__":
    main()
