#!/usr/bin/env python3
"""
Plot recipient + sender CPU + YCSB throughput/latency over the migration
window. Reads mpstat from each host and the YCSB run-output, aligns timelines,
and shades migration windows.

Usage:
    python3 plot_cpu_tputlat.py <snapshot_dir> <output_dir> [ansible_log]

<snapshot_dir> should contain:
    redis0_mpstat.txt, redis1_mpstat.txt, redis2_mpstat.txt, redis3_mpstat.txt
    ycsb_output_ycsb0

[ansible_log] optional: workloada playbook log; used to extract migration
window timestamps for shading.
"""
from __future__ import annotations
import re, sys
from datetime import datetime, timedelta
from pathlib import Path
import matplotlib.pyplot as plt

# ---- parsing helpers --------------------------------------------------------

MPSTAT_RE = re.compile(
    r"^(\d{2}:\d{2}:\d{2})\s*(AM|PM)\s+all\s+"
    r"(\S+)\s+\S+\s+(\S+)\s+(\S+)"
)

def parse_mpstat(path: Path):
    """Return list of (datetime_today, cpu_used_pct)."""
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    out = []
    with open(path) as f:
        for line in f:
            m = MPSTAT_RE.match(line)
            if not m:
                continue
            hhmmss, ampm, usr, sys_, iowait = m.groups()
            hh, mm, ss = map(int, hhmmss.split(":"))
            if ampm == "PM" and hh != 12:
                hh += 12
            if ampm == "AM" and hh == 12:
                hh = 0
            ts = today.replace(hour=hh, minute=mm, second=ss)
            # rest of mpstat line: idle is last numeric column
            idle = float(line.strip().split()[-1])
            out.append((ts, 100.0 - idle))
    return out

YCSB_TS_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2}):(\d{3})\s+"
    r"(\d+)\s+sec:\s+(\d+)\s+operations;\s+([\d.]+)\s+current ops/sec"
)
YCSB_READ_LAT_RE   = re.compile(r"\[READ\s+AverageLatency\(us\)=([\d.]+)")
YCSB_UPDATE_LAT_RE = re.compile(r"\[UPDATE\s+AverageLatency\(us\)=([\d.]+)")

def parse_ycsb(path: Path):
    """Return (t0, samples) where samples = [(t, current_ops_per_sec, read_us, update_us)]."""
    samples = []
    with open(path) as f:
        for line in f:
            m = YCSB_TS_RE.match(line)
            if not m:
                continue
            yy, mm, dd, hh, mn, sc, ms, sec, ops, cur = m.groups()
            t = datetime(int(yy), int(mm), int(dd), int(hh), int(mn), int(sc),
                         int(ms) * 1000)
            r = YCSB_READ_LAT_RE.search(line)
            u = YCSB_UPDATE_LAT_RE.search(line)
            r_us = float(r.group(1)) if r else None
            u_us = float(u.group(1)) if u else None
            samples.append((t, float(cur), r_us, u_us))
    if not samples:
        return None, []
    return samples[0][0], samples

# ---- plot --------------------------------------------------------------------

def to_rel(ts, t0):
    return (ts - t0).total_seconds()

ANSIBLE_TS_RE = re.compile(r"^(\w+ \d{1,2} \w+ \d{4})\s+(\d{2}:\d{2}:\d{2})")
# Ansible doesn't timestamp every line by default. Use only the lines that
# DO have a timestamp from the start_ycsb_async result (registered fact). For
# our purposes we hand the script (datetime, "MIGRATE_START"/"MIGRATE_DONE")
# pairs from the log via a simple heuristic on Dispatch/Show-final TASK lines
# plus the dwells.
DISPATCH_RE = re.compile(r"^TASK \[Dispatch RDMA MIGRATE")
DONE_RE     = re.compile(r"^TASK \[Show final MIGRATE-STATUS")

def parse_ansible_milestones(path: Path):
    """Return list of (label, rel_time_seconds_index) — ansible-log line index.
    We don't have wall-clock per task line, so we just return an ordered list
    of milestones; the caller can place them on the time axis using the
    YCSB samples (each ~1s apart)."""
    if not path or not path.exists():
        return []
    milestones = []
    with open(path) as f:
        for line in f:
            if DISPATCH_RE.match(line):
                milestones.append(("DISPATCH", None))
            elif DONE_RE.match(line):
                milestones.append(("DONE", None))
    return milestones

def plot(snap: Path, outdir: Path, ansible_log: Path | None = None) -> None:
    outdir.mkdir(parents=True, exist_ok=True)

    ycsb_path = snap / "ycsb_output_ycsb0"
    if not ycsb_path.exists():
        sys.exit(f"missing {ycsb_path}")
    t0, ycsb_samples = parse_ycsb(ycsb_path)
    if not ycsb_samples:
        sys.exit("no YCSB samples parsed")

    senders = ["redis0", "redis1", "redis2"]
    recipient = "redis3"
    hosts = senders + [recipient]
    mpstat_data = {}
    for h in hosts:
        p = snap / f"{h}_mpstat.txt"
        if p.exists():
            mpstat_data[h] = parse_mpstat(p)
        else:
            mpstat_data[h] = []

    # Anchor on YCSB t0 (UTC). mpstat is in local time on the redis host (often
    # different TZ from YCSB host). Heuristic: align so each mpstat's median
    # corresponds to YCSB t0+half_run, i.e. just shift each host's mpstat
    # series to overlap YCSB's window.
    # Simpler: find the earliest mpstat that has CPU > 1% per host, use as t=0
    # for that host. But that distorts cross-host comparisons. Instead: shift
    # all mpstat timestamps so the FIRST sample's hour matches the YCSB t0's
    # hour. Since YCSB and redis run on the same date, this is a TZ shift.
    # mpstat is on redis side (UTC+local), YCSB output is in UTC. Cloudlab
    # rigs typically run UTC. Anyway: we just plot using mpstat's wall-clock
    # and YCSB's wall-clock; align by minute precision.

    # Auto-detect TZ offset between mpstat hosts and YCSB host: the first
    # samples should be roughly co-temporal. Compute the offset as the
    # mpstat-first - YCSB-t0 (rounded to nearest hour).
    # We assume all redis hosts share the same offset.
    mp_first = None
    for h in hosts:
        if mpstat_data[h]:
            mp_first = mpstat_data[h][0][0]
            break
    tz_offset_s = 0
    if mp_first is not None:
        raw_diff_s = (mp_first - t0).total_seconds()
        tz_offset_s = round(raw_diff_s / 3600.0) * 3600
    tz_shift = timedelta(seconds=-tz_offset_s)

    def to_rel_shifted(ts):
        return (ts + tz_shift - t0).total_seconds()

    fig, axes = plt.subplots(3, 1, figsize=(11, 9), sharex=False)
    ax_cpu, ax_tp, ax_lat = axes

    colors = {"redis0": "#7faedb", "redis1": "#a4d4a4", "redis2": "#e69999",
              "redis3": "#7a2da6"}
    for h in hosts:
        series = mpstat_data[h]
        if not series:
            continue
        xs = [to_rel_shifted(t) for t, _ in series]
        ys = [v for _, v in series]
        if h == recipient:
            label = f"{h} (recipient)"
            ax_cpu.plot(xs, ys, label=label, color=colors[h], linewidth=2.6,
                        alpha=1.0, zorder=5)
        else:
            label = f"{h} (sender)"
            ax_cpu.plot(xs, ys, label=label, color=colors[h], linewidth=1.0,
                        alpha=0.75)
    ax_cpu.set_ylabel("CPU used (%)")
    ax_cpu.set_xlabel("seconds since YCSB t0")
    ax_cpu.set_title("CPU usage per host across migrations")
    ax_cpu.legend(loc="upper left", fontsize=9)
    ax_cpu.grid(True, alpha=0.3)
    # Clip x-axis to YCSB-run window for clarity
    y_xmin = min(to_rel(s[0], t0) for s in ycsb_samples)
    y_xmax = max(to_rel(s[0], t0) for s in ycsb_samples)
    pad = (y_xmax - y_xmin) * 0.05
    for ax in (ax_cpu, ax_tp, ax_lat):
        ax.set_xlim(y_xmin - pad, y_xmax + pad)

    # Throughput
    tp_xs = [to_rel(t, t0) for t, _, _, _ in ycsb_samples]
    tp_ys = [v for _, v, _, _ in ycsb_samples]
    ax_tp.plot(tp_xs, tp_ys, color="#1a1a1a", linewidth=1.4)
    ax_tp.set_ylabel("Throughput (ops/sec)")
    ax_tp.set_xlabel("seconds since YCSB t0")
    ax_tp.set_title("YCSB throughput")
    ax_tp.grid(True, alpha=0.3)

    # Latency (read + update)
    rd_ys = [r for _, _, r, _ in ycsb_samples if r is not None]
    up_ys = [u for _, _, _, u in ycsb_samples if u is not None]
    rd_xs = [to_rel(t, t0) for t, _, r, _ in ycsb_samples if r is not None]
    up_xs = [to_rel(t, t0) for t, _, _, u in ycsb_samples if u is not None]
    ax_lat.plot(rd_xs, rd_ys, label="READ", color="#1f77b4", linewidth=1.2)
    ax_lat.plot(up_xs, up_ys, label="UPDATE", color="#d62728", linewidth=1.2)
    ax_lat.set_ylabel("Avg latency (µs)")
    ax_lat.set_xlabel("seconds since YCSB t0")
    ax_lat.set_title("YCSB latency")
    ax_lat.legend(loc="upper left", fontsize=9)
    ax_lat.grid(True, alpha=0.3)
    # Clip to typical operating range — last-second cleanup latency spike
    # can dwarf everything else.
    p95_rd = sorted(rd_ys)[max(0, int(len(rd_ys) * 0.95) - 1)] if rd_ys else 1000
    p95_up = sorted(up_ys)[max(0, int(len(up_ys) * 0.95) - 1)] if up_ys else 1000
    ax_lat.set_ylim(0, max(p95_rd, p95_up) * 1.3)

    plt.tight_layout()
    out_png = outdir / "cpu_tputlat.png"
    plt.savefig(out_png, dpi=120, bbox_inches="tight")
    plt.savefig(outdir / "cpu_tputlat.pdf", bbox_inches="tight")
    print(f"saved -> {out_png}")
    print(f"saved -> {outdir / 'cpu_tputlat.pdf'}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    plot(Path(sys.argv[1]), Path(sys.argv[2]))
