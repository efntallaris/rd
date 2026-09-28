#!/usr/bin/env python3
"""
Plot recipient + sender CPU + network + YCSB throughput/latency for a single
completed experiment directory.

Usage:
    python3 plot_full_run.py <expdir> <output_path.png>
"""
from __future__ import annotations
import re, sys
from datetime import datetime, timedelta
from pathlib import Path
import matplotlib.pyplot as plt

MPSTAT_RE = re.compile(
    r"^(\d{2}:\d{2}:\d{2})\s*(AM|PM)?\s+all\s+"
)

IFSTAT_RE = re.compile(
    r"^(\d{2}:\d{2}:\d{2})\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)"
)

def parse_mpstat(path: Path):
    """Return list of (datetime, cpu_used_pct). 24h timestamps (no AM/PM)."""
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    out = []
    with open(path) as f:
        for line in f:
            m = re.match(r"^(\d{2}:\d{2}:\d{2})\s+(AM|PM)?\s*all\b", line)
            if not m:
                continue
            hhmmss = m.group(1)
            hh, mm, ss = map(int, hhmmss.split(":"))
            ampm = m.group(2)
            if ampm == "PM" and hh != 12:
                hh += 12
            elif ampm == "AM" and hh == 12:
                hh = 0
            ts = today.replace(hour=hh, minute=mm, second=ss)
            tokens = line.strip().split()
            idle = float(tokens[-1])
            out.append((ts, 100.0 - idle))
    return out

def parse_ifstat(path: Path, iface_idx: int = 1):
    """Return list of (datetime, in_KBps, out_KBps) for the iface_idx-th column
    (default 1 = second iface, typically ens1f1np1 = 10.10.x cluster network).
    ifstat output format:
      Time          eno49np0           ens1f1np1
      HH:MM:SS   KB/s in  KB/s out   KB/s in  KB/s out
      18:59:41    ...        ...        ...        ...
    """
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    out = []
    with open(path) as f:
        for line in f:
            m = re.match(r"^(\d{2}:\d{2}:\d{2})\s+(.*)$", line)
            if not m:
                continue
            hhmmss = m.group(1)
            cols = m.group(2).split()
            # columns: in0 out0 in1 out1
            if len(cols) < 4:
                continue
            try:
                in_kbps  = float(cols[iface_idx * 2])
                out_kbps = float(cols[iface_idx * 2 + 1])
            except (ValueError, IndexError):
                continue
            hh, mm, ss = map(int, hhmmss.split(":"))
            ts = today.replace(hour=hh, minute=mm, second=ss)
            out.append((ts, in_kbps, out_kbps))
    return out

YCSB_TS_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2}):(\d{3})\s+"
    r"(\d+)\s+sec:\s+(\d+)\s+operations;\s+([\d.]+)\s+current ops/sec"
)
YCSB_READ_LAT_RE   = re.compile(r"\[READ\s+AverageLatency\(us\)=([\d.]+)")
YCSB_UPDATE_LAT_RE = re.compile(r"\[UPDATE\s+AverageLatency\(us\)=([\d.]+)")

def parse_ycsb(path: Path):
    """Parse YCSB output. The file may contain BOTH load + run phases. We
    keep ONLY the last contiguous block of samples (the run phase)."""
    samples = []
    with open(path) as f:
        # Find the LAST "Starting test." line — everything after it is the
        # run phase.
        contents = f.read()
        # Pull the offset of the last "Starting test."
        last_start = contents.rfind("Starting test.")
        if last_start >= 0:
            contents = contents[last_start:]
        for line in contents.splitlines():
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
    return samples

def to_rel(ts, t0):
    return (ts - t0).total_seconds()

def detect_tz_shift(t_ref, mp_first):
    """Return timedelta to shift mpstat times into the YCSB t0 frame."""
    if mp_first is None:
        return timedelta(0)
    diff_h = round((mp_first - t_ref).total_seconds() / 3600.0)
    return timedelta(hours=-diff_h)

def main(expdir: Path, out_path: Path):
    ycsb_path = expdir / "ycsb" / "ycsb0" / "tmp" / "ycsb_output_ycsb0"
    if not ycsb_path.exists():
        sys.exit(f"missing {ycsb_path}")
    ycsb_samples = parse_ycsb(ycsb_path)
    if not ycsb_samples:
        sys.exit("no YCSB samples parsed")
    t0 = ycsb_samples[0][0]

    senders = ["redis0", "redis1", "redis2"]
    recipient = "redis3"
    hosts = senders + [recipient]
    mp = {}; ifs = {}
    for h in hosts:
        sdir = expdir / "logs" / h / "users" / "entall" / "systat_logs"
        mpp = sdir / f"{h}_mpstat.txt"
        ifp = sdir / f"{h}_ifstat.txt"
        mp[h]  = parse_mpstat(mpp) if mpp.exists() else []
        ifs[h] = parse_ifstat(ifp, iface_idx=1) if ifp.exists() else []

    # Auto-detect TZ offset between mpstat hosts and YCSB host. Default = UTC
    # YCSB vs MDT redis (UTC-6) → shift mpstat by +6h to align.
    mp_first = next((m[0][0] for m in mp.values() if m), None)
    tz_shift = detect_tz_shift(t0, mp_first)
    def rel(t):
        return to_rel(t + tz_shift, t0)

    fig, axes = plt.subplots(4, 1, figsize=(11.5, 11), sharex=False)
    ax_cpu, ax_net, ax_tp, ax_lat = axes

    colors = {"redis0": "#7faedb", "redis1": "#a4d4a4", "redis2": "#e69999",
              "redis3": "#7a2da6"}

    # --- CPU ---
    for h in hosts:
        if not mp[h]:
            continue
        xs = [rel(t) for t, _ in mp[h]]
        ys = [v for _, v in mp[h]]
        if h == recipient:
            ax_cpu.plot(xs, ys, label=f"{h} (recipient)", color=colors[h],
                        linewidth=2.6, zorder=5)
        else:
            ax_cpu.plot(xs, ys, label=f"{h} (sender)", color=colors[h],
                        linewidth=1.1, alpha=0.8)
    ax_cpu.set_ylabel("CPU used (%)")
    ax_cpu.set_title("CPU utilization across migrations")
    ax_cpu.legend(loc="upper left", fontsize=9, ncol=2)
    ax_cpu.grid(True, alpha=0.3)

    # --- Network throughput (MB/s on cluster NIC) ---
    for h in hosts:
        if not ifs[h]:
            continue
        xs = [rel(t) for t, _, _ in ifs[h]]
        ys_in  = [i / 1024.0 for _, i, _ in ifs[h]]
        ys_out = [o / 1024.0 for _, _, o in ifs[h]]
        ys_total = [i + o for i, o in zip(ys_in, ys_out)]
        if h == recipient:
            ax_net.plot(xs, ys_total, label=f"{h} (recipient)",
                        color=colors[h], linewidth=2.6, zorder=5)
        else:
            ax_net.plot(xs, ys_total, label=f"{h} (sender)",
                        color=colors[h], linewidth=1.1, alpha=0.8)
    ax_net.set_ylabel("Net traffic in+out (MB/s)")
    ax_net.set_title("Network traffic on cluster NIC (ens1f1np1)")
    ax_net.legend(loc="upper left", fontsize=9, ncol=2)
    ax_net.grid(True, alpha=0.3)

    # --- Throughput ---
    tp_xs = [to_rel(t, t0) for t, _, _, _ in ycsb_samples]
    tp_ys = [v for _, v, _, _ in ycsb_samples]
    ax_tp.plot(tp_xs, tp_ys, color="#1a1a1a", linewidth=1.4)
    ax_tp.set_ylabel("Throughput (ops/sec)")
    ax_tp.set_title("YCSB throughput")
    ax_tp.grid(True, alpha=0.3)

    # --- Latency ---
    rd_pairs = [(to_rel(t, t0), r) for t, _, r, _ in ycsb_samples if r is not None]
    up_pairs = [(to_rel(t, t0), u) for t, _, _, u in ycsb_samples if u is not None]
    rd_xs = [x for x, _ in rd_pairs]; rd_ys = [y for _, y in rd_pairs]
    up_xs = [x for x, _ in up_pairs]; up_ys = [y for _, y in up_pairs]
    ax_lat.plot(rd_xs, rd_ys, label="READ", color="#1f77b4", linewidth=1.2)
    ax_lat.plot(up_xs, up_ys, label="UPDATE", color="#d62728", linewidth=1.2)
    ax_lat.set_ylabel("Avg latency (µs)")
    ax_lat.set_xlabel("seconds since YCSB t0")
    ax_lat.set_title("YCSB latency")
    ax_lat.legend(loc="upper left", fontsize=9)
    ax_lat.grid(True, alpha=0.3)
    p95_rd = sorted(rd_ys)[max(0, int(len(rd_ys) * 0.95) - 1)] if rd_ys else 1000
    p95_up = sorted(up_ys)[max(0, int(len(up_ys) * 0.95) - 1)] if up_ys else 1000
    ax_lat.set_ylim(0, max(p95_rd, p95_up) * 1.3)

    # Same X for all panels
    xmin = min(tp_xs) - 2
    xmax = max(tp_xs) + 2
    for ax in axes:
        ax.set_xlim(xmin, xmax)

    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=120, bbox_inches="tight")
    pdf = out_path.with_suffix(".pdf")
    plt.savefig(pdf, bbox_inches="tight")
    print(f"saved -> {out_path}")
    print(f"saved -> {pdf}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    main(Path(sys.argv[1]), Path(sys.argv[2]))
