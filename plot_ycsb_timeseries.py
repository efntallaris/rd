#!/usr/bin/env python3
"""
plot_ycsb_timeseries.py — plot YCSB throughput + latency over time from an
experiment result directory, with vertical axvlines marking the RDMA reshard
phase boundaries (RESHARD-FLIP, RESHARD-EXEC, DONE-SLOTS).

Usage:
    python3 plot_ycsb_timeseries.py /tmp/experiments/custom_reshard_v2_workloada
    python3 plot_ycsb_timeseries.py /tmp/experiments/custom_reshard_v2_workloada \
        --output /tmp/v2_smoke3.png

The script expects a result directory laid out by `collect_results.yml`:
    <expdir>/ycsb/ycsb0/tmp/ycsb_output_ycsb0     # YCSB per-second status lines
    <expdir>/logs/redis*/tmp/redis_logs/*.log     # redis-server logs (RDMA events)
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.size": 9,
    "axes.linewidth": 0.6,
    "xtick.direction": "out",
    "ytick.direction": "out",
})

# YCSB per-second status line. Example:
#   2026-05-15 21:32:04:362 3 sec: 85555 operations; 85726.45 current ops/sec; \
#     est completion in 33 seconds [READ AverageLatency(us)=121.83] \
#     [UPDATE AverageLatency(us)=126.47]
RE_YCSB_LINE = re.compile(
    r"(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}:\d{3})\s+"
    r"(?P<sec>\d+)\s+sec:\s+(?P<ops>\d+)\s+operations;\s+"
    r"(?P<tp>\d+(?:\.\d+)?)\s+current ops/sec"
)
RE_READ_LAT = re.compile(r"\[READ\s+AverageLatency\(us\)=(?P<v>\d+(?:\.\d+)?)\]")
RE_UPD_LAT  = re.compile(r"\[UPDATE\s+AverageLatency\(us\)=(?P<v>\d+(?:\.\d+)?)\]")

# Redis-server log timestamp format. Example:
#   176648:M 15 May 2026 07:42:37.684 * <raft> Raft Cluster initialized, ...
RE_REDIS_TS = re.compile(
    r"\d+:[MSC]\s+(?P<ts>\d{2}\s+\w+\s+\d{4}\s+\d{2}:\d{2}:\d{2}\.\d{3})"
)


def parse_ycsb_ts(s: str) -> datetime:
    """Parse 'YYYY-MM-DD HH:MM:SS:mmm' (YCSB uses ':' before millis)."""
    head, _, ms = s.rpartition(":")
    return datetime.strptime(f"{head}.{ms}", "%Y-%m-%d %H:%M:%S.%f")


def parse_redis_ts(s: str) -> datetime:
    return datetime.strptime(s, "%d %b %Y %H:%M:%S.%f")


@dataclass
class Sample:
    t: datetime
    tp: float          # current ops/sec
    read_lat: float    # READ AverageLatency(us) — NaN if not present
    upd_lat: float     # UPDATE AverageLatency(us) — NaN if not present


def parse_ycsb_log(path: Path) -> list[Sample]:
    samples: list[Sample] = []
    seen: set[tuple[int, float]] = set()
    with open(path, "r", errors="replace") as f:
        for line in f:
            m = RE_YCSB_LINE.search(line)
            if not m:
                continue
            t = parse_ycsb_ts(m.group("ts"))
            sec = int(m.group("sec"))
            tp = float(m.group("tp"))
            # YCSB writes each status line twice (stdout + stderr both redirected
            # to the same file). De-dup on (sec, tp).
            key = (sec, tp)
            if key in seen:
                continue
            seen.add(key)
            rd = RE_READ_LAT.search(line)
            up = RE_UPD_LAT.search(line)
            samples.append(Sample(
                t=t,
                tp=tp,
                read_lat=float(rd.group("v")) if rd else float("nan"),
                upd_lat=float(up.group("v")) if up else float("nan"),
            ))
    samples.sort(key=lambda s: s.t)
    return samples


def find_first_marker(expdir: Path, *needles: str) -> Optional[datetime]:
    """Earliest timestamp across all redis logs whose line contains any needle."""
    earliest: Optional[datetime] = None
    log_root = expdir / "logs"
    if not log_root.exists():
        return None
    for log_path in log_root.rglob("redis_logs/redis*"):
        try:
            with open(log_path, "r", errors="replace") as f:
                for line in f:
                    if not any(p in line for p in needles):
                        continue
                    m = RE_REDIS_TS.search(line)
                    if not m:
                        continue
                    t = parse_redis_ts(m.group("ts"))
                    if earliest is None or t < earliest:
                        earliest = t
                    break
        except Exception:
            continue
    return earliest


def find_last_marker(expdir: Path, *needles: str) -> Optional[datetime]:
    """Latest timestamp across all redis logs whose line contains any needle."""
    latest: Optional[datetime] = None
    log_root = expdir / "logs"
    if not log_root.exists():
        return None
    for log_path in log_root.rglob("redis_logs/redis*"):
        try:
            with open(log_path, "r", errors="replace") as f:
                last_match: Optional[datetime] = None
                for line in f:
                    if not any(p in line for p in needles):
                        continue
                    m = RE_REDIS_TS.search(line)
                    if m:
                        last_match = parse_redis_ts(m.group("ts"))
                if last_match and (latest is None or last_match > latest):
                    latest = last_match
        except Exception:
            continue
    return latest


def auto_tz_offset(redis_t: Optional[datetime], ycsb_t0: datetime) -> Optional[datetime]:
    """YCSB Java logs timestamps in UTC; redis-server logs in local time.
    On a UTC-6 host the two streams drift by ~6 hours. Detect by rounding
    the redis-vs-ycsb gap to the nearest hour when it exceeds 30 minutes,
    and subtract that many hours from the redis timestamp so the two
    streams align in a common wall-clock frame."""
    if redis_t is None:
        return None
    diff = (redis_t - ycsb_t0).total_seconds()
    if abs(diff) > 1800:
        offset_hours = round(diff / 3600)
        return redis_t - timedelta(hours=offset_hours)
    return redis_t


def to_rel(t: Optional[datetime], t0: datetime) -> Optional[float]:
    return None if t is None else (t - t0).total_seconds()


def plot(expdir: Path, output: Path) -> None:
    ycsb_path = expdir / "ycsb" / "ycsb0" / "tmp" / "ycsb_output_ycsb0"
    if not ycsb_path.exists():
        cands = list(expdir.rglob("ycsb_output_*"))
        if not cands:
            sys.exit(f"No YCSB output file found under {expdir}")
        ycsb_path = cands[0]

    samples = parse_ycsb_log(ycsb_path)
    if not samples:
        sys.exit(f"No timeseries samples parsed from {ycsb_path}")

    t0 = samples[0].t
    t_rel = [(s.t - t0).total_seconds() for s in samples]
    tp = [s.tp for s in samples]
    rd_lat = [s.read_lat for s in samples]
    up_lat = [s.upd_lat for s in samples]

    flip_rel       = to_rel(auto_tz_offset(find_first_marker(expdir,
                            "RDMA RESHARD-FLIP: slot=", "ownership flipped to"), t0), t0)
    exec_first_rel = to_rel(auto_tz_offset(find_first_marker(expdir, "RDMA RESHARD-EXEC: slot="), t0), t0)
    exec_last_rel  = to_rel(auto_tz_offset(find_last_marker (expdir, "RDMA RESHARD-EXEC: slot="), t0), t0)
    done_rel       = to_rel(auto_tz_offset(find_first_marker(expdir, "RDMA DONE-SLOTS: applied"), t0), t0)

    fig, (ax_tp, ax_lat) = plt.subplots(
        2, 1, figsize=(11, 6), sharex=True,
        gridspec_kw={"height_ratios": [2, 2], "hspace": 0.18}
    )

    ax_tp.plot(t_rel, tp, color="#1f3a5e", lw=1.6)
    ax_tp.fill_between(t_rel, 0, tp, color="#1f3a5e", alpha=0.10)
    ax_tp.set_ylabel("Throughput (ops/sec)")
    ax_tp.grid(True, axis="y", alpha=0.3, lw=0.4)
    ax_tp.set_title(f"YCSB throughput + latency — {expdir.name}",
                    fontsize=11, fontweight="bold")

    ax_lat.plot(t_rel, rd_lat, color="#2a7f3f", lw=1.4, label="READ avg latency (μs)")
    ax_lat.plot(t_rel, up_lat, color="#c44400", lw=1.4, label="UPDATE avg latency (μs)")
    ax_lat.set_ylabel("Avg latency (μs)")
    ax_lat.set_xlabel("Time since YCSB start (seconds)")
    ax_lat.grid(True, axis="y", alpha=0.3, lw=0.4)
    ax_lat.legend(loc="upper right", fontsize=8, frameon=False)

    # Migration-window shaded band (FLIP → DONE-SLOTS, or fallback to EXEC span).
    band_start = flip_rel if flip_rel is not None else exec_first_rel
    band_end   = done_rel if done_rel is not None else exec_last_rel
    if band_start is not None and band_end is not None and band_end > band_start:
        for ax in (ax_tp, ax_lat):
            ax.axvspan(band_start, band_end, alpha=0.10, color="#c44400")

    # Compute y-limits before annotating.
    ax_tp.relim(); ax_tp.autoscale_view()
    ax_lat.relim(); ax_lat.autoscale_view()

    def vline(ax, x: Optional[float], color: str, label: str):
        if x is None:
            return
        ax.axvline(x, color=color, ls="--", lw=1.0, alpha=0.85)
        _, ymax = ax.get_ylim()
        ax.annotate(label, xy=(x, ymax * 0.95),
                    xytext=(2, 0), textcoords="offset points",
                    rotation=90, va="top", ha="left",
                    color=color, fontsize=7.5)

    for ax in (ax_tp, ax_lat):
        vline(ax, flip_rel,       "#c44400", "FLIP")
        vline(ax, exec_first_rel, "#a16524", "EXEC start")
        vline(ax, exec_last_rel,  "#a16524", "EXEC end")
        vline(ax, done_rel,       "#6c4a96", "DONE-SLOTS")

    plt.tight_layout()
    plt.savefig(output, dpi=160, bbox_inches="tight")
    print(f"wrote {output}")
    print(f"  samples: {len(samples)}    span: {t_rel[-1]:.1f}s")
    if flip_rel       is not None: print(f"  FLIP:         t+{flip_rel:.2f}s")
    if exec_first_rel is not None: print(f"  EXEC start:   t+{exec_first_rel:.2f}s")
    if exec_last_rel  is not None: print(f"  EXEC end:     t+{exec_last_rel:.2f}s")
    if done_rel       is not None: print(f"  DONE-SLOTS:   t+{done_rel:.2f}s")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("expdir", type=Path,
                   help="experiment result directory under /tmp/experiments/")
    p.add_argument("--output", "-o", type=Path, default=None,
                   help="output PNG path (default: <expdir>/ycsb_timeseries.png)")
    args = p.parse_args()

    expdir = args.expdir.resolve()
    if not expdir.is_dir():
        sys.exit(f"Not a directory: {expdir}")
    output = args.output or (expdir / "ycsb_timeseries.png")
    plot(expdir, output)


if __name__ == "__main__":
    main()
