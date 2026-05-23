#!/usr/bin/env python3
"""
plot_ycsb_timeseries.py — plot YCSB throughput + latency over time from an
experiment result directory, with vertical axvlines marking the RDMA reshard
phase boundaries (RESHARD-FLIP, RESHARD-TRANSFER, DONE-SLOTS).

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
import matplotlib.ticker as mticker
import shutil

# Use LaTeX rendering when available; otherwise fall back to mathtext
# rendering with the same Computer-Modern-style serif font.
_HAS_LATEX = shutil.which("latex") is not None and shutil.which("dvipng") is not None

plt.rcParams.update({
    "text.usetex":     _HAS_LATEX,
    # Computer-Modern-style geometric sans-serif. When LaTeX is available
    # the preamble below switches the default family to sans (\sffamily);
    # otherwise matplotlib's font search picks from the list.
    "font.family":         "sans-serif",
    "font.sans-serif":     ["DejaVu Sans"],
    "mathtext.fontset":    "dejavusans",
    "text.latex.preamble": r"\renewcommand{\familydefault}{\sfdefault}",
    # Paper-style hierarchy: body 15 anchors title (~1.5x), axis (~1.2x),
    # legend (1.0x), tick (~0.85x).
    "font.size":       15,
    "axes.titlesize":  16,
    "axes.labelsize":  13,
    "legend.fontsize": 11,
    "xtick.labelsize": 15,
    "ytick.labelsize": 15,
    # Borderless panels — no spines on any side.
    "axes.linewidth":      0.7,
    "axes.spines.top":     False,
    "axes.spines.right":   False,
    "axes.spines.left":    False,
    "axes.spines.bottom":  False,
    "axes.edgecolor":      "#333333",
    # Outward ticks; small extra pad to push tick labels further from axis.
    "xtick.direction":     "out",
    "ytick.direction":     "out",
    "xtick.major.size":    4.0,
    "ytick.major.size":    4.0,
    "xtick.major.width":   1.4,
    "ytick.major.width":   1.4,
    "xtick.major.pad":     5.0,
    "ytick.major.pad":     5.0,
    "xtick.color":         "#333333",
    "ytick.color":         "#333333",
    # Sparse dotted horizontal gridlines for read-off.
    "axes.grid":           True,
    "axes.grid.axis":      "y",
    "grid.color":          "#666666",
    "grid.linewidth":      0.9,
    "grid.linestyle":      (0, (1, 5)),   # dot, 5x gap = sparse dots
    "grid.alpha":          1.0,
    # Editable text in PDFs (USENIX/ACM expectation).
    "pdf.fonttype":        42,
    "ps.fonttype":         42,
    "legend.frameon":      False,
})

# Inline annotation font sizes (band labels, vertical-line labels, summary).
_FS_BAND_LABEL = 13   # "M1 7.4s" centered above each migration band
_FS_VLINE      = 13   # rotated FLIP label next to vertical line
_FS_SUMMARY    = 13   # "Total migration: 34.7s (3 sources, serial)"

# Crop the x-axis so the YCSB ramp-up (no ops in seconds 0-8) is hidden,
# and bound the upper end so all three workloads share the same window.
PLOT_X_MIN = 0
PLOT_X_MAX = 80

# Grayscale palette — differentiate by line style + marker shape, not color.
THROUGHPUT_COLOR = "#1A1A1A"     # near-black throughput line
THROUGHPUT_FILL  = "#4A4A4A"     # mid-dark gray for area shading under tp
AQUEDUCT_STYLE = dict(
    color=THROUGHPUT_COLOR, marker="D", markersize=6, markevery=6,
    linewidth=1.8, linestyle="-",
    markerfacecolor=THROUGHPUT_COLOR, markeredgecolor="white",
    markeredgewidth=0.7,
)
LATENCY_READ_STYLE = dict(
    color="#1a1a1a", marker="o", markersize=6, markevery=4,
    linewidth=1.5, linestyle=(0, (5, 2)),       # dashed
    markerfacecolor="white", markeredgecolor="#1a1a1a", markeredgewidth=1.2,
)
LATENCY_UPDATE_STYLE = dict(
    color="#1a1a1a", marker="^", markersize=7, markevery=4,
    linewidth=1.5, linestyle=(0, (1, 1.5)),     # dotted
    markerfacecolor="#1a1a1a", markeredgecolor="white", markeredgewidth=0.7,
)

# Migration-band palette — neutral gray fill + dark gray edges / labels.
PHASE_COLORS = {
    "FLIP":      "#3A3A3A",     # dark gray (legacy vline)
    "BAND_FILL": "#9E9E9E",     # mid-gray band fill (used with low alpha)
    "BAND_EDGE": "#333333",     # dark gray edge / label color
    "DONE":      "#666666",     # gray
}

# Pure white backgrounds.
FIG_BG  = "#FFFFFF"
AXES_BG = "#FFFFFF"

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


def find_per_source_migration_windows(expdir: Path) -> list[tuple[str, datetime, datetime]]:
    """Per-source (PREP-start, DONE-end) for each source master that did a
    migration in this experiment. One tuple per source. Returns
    [(source-id, start_dt, end_dt), ...] sorted by start time. Source ID is
    just the redis log filename stem (e.g. "redis0"). Uses these markers:
        start: "RDMA MIGRATE-PREP: new outbound link"
        end:   "RDMA MIGRATE worker: id=*  DONE n_slots="
    """
    windows: list[tuple[str, datetime, datetime]] = []
    log_root = expdir / "logs"
    if not log_root.exists():
        return windows
    for log_path in log_root.rglob("redis_logs/redis*"):
        # The recipient (redis3) also has "DONE" lines from its backpatch thread,
        # but no "MIGRATE-PREP" line — skip silently if PREP isn't found.
        prep_t: Optional[datetime] = None
        done_t: Optional[datetime] = None
        try:
            with open(log_path, "r", errors="replace") as f:
                for line in f:
                    if "RDMA MIGRATE-PREP: new outbound link" in line and prep_t is None:
                        m = RE_REDIS_TS.search(line)
                        if m:
                            prep_t = parse_redis_ts(m.group("ts"))
                    if "RDMA MIGRATE worker:" in line and "DONE n_slots=" in line:
                        m = RE_REDIS_TS.search(line)
                        if m:
                            done_t = parse_redis_ts(m.group("ts"))  # keep latest
        except Exception:
            continue
        if prep_t and done_t and done_t > prep_t:
            windows.append((log_path.name, prep_t, done_t))
    windows.sort(key=lambda w: w[1])
    return windows


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


def _k_formatter(v: float, _pos) -> str:
    """Format throughput ticks as `200K`, `1M`, etc."""
    if v >= 1_000_000:
        return f"{v / 1_000_000:.1f}M" if v % 1_000_000 else f"{int(v // 1_000_000)}M"
    if v >= 1_000:
        return f"{int(v / 1_000)}K"
    return f"{int(v)}"


def _annotate_vline(ax, x: Optional[float], color: str, label: str) -> None:
    if x is None:
        return
    ax.axvline(x, color=color, ls="--", lw=1.0, alpha=0.85, zorder=2)
    _, ymax = ax.get_ylim()
    ax.annotate(
        label,
        xy=(x, ymax),
        xytext=(3, -3),
        textcoords="offset points",
        rotation=90,
        va="top",
        ha="left",
        color=color,
        fontsize=_FS_VLINE,
    )


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
    transfer_first_rel = to_rel(auto_tz_offset(find_first_marker(expdir, "RDMA RESHARD-TRANSFER: slot="), t0), t0)
    transfer_last_rel  = to_rel(auto_tz_offset(find_last_marker (expdir, "RDMA RESHARD-TRANSFER: slot="), t0), t0)
    done_rel       = to_rel(auto_tz_offset(find_first_marker(expdir, "RDMA DONE-SLOTS: applied"), t0), t0)

    # Per-source migration windows (PREP -> DONE per source). One band per
    # migration, plus the overall span.
    raw_windows = find_per_source_migration_windows(expdir)
    migration_bands: list[tuple[str, float, float]] = []
    for src, p, d in raw_windows:
        ps = auto_tz_offset(p, t0)
        ds = auto_tz_offset(d, t0)
        if ps is None or ds is None:
            continue
        s_rel = to_rel(ps, t0)
        e_rel = to_rel(ds, t0)
        if s_rel is None or e_rel is None or e_rel <= s_rel:
            continue
        migration_bands.append((src, s_rel, e_rel))

    # Workload nickname: strip the experiment prefix for the panel titles.
    workload = expdir.name
    for prefix in ("custom_reshard_v2_", "custom_reshard_"):
        if workload.startswith(prefix):
            workload = workload[len(prefix):]
            break

    # 2 panels side-by-side, each ≈ φ:1 (width:height).
    PHI = 1.618
    panel_h = 4.0
    panel_w = panel_h * PHI  # ≈ 6.47
    fig_w   = 2 * panel_w + 1 * 0.35 * panel_w + 0.8
    fig_h   = panel_h + 1.5  # room for band labels, x-label, footer legend
    fig, (ax_tp, ax_lat) = plt.subplots(
        1, 2, figsize=(fig_w, fig_h), sharex=True,
        gridspec_kw={"width_ratios": [1, 1], "wspace": 0.35},
        facecolor=FIG_BG,
    )

    def _stylize_axes(ax):
        # Borderless + dotted grid; ticks themselves separate x/y axes.
        ax.set_facecolor(AXES_BG)
        for side in ("top", "right", "left", "bottom"):
            ax.spines[side].set_visible(False)
        ax.grid(True, axis="y", color="#666666", linewidth=0.9,
                linestyle=(0, (1, 5)), alpha=1.0, zorder=0)
        ax.set_axisbelow(True)

    _stylize_axes(ax_tp)
    _stylize_axes(ax_lat)

    # ---- Panel (a): Throughput --------------------------------------------------
    ax_tp.plot(t_rel, tp, label="Aqueduct", zorder=3, **AQUEDUCT_STYLE)
    ax_tp.set_title("Throughput", pad=22)
    ax_tp.set_ylabel("Th/put (Kops/s)")
    # Y-axis in Kops/s with exactly 3 ticks framing the data range (no 0 tick).
    ax_tp.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda v, _: f"{v/1000:.0f}"))
    # Fixed ticks at 300/350/400/450 Kops/s so the throughput series sits mid-axis.
    ax_tp.set_ylim(260_000, 450_000)
    ax_tp.yaxis.set_major_locator(mticker.FixedLocator([300_000, 350_000, 400_000, 450_000]))

    # ---- Panel (b): Avg latency -------------------------------------------------
    ax_lat.plot(t_rel, rd_lat, label="READ", zorder=3, **LATENCY_READ_STYLE)
    ax_lat.plot(t_rel, up_lat, label="UPDATE", zorder=3, **LATENCY_UPDATE_STYLE)
    ax_lat.set_title("Average Latency", pad=22)
    ax_lat.set_xlabel("Time since YCSB start (seconds)")
    ax_tp.set_xlabel("Time since YCSB start (seconds)")
    ax_lat.set_ylabel(r"Latency ($\mu$s)")
    # Fixed ticks at 100/150/200 μs; ylim matches the throughput panel's
    # visual proportions (ticks span ~25%→87% of the panel height) so the
    # latency series sits in the same band of the axis.
    ax_lat.set_ylim(60, 220)
    ax_lat.yaxis.set_major_locator(mticker.FixedLocator([100, 150, 200]))
    if t_rel:
        ax_tp.set_xlim(PLOT_X_MIN, PLOT_X_MAX)
        ax_lat.set_xlim(PLOT_X_MIN, PLOT_X_MAX)
    # Legend moved to the figure footer (below both panels and below the
    # x-axis title) so it never overlaps data or labels.
    handles, labels = ax_lat.get_legend_handles_labels()
    fig.legend(
        handles, labels,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.02),
        ncol=len(labels),
        frameon=False,
        handlelength=2.0,
        handletextpad=0.5,
        columnspacing=1.8,
    )

    # Per-source migration bands. Shade each PREP->DONE window with a soft
    # olive fill and a thin olive border on the left/right edges so the band
    # reads as a panel guide rather than a blob.
    for src, s_rel, e_rel in migration_bands:
        for ax in (ax_tp, ax_lat):
            ax.axvspan(s_rel, e_rel, alpha=0.20,
                       color=PHASE_COLORS["BAND_FILL"], zorder=1)

    # Recompute y-limits before annotating so labels sit at correct height.
    ax_lat.relim(); ax_lat.autoscale_view()
    # ax_tp y-limits were set just-above to bracket the actual data range
    # (no 0 tick); do not relim/autoscale here — that would re-include 0.

    # Duration labels in white rounded boxes with olive border, centered above
    # each migration band so they pop out of the band fill.
    if migration_bands:
        _, ymax_tp = ax_tp.get_ylim()
        label_bbox = dict(
            boxstyle="round,pad=0.30,rounding_size=0.20",
            facecolor="white", edgecolor=PHASE_COLORS["BAND_EDGE"],
            linewidth=1.0, alpha=0.95,
        )
        for i, (src, s_rel, e_rel) in enumerate(migration_bands, start=1):
            dur = e_rel - s_rel
            mid = (s_rel + e_rel) / 2.0
            # Place the label just above the panel top edge, centered over the
            # band, with text on a single line ("M1 7.4s"). Side-by-side
            # layout: label both panels so the latency panel doesn't look
            # orphaned.
            for ax in (ax_tp, ax_lat):
                ax.annotate(
                    f"M{i} {dur:.1f}s",
                    xy=(mid, 1.0), xycoords=("data", "axes fraction"),
                    xytext=(0, 4), textcoords="offset points",
                    ha="center", va="bottom",
                    color=PHASE_COLORS["BAND_EDGE"],
                    fontsize=_FS_BAND_LABEL,
                )
        # Overall span summary as a footer line below the legend, so it never
        # covers data or the migration bands.
        total_start = migration_bands[0][1]
        total_end   = migration_bands[-1][2]
        total       = total_end - total_start
        fig.text(
            0.5, -0.12,
            f"Total migration: {total:.1f}s ({len(migration_bands)} sources)",
            ha="center", va="top",
            color=PHASE_COLORS["BAND_EDGE"],
            fontsize=_FS_SUMMARY,
            transform=fig.transFigure,
        )

    # FLIP vline intentionally not drawn — the M1 band's left edge already
    # marks the first FLIP. Keep _annotate_vline available for future use.

    plt.subplots_adjust(left=0.08, right=0.97, top=0.92, bottom=0.18)
    fig.align_ylabels([ax_tp, ax_lat])
    for ax in (ax_tp, ax_lat):
        _bold_tick_labels(ax)
    plt.savefig(output, dpi=300, bbox_inches="tight")
    print(f"wrote {output}")
    print(f"  samples: {len(samples)}    span: {t_rel[-1]:.1f}s")
    if flip_rel           is not None: print(f"  FLIP:             t+{flip_rel:.2f}s")
    if transfer_first_rel is not None: print(f"  TRANSFER start:   t+{transfer_first_rel:.2f}s")
    if transfer_last_rel  is not None: print(f"  TRANSFER end:     t+{transfer_last_rel:.2f}s")
    if done_rel           is not None: print(f"  DONE-SLOTS:       t+{done_rel:.2f}s")


# ============================================================================ #
#  System-resource parsers (mpstat / iostat / ifstat) + plot_resources         #
# ============================================================================ #

# mpstat / iostat line prefix: "HH:MM:SS AM" or "HH:MM:SS PM"
RE_HHMMSS_AMPM = re.compile(r"^(\d{1,2}:\d{2}:\d{2})\s+(AM|PM)\b")
# ifstat line prefix: "HH:MM:SS"
RE_HHMMSS = re.compile(r"^(\d{2}:\d{2}:\d{2})\s+")


def _parse_hhmmss_ampm(date_anchor: datetime, hhmmss: str, ampm: str) -> datetime:
    """Combine an HH:MM:SS [AM|PM] time-of-day with a same-day date anchor."""
    h, m, s = (int(x) for x in hhmmss.split(":"))
    if ampm == "PM" and h != 12:
        h += 12
    elif ampm == "AM" and h == 12:
        h = 0
    return date_anchor.replace(hour=h, minute=m, second=s, microsecond=0)


def parse_mpstat(path: Path, date_anchor: datetime) -> list[tuple[datetime, float, float]]:
    """Return list of (t, cpu_used_pct, iowait_pct) per second from `mpstat 1`.
    cpu_used_pct = 100 - %idle from the `all` CPU row."""
    out: list[tuple[datetime, float, float]] = []
    with open(path, "r", errors="replace") as f:
        for line in f:
            m = RE_HHMMSS_AMPM.match(line)
            if not m:
                continue
            parts = line.split()
            # `HH:MM:SS PM all %usr %nice %sys %iowait %irq %soft %steal %guest %gnice %idle`
            if len(parts) < 13 or parts[2] != "all":
                continue
            try:
                iowait = float(parts[6])
                idle = float(parts[-1])
                t = _parse_hhmmss_ampm(date_anchor, m.group(1), m.group(2))
                out.append((t, 100.0 - idle, iowait))
            except (ValueError, IndexError):
                continue
    return out


def parse_iostat(path: Path, date_anchor: datetime,
                 device: str = "sda") -> list[tuple[datetime, float, float, float]]:
    """Return list of (t, tps, kB_read_per_s, kB_wrtn_per_s) per interval from
    `iostat 1`. iostat dumps a multi-line block per interval; we anchor to the
    `MM/DD/YYYY HH:MM:SS PM` header line and then read the `device` row that
    follows. The first block is the boot-time cumulative — we skip it."""
    RE_BLOCK_TS = re.compile(
        r"^(\d{2}/\d{2}/\d{4})\s+(\d{1,2}:\d{2}:\d{2})\s+(AM|PM)\s*$"
    )
    out: list[tuple[datetime, float, float, float]] = []
    cur_t: Optional[datetime] = None
    seen_blocks = 0
    with open(path, "r", errors="replace") as f:
        for line in f:
            m = RE_BLOCK_TS.match(line.rstrip())
            if m:
                seen_blocks += 1
                try:
                    cur_t = _parse_hhmmss_ampm(date_anchor, m.group(2), m.group(3))
                except Exception:
                    cur_t = None
                continue
            if cur_t is None or seen_blocks <= 1:
                continue
            parts = line.split()
            if not parts or parts[0] != device:
                continue
            # `device tps kB_read/s kB_wrtn/s ...` (newer iostat may add kB_dscd/s)
            try:
                tps = float(parts[1])
                kb_read = float(parts[2])
                kb_wrtn = float(parts[3])
                out.append((cur_t, tps, kb_read, kb_wrtn))
            except (ValueError, IndexError):
                continue
    return out


def parse_ifstat(path: Path, date_anchor: datetime,
                 interface: str = "ens1f1np1") -> list[tuple[datetime, float, float]]:
    """Return list of (t, kB_in, kB_out) per second from `ifstat 1` for the
    chosen interface. The header line lists interfaces; we find which column
    pair belongs to `interface`. Time-of-day prefix is HH:MM:SS without AM/PM —
    we infer AM/PM from the date_anchor (the first sample is assumed to be on
    the same wall-clock half as the anchor)."""
    out: list[tuple[datetime, float, float]] = []
    iface_col: Optional[int] = None
    with open(path, "r", errors="replace") as f:
        for line in f:
            stripped = line.rstrip()
            if iface_col is None:
                # Header lines: first lists interface names, second lists units.
                cols = stripped.split()
                if interface in cols:
                    iface_col = cols.index(interface)
                continue
            m = RE_HHMMSS.match(stripped)
            if not m:
                continue
            parts = stripped.split()
            # Header tokens: ['Time', iface0, iface1, ...]. Data tokens:
            # [HH:MM:SS, in0, out0, in1, out1, ...]. For interface at header
            # index k (k >= 1), data columns are 2*k - 1 and 2*k.
            data_idx_in = 2 * iface_col - 1
            data_idx_out = 2 * iface_col
            if len(parts) <= data_idx_out:
                continue
            try:
                h, mi, s = (int(x) for x in parts[0].split(":"))
                t = date_anchor.replace(hour=h, minute=mi, second=s, microsecond=0)
                kb_in = float(parts[data_idx_in])
                kb_out = float(parts[data_idx_out])
                out.append((t, kb_in, kb_out))
            except (ValueError, IndexError):
                continue
    return out


def _stylize_axes(ax) -> None:
    """Borderless + dotted grid (mirrors plot()'s helper)."""
    ax.set_facecolor(AXES_BG)
    for side in ("top", "right", "left", "bottom"):
        ax.spines[side].set_visible(False)
    ax.grid(True, axis="y", color="#666666", linewidth=0.9,
            linestyle=(0, (1, 5)), alpha=1.0, zorder=0)
    ax.set_axisbelow(True)


def _bold_tick_labels(ax) -> None:
    """No-op kept for call-site compatibility — tick labels render at the
    default regular weight set in rcParams."""
    return


def plot_resources(expdir: Path, output: Path, host: str) -> None:
    """Three-panel CPU / Network / Disk timeseries for one redis host, with
    the same migration-band shading as the YCSB plot for visual alignment."""
    systat_dir = expdir / "logs" / host / "users" / "entall" / "systat_logs"
    if not systat_dir.is_dir():
        # Some collected layouts use a flat directory; try that.
        cands = list(expdir.rglob(f"{host}_mpstat.txt"))
        if not cands:
            sys.exit(f"No systat logs for {host} under {expdir}")
        systat_dir = cands[0].parent
    mpstat_p = systat_dir / f"{host}_mpstat.txt"
    iostat_p = systat_dir / f"{host}_iostat.txt"
    ifstat_p = systat_dir / f"{host}_ifstat.txt"

    # Align timestamps using the YCSB t0 (same anchor as the YCSB plot).
    ycsb_path = expdir / "ycsb" / "ycsb0" / "tmp" / "ycsb_output_ycsb0"
    if not ycsb_path.exists():
        cands = list(expdir.rglob("ycsb_output_*"))
        if not cands:
            sys.exit(f"No YCSB output file found under {expdir}")
        ycsb_path = cands[0]
    samples = parse_ycsb_log(ycsb_path)
    if not samples:
        sys.exit(f"No YCSB samples parsed; cannot anchor resource timeline")
    t0 = samples[0].t

    # mpstat / iostat / ifstat all log HH:MM:SS in the redis-host local-time
    # frame (UTC-offset from YCSB on this cloudlab rig). Use the same TZ
    # heuristic the YCSB plot uses to translate the first observable redis-
    # log timestamp into the YCSB frame, then anchor parsing to the same
    # calendar date as the YCSB t0.
    date_anchor = t0.replace(microsecond=0)

    mpstat = parse_mpstat(mpstat_p, date_anchor) if mpstat_p.exists() else []
    iostat = parse_iostat(iostat_p, date_anchor) if iostat_p.exists() else []
    ifstat = parse_ifstat(ifstat_p, date_anchor) if ifstat_p.exists() else []

    def _to_rel_series(seq, k):
        ts = [auto_tz_offset(row[0], t0) for row in seq]
        xs = [to_rel(t, t0) for t in ts]
        ys = [row[k] for row in seq]
        # Filter out None/negative-large entries.
        return ([x for x in xs if x is not None],
                [y for x, y in zip(xs, ys) if x is not None])

    x_cpu, y_cpu_used = _to_rel_series(mpstat, 1)
    _,     y_cpu_iow  = _to_rel_series(mpstat, 2)
    x_io,  y_io_read  = _to_rel_series(iostat, 2)
    _,     y_io_wrtn  = _to_rel_series(iostat, 3)
    x_if,  y_if_in    = _to_rel_series(ifstat, 1)
    _,     y_if_out   = _to_rel_series(ifstat, 2)

    # Migration windows (relative seconds) for shading.
    raw_windows = find_per_source_migration_windows(expdir)
    migration_bands: list[tuple[float, float]] = []
    for src, p, d in raw_windows:
        ps = auto_tz_offset(p, t0)
        ds = auto_tz_offset(d, t0)
        if ps is None or ds is None:
            continue
        s_rel = to_rel(ps, t0)
        e_rel = to_rel(ds, t0)
        if s_rel is not None and e_rel is not None and e_rel > s_rel:
            migration_bands.append((s_rel, e_rel))

    workload = expdir.name
    for prefix in ("custom_reshard_v2_", "custom_reshard_"):
        if workload.startswith(prefix):
            workload = workload[len(prefix):]
            break

    fig, axes = plt.subplots(
        3, 1, figsize=(5.5, 5.4), sharex=True,
        gridspec_kw={"height_ratios": [1, 1, 1], "hspace": 1.1},
        facecolor=FIG_BG,
    )
    ax_cpu, ax_net, ax_disk = axes
    for ax in axes:
        _stylize_axes(ax)

    # Two distinct dash + marker patterns so paired lines remain visually
    # separable even when they overlap in value.
    PRIMARY = dict(color="#1a1a1a", linewidth=1.5, linestyle=(0, (5, 2)),
                   marker="o", markersize=6, markevery=4,
                   markerfacecolor="white", markeredgecolor="#1a1a1a",
                   markeredgewidth=1.2)
    SECONDARY = dict(color="#1a1a1a", linewidth=1.5, linestyle=(0, (1, 1.5)),
                     marker="^", markersize=7, markevery=4,
                     markerfacecolor="#1a1a1a", markeredgecolor="white",
                     markeredgewidth=0.7)

    # ---- (a) CPU --------------------------------------------------------------
    ax_cpu.plot(x_cpu, y_cpu_used, label="CPU used (%)", **PRIMARY)
    ax_cpu.plot(x_cpu, y_cpu_iow,  label="iowait (%)",  **SECONDARY)
    ax_cpu.set_ylabel("CPU (%)")
    ax_cpu.set_ylim(0, 100)
    ax_cpu.set_title(f"CPU utilization — {host}", pad=8)

    # ---- (b) Network ----------------------------------------------------------
    def _mb(v):
        return [x / 1024.0 for x in v]
    ax_net.plot(x_if, _mb(y_if_in),  label="RX (MB/s)", **PRIMARY)
    ax_net.plot(x_if, _mb(y_if_out), label="TX (MB/s)", **SECONDARY)
    ax_net.set_ylabel("Net Th/put (MB/s)")
    ax_net.set_title(f"Network — {host}", pad=8)
    ax_net.set_ylim(0, 35)
    ax_net.yaxis.set_major_locator(mticker.FixedLocator([0, 15, 30]))

    # ---- (c) Disk -------------------------------------------------------------
    ax_disk.plot(x_io, _mb(y_io_read), label="Read (MB/s)",  **PRIMARY)
    ax_disk.plot(x_io, _mb(y_io_wrtn), label="Write (MB/s)", **SECONDARY)
    ax_disk.set_ylabel("Disk (MB/s)")
    ax_disk.set_xlabel("Time since YCSB start (seconds)")
    ax_disk.set_title(f"Disk — {host}", pad=8)

    # Migration bands across all panels.
    for s_rel, e_rel in migration_bands:
        for ax in axes:
            ax.axvspan(s_rel, e_rel, alpha=0.20,
                       color=PHASE_COLORS["BAND_FILL"], zorder=1)

    # Clip x-axis to the YCSB run window so the resource plots align with the
    # YCSB throughput/latency timeline.
    t_rel = [(s.t - t0).total_seconds() for s in samples]
    if t_rel:
        for ax in axes:
            ax.set_xlim(PLOT_X_MIN, PLOT_X_MAX)

    # Footer legend — one per panel below each axis is noisy; use a single
    # figure-level legend that lists the per-panel colors and dashed/solid.
    handles_all = []
    labels_all  = []
    for ax in axes:
        h, l = ax.get_legend_handles_labels()
        for hi, li in zip(h, l):
            if li not in labels_all:
                handles_all.append(hi); labels_all.append(li)
    fig.legend(
        handles_all, labels_all,
        loc="lower center",
        bbox_to_anchor=(0.5, -0.10),
        ncol=min(len(labels_all), 6),
        frameon=False,
        handlelength=2.0,
        handletextpad=0.5,
        columnspacing=1.6,
    )

    plt.subplots_adjust(left=0.08, right=0.97, top=0.95, bottom=0.13)
    fig.align_ylabels(axes)
    for ax in axes:
        _bold_tick_labels(ax)
    plt.savefig(output, dpi=300, bbox_inches="tight")
    print(f"wrote {output}")
    print(f"  mpstat samples: {len(mpstat)}  iostat: {len(iostat)}  ifstat: {len(ifstat)}")


def plot_resources_combined(expdir: Path, output: Path, hosts: list[str]) -> None:
    """Single 3-panel figure overlaying CPU / Network / Disk for all hosts.
    One summary line per host per panel: CPU used %, total NIC MB/s (RX+TX),
    disk write MB/s. Migration bands shaded as in the YCSB plot."""
    ycsb_path = expdir / "ycsb" / "ycsb0" / "tmp" / "ycsb_output_ycsb0"
    if not ycsb_path.exists():
        cands = list(expdir.rglob("ycsb_output_*"))
        if not cands:
            sys.exit(f"No YCSB output file found under {expdir}")
        ycsb_path = cands[0]
    samples = parse_ycsb_log(ycsb_path)
    if not samples:
        sys.exit("No YCSB samples parsed; cannot anchor resource timeline")
    t0 = samples[0].t
    date_anchor = t0.replace(microsecond=0)

    # Per-host styles: distinct linestyle + marker, all grayscale.
    HOST_STYLES = [
        dict(linestyle="-",          marker="o", markersize=6,
             markerfacecolor="white", markeredgewidth=1.2),
        dict(linestyle=(0, (5, 2)),  marker="s", markersize=6,
             markerfacecolor="white", markeredgewidth=1.2),
        dict(linestyle=(0, (3, 1, 1, 1)), marker="^", markersize=7,
             markerfacecolor="#1a1a1a", markeredgecolor="white",
             markeredgewidth=0.7),
        dict(linestyle=(0, (1, 1.5)), marker="D", markersize=6,
             markerfacecolor="#1a1a1a", markeredgecolor="white",
             markeredgewidth=0.7),
    ]

    per_host: dict[str, dict] = {}
    for host in hosts:
        systat_dir = expdir / "logs" / host / "users" / "entall" / "systat_logs"
        if not systat_dir.is_dir():
            cands = list(expdir.rglob(f"{host}_mpstat.txt"))
            if not cands:
                print(f"WARN: no systat logs for {host}, skipping")
                continue
            systat_dir = cands[0].parent
        mpstat_p = systat_dir / f"{host}_mpstat.txt"
        iostat_p = systat_dir / f"{host}_iostat.txt"
        ifstat_p = systat_dir / f"{host}_ifstat.txt"

        mpstat = parse_mpstat(mpstat_p, date_anchor) if mpstat_p.exists() else []
        iostat = parse_iostat(iostat_p, date_anchor) if iostat_p.exists() else []
        ifstat = parse_ifstat(ifstat_p, date_anchor) if ifstat_p.exists() else []

        def _to_rel_series(seq, k):
            ts = [auto_tz_offset(row[0], t0) for row in seq]
            xs = [to_rel(t, t0) for t in ts]
            ys = [row[k] for row in seq]
            return ([x for x in xs if x is not None],
                    [y for x, y in zip(xs, ys) if x is not None])

        x_cpu, y_cpu_used = _to_rel_series(mpstat, 1)
        x_if,  y_if_in    = _to_rel_series(ifstat, 1)
        _,     y_if_out   = _to_rel_series(ifstat, 2)
        x_io,  y_io_wrtn  = _to_rel_series(iostat, 3)
        y_if_total = [a + b for a, b in zip(y_if_in, y_if_out)]
        per_host[host] = dict(
            x_cpu=x_cpu, y_cpu=y_cpu_used,
            x_if=x_if,   y_if=[v / 1024.0 for v in y_if_total],
            x_io=x_io,   y_io=[v / 1024.0 for v in y_io_wrtn],
            n_mp=len(mpstat), n_io=len(iostat), n_if=len(ifstat),
        )

    if not per_host:
        sys.exit("No host resource data found")

    raw_windows = find_per_source_migration_windows(expdir)
    migration_bands: list[tuple[float, float]] = []
    for src, p, d in raw_windows:
        ps = auto_tz_offset(p, t0)
        ds = auto_tz_offset(d, t0)
        if ps is None or ds is None:
            continue
        s_rel = to_rel(ps, t0)
        e_rel = to_rel(ds, t0)
        if s_rel is not None and e_rel is not None and e_rel > s_rel:
            migration_bands.append((s_rel, e_rel))

    workload = expdir.name
    for prefix in ("custom_reshard_v2_", "custom_reshard_"):
        if workload.startswith(prefix):
            workload = workload[len(prefix):]
            break

    # 3 panels side-by-side, each ≈ φ:1 (width:height).
    PHI = 1.618
    panel_h = 4.0
    panel_w = panel_h * PHI  # ≈ 6.47
    fig_w   = 3 * panel_w + 2 * 0.35 * panel_w + 0.8
    fig_h   = panel_h + 1.2  # room for x-label + footer legend
    fig, axes = plt.subplots(
        1, 3, figsize=(fig_w, fig_h), sharex=True,
        gridspec_kw={"width_ratios": [1, 1, 1], "wspace": 0.35},
        facecolor=FIG_BG,
    )
    ax_cpu, ax_net, ax_disk = axes
    for ax in axes:
        _stylize_axes(ax)

    for i, (host, d) in enumerate(per_host.items()):
        style = dict(HOST_STYLES[i % len(HOST_STYLES)])
        style.setdefault("markeredgecolor", "#1a1a1a")
        common = dict(color="#1a1a1a", linewidth=1.5,
                      markevery=max(1, 4 + i))
        ax_cpu.plot(d["x_cpu"], d["y_cpu"], label=host, **common, **style)
        ax_net.plot(d["x_if"],  d["y_if"],  label=host, **common, **style)
        ax_disk.plot(d["x_io"], d["y_io"], label=host, **common, **style)

    ax_cpu.set_ylabel("CPU used (%)")
    ax_cpu.set_ylim(0, 25)
    ax_cpu.yaxis.set_major_locator(mticker.FixedLocator([0, 10, 20]))
    ax_cpu.set_title("CPU Utilization", pad=8)
    ax_net.set_ylabel("Net total (MB/s)")
    ax_net.set_title("Network Throughput (YCSB Clients)", pad=8)
    ax_disk.set_ylabel("Disk write (MB/s)")
    ax_disk.set_title("Disk", pad=8)
    # Side-by-side: every panel needs its own x-axis label.
    for ax in axes:
        ax.set_xlabel("Time since YCSB start (seconds)")

    for s_rel, e_rel in migration_bands:
        for ax in axes:
            ax.axvspan(s_rel, e_rel, alpha=0.20,
                       color=PHASE_COLORS["BAND_FILL"], zorder=1)

    for ax in axes:
        ax.set_xlim(PLOT_X_MIN, PLOT_X_MAX)

    handles, labels = ax_cpu.get_legend_handles_labels()
    fig.legend(
        handles, labels,
        loc="lower center",
        bbox_to_anchor=(0.5, -0.10),
        ncol=min(len(labels), 6),
        frameon=False,
        handlelength=2.4,
        handletextpad=0.5,
        columnspacing=1.6,
    )

    plt.subplots_adjust(left=0.08, right=0.97, top=0.95, bottom=0.13)
    fig.align_ylabels(axes)
    for ax in axes:
        _bold_tick_labels(ax)
    plt.savefig(output, dpi=300, bbox_inches="tight")
    print(f"wrote {output}")
    for host, d in per_host.items():
        print(f"  {host}: mpstat={d['n_mp']} iostat={d['n_io']} ifstat={d['n_if']}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("expdir", type=Path,
                   help="experiment result directory under /tmp/experiments/")
    p.add_argument("--output", "-o", type=Path, default=None,
                   help="output path for YCSB plot (default: <expdir>/ycsb_timeseries.png)")
    p.add_argument("--with-resources", action="store_true",
                   help="also emit per-host CPU/network/disk plots from systat logs")
    p.add_argument("--hosts", nargs="+", default=["redis0", "redis3"],
                   help="hosts to plot resources for (default: redis0 redis3)")
    args = p.parse_args()

    expdir = args.expdir.resolve()
    if not expdir.is_dir():
        sys.exit(f"Not a directory: {expdir}")
    output = args.output or (expdir / "ycsb_timeseries.png")
    plot(expdir, output)

    if args.with_resources:
        ext = output.suffix or ".png"
        res_out = output.parent / f"resources{ext}"
        try:
            plot_resources_combined(expdir, res_out, hosts=args.hosts)
        except SystemExit as e:
            print(f"WARN: {e}")


if __name__ == "__main__":
    main()
