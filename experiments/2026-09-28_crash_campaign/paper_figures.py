#!/usr/bin/env python3
"""Paper versions of the final crash-campaign figures.

  crash_timelines.{pdf,png}   S1-S6 throughput around the migration, HEALTHY underneath
  recovery_breakdown.{pdf,png} kill -> new leader -> first recovery action -> all durable

Usage: paper_figures.py <campaign dir> [out dir]
Reuses final_analysis.py's log parsing (everything before it writes its outputs) and
reads figures/final/recovery_times.json.
"""
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

here = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else ".")
out_dir = sys.argv[2] if len(sys.argv) > 2 else os.path.join(here, "figures", "paper")
os.makedirs(out_dir, exist_ok=True)

src = open(os.path.join(here, "final_analysis.py")).read()
g = {"__name__": "final_analysis"}
sys.argv = ["final_analysis.py", here]
exec(src[:src.index("json.dump(")], g)
M = g["metrics"]

plt.rcParams.update({
    "font.family": "STIXGeneral",
    "mathtext.fontset": "stix",
    "font.size": 8,
    "axes.linewidth": 0.6,
    "axes.edgecolor": "#8a8984",
    "axes.labelcolor": "#2b2b2a",
    "xtick.color": "#52514e",
    "ytick.color": "#52514e",
    "xtick.major.width": 0.6,
    "ytick.major.width": 0.6,
    "xtick.major.size": 2.5,
    "ytick.major.size": 2.5,
    "pdf.fonttype": 42,
})

INK, MUTED = "#0b0b0b", "#52514e"
SERIES = "#2a78d6"      # categorical slot 1
REF = "#bdbcb6"         # HEALTHY reference, neutral
KILL = "#e34948"        # slot 8 red, used only for the crash marker
PHASES = ["#2a78d6", "#1baf7a"]   # categorical slots 1 and 3

TITLES = {"s1": ("S1", "recipient leader"), "s2": ("S2", "donor leader (orchestrator)"),
          "s3": ("S3", "donor follower"), "s4": ("S4", "recipient follower"),
          "s5": ("S5", "donor leader, after its transfer"), "s6": ("S6", "donor + recipient leaders")}
T0, T1 = -8, 32          # seconds around migration start


def aligned(run):
    r = M[run]
    m0 = r["_mig_rel"][0]
    return [(x - m0, y) for x, y in r["_series"] if T0 - 1 <= x - m0 <= T1 + 1], \
           [k - m0 for k in r["_kills_rel"]]


def recovered_at(series, kill, pre):
    """First second after the kill from which throughput stays >= 90% of the
    pre-migration level for 3 samples."""
    pts = [(x, y) for x, y in series if x >= kill]
    for i in range(len(pts) - 2):
        if all(y >= 0.9 * pre for _, y in pts[i:i + 3]):
            return pts[i][0]
    return None


def style(ax):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.grid(axis="y", color="#e6e5e0", lw=0.6)
    ax.set_axisbelow(True)


# ── Figure 1: throughput timelines ─────────────────────────────────────
healthy, _ = aligned("healthy")
runs = [r for r in ("s1", "s2", "s3", "s4", "s5", "s6") if r in M]
fig, axs = plt.subplots(3, 2, figsize=(7.16, 4.3), sharex=True, sharey=True)
for ax, run in zip(axs.flat, runs):
    style(ax)
    series, kills = aligned(run)
    pre = M[run]["tput_pre_kops"]
    ax.plot(*zip(*healthy), color=REF, lw=1.3, solid_capstyle="round", zorder=2)
    ax.plot(*zip(*series), color=SERIES, lw=1.5, solid_capstyle="round", zorder=3)
    for k in kills:
        ax.axvline(k, color=KILL, lw=0.9, zorder=4)
    kx = kills[0]
    rec = recovered_at(series, kills[-1], pre)
    if rec is not None:
        ax.axvspan(kx, rec, color=KILL, alpha=0.08, lw=0, zorder=1)
        ax.text((kx + rec) / 2 if rec - kx > 6 else rec + 0.6, 203, f"{rec - kx:.0f} s",
                ha="center" if rec - kx > 6 else "left", va="center", fontsize=7.4,
                color=INK, zorder=6,
                bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.85))
    tag, what = TITLES[run]
    ax.set_title(f"$\\bf{{{tag}}}$  {what}", loc="left", fontsize=8.2, color=INK, pad=5)
    ax.set_xlim(T0, T1)
    ax.set_ylim(0, 210)
    ax.set_yticks([0, 100, 200])
    ax.set_xticks(range(-5, T1 + 1, 5))
for ax in axs[-1]:
    ax.set_xlabel("seconds since migration start")
for ax in axs[:, 0]:
    ax.set_ylabel("Kops/s")
fig.legend(handles=[Line2D([], [], color=SERIES, lw=1.5, label="run with crash"),
                    Line2D([], [], color=REF, lw=1.3, label="HEALTHY (no crash)"),
                    Line2D([], [], color=KILL, lw=0.9, label="node killed"),
                    plt.Rectangle((0, 0), 1, 1, color=KILL, alpha=0.15,
                                  label="until back to 90% of pre-migration")],
           loc="upper center", ncol=4, frameon=False, fontsize=7.8,
           bbox_to_anchor=(0.5, 1.03), handlelength=1.8)
fig.tight_layout(h_pad=0.9, w_pad=1.2, rect=(0, 0, 1, 0.97))
for ext in ("pdf", "png"):
    fig.savefig(os.path.join(out_dir, f"crash_timelines.{ext}"), dpi=300, bbox_inches="tight",
                pad_inches=0.02)
plt.close(fig)

# ── Figure 2: recovery breakdown ───────────────────────────────────────
R = json.load(open(os.path.join(here, "figures", "final", "recovery_times.json")))
order = [r for r in ("s1", "s2", "s5", "s6") if r in R]
fig, ax = plt.subplots(figsize=(3.4, 1.75))
for s in ("top", "right", "left"):
    ax.spines[s].set_visible(False)
ax.grid(axis="x", color="#e6e5e0", lw=0.6)
ax.set_axisbelow(True)
for i, run in enumerate(order):
    r = R[run]
    el = max(r["new_leader_s"].values()) if r["new_leader_s"] else 0
    ad = max(r["all_durable_s"] or el, el)
    ax.barh(i, el, height=0.56, color=PHASES[0], edgecolor="white", lw=0.8)
    ax.barh(i, ad - el, left=el, height=0.56, color=PHASES[1], edgecolor="white", lw=0.8)
    ax.text(ad + 1.2, i, f"{ad:.1f} s", va="center", fontsize=7.4, color=INK)
labels = [f"$\\bf{{{R[r]['tag']}}}$ {TITLES[r][1]}" for r in order]
ax.set_yticks(range(len(order)))
ax.set_yticklabels(labels, fontsize=7.4)
ax.tick_params(axis="y", length=0)
ax.invert_yaxis()
ax.set_xlim(0, max(R[r]["all_durable_s"] for r in order) * 1.14)
ax.set_xlabel("seconds after the kill")
ax.legend(handles=[plt.Rectangle((0, 0), 1, 1, color=c) for c in PHASES],
          labels=["kill $\\to$ new leader", "$\\to$ all ranges durable"],
          loc="lower center", bbox_to_anchor=(0.42, 1.0), ncol=2, frameon=False,
          fontsize=7.0, handlelength=1.0, handleheight=0.8, columnspacing=0.9,
          handletextpad=0.4)
fig.tight_layout()
for ext in ("pdf", "png"):
    fig.savefig(os.path.join(out_dir, f"recovery_breakdown.{ext}"), dpi=300,
                bbox_inches="tight", pad_inches=0.02)
print("wrote", out_dir)
