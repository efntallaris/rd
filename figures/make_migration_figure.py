#!/usr/bin/env python3
"""Generate the AqRaft slot-migration protocol figure.

Two stacked panels in one figure:
  A) phase timeline + slot-state swimlanes (donor / recipient)
  B) data-plane architecture: leader RDMA-backpatch vs follower chain replication

Outputs PDF (vector, for the paper), SVG, and PNG into figures/.
Run: python3 figures/make_migration_figure.py
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from matplotlib.lines import Line2D

# ---- palette ----------------------------------------------------------------
C_DONOR   = "#2f6db0"   # donor shardgroup
C_RECIP   = "#2e8b57"   # recipient shardgroup
C_RAFT    = "#b0852f"   # raft log / consensus overlay
C_RDMA    = "#c0392b"   # RDMA bulk-data arrows
C_TCP     = "#555555"   # TCP control-plane arrows
C_BOXFILL = "#f4f7fb"
C_BOXFILL2= "#f1faf4"
INK       = "#1a1a1a"

def box(ax, x, y, w, h, text, ec, fc=C_BOXFILL, fs=9, bold=False, tc=INK):
    p = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4,rounding_size=2",
                       linewidth=1.6, edgecolor=ec, facecolor=fc, zorder=2)
    ax.add_patch(p)
    ax.text(x + w/2, y + h/2, text, ha="center", va="center",
            fontsize=fs, color=tc, zorder=3,
            fontweight=("bold" if bold else "normal"))

def arrow(ax, p0, p1, color, style="-", lw=2.2, label=None, lpos=0.5,
          loff=(0, 1.6), rad=0.0, fs=8, dash=None):
    a = FancyArrowPatch(p0, p1, arrowstyle="-|>", mutation_scale=16,
                        linewidth=lw, color=color, zorder=4,
                        connectionstyle=f"arc3,rad={rad}",
                        linestyle=(dash if dash else style))
    ax.add_patch(a)
    if label:
        mx = p0[0] + (p1[0]-p0[0])*lpos + loff[0]
        my = p0[1] + (p1[1]-p0[1])*lpos + loff[1]
        ax.text(mx, my, label, ha="center", va="center", fontsize=fs,
                color=color, zorder=5,
                bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.85))

fig = plt.figure(figsize=(12.5, 13.5))
gs = fig.add_gridspec(2, 1, height_ratios=[1.0, 2.25], hspace=0.16)

# =============================================================================
# PANEL A — phase timeline
# =============================================================================
axA = fig.add_subplot(gs[0])
axA.set_xlim(0, 100); axA.set_ylim(0, 42); axA.axis("off")
axA.set_title("A.  Migration phases & slot states (time →)",
              loc="left", fontsize=12, fontweight="bold", color=INK)

phases = ["TXN_START", "PREP", "REGISTER", "FLIP", "TRANSFER", "BACKPATCH", "DONE"]
xs = [4, 17, 31, 45, 60, 76, 92]
for name, x in zip(phases, xs):
    axA.text(x, 39, name, ha="center", va="center", fontsize=8.5,
             fontweight="bold", color=INK)
    axA.plot([x, x], [6, 35], color="#cccccc", lw=0.8, ls=":", zorder=0)

# donor slot-state bar
def seg(ax, x0, x1, y, h, color, label, tc="white"):
    ax.add_patch(plt.Rectangle((x0, y), x1-x0, h, facecolor=color,
                               edgecolor="white", lw=1.2, zorder=2))
    ax.text((x0+x1)/2, y+h/2, label, ha="center", va="center",
            fontsize=8, color=tc, fontweight="bold", zorder=3)

axA.text(-1, 27.5, "donor", ha="right", va="center", fontsize=9,
         fontweight="bold", color=C_DONOR)
seg(axA, 2, 45, 24, 7, "#7fa9d4", "STABLE", tc=INK)
seg(axA, 45, 60, 24, 7, C_DONOR, "MIGRATING")
seg(axA, 60, 96, 24, 7, "#1f4e79", "MIGRATED")

axA.text(-1, 15.5, "recipient", ha="right", va="center", fontsize=9,
         fontweight="bold", color=C_RECIP)
seg(axA, 17, 45, 12, 7, C_RECIP, "MIGRATING")
seg(axA, 45, 96, 12, 7, "#1d6b40", "STABLE (now OWNER)")

# raft-log markers
for x, t in [(4, "TXN_START"), (76, "MGN_INDX_UPD"), (92, "TXN_DONE")]:
    axA.plot(x, 4.5, marker="v", ms=9, color=C_RAFT, zorder=4)
    axA.text(x, 1.2, t, ha="center", va="center", fontsize=7, color=C_RAFT)
axA.text(50, 4.5,
         "▼ = raft log entry (RAFT.MGN-LOG, replicated by AppendEntries)",
         ha="center", va="center", fontsize=7.5, color=C_RAFT, style="italic")

# FLIP callout — placed in the clear gap between the donor and recipient bars
axA.annotate("FLIP: ownership moves BEFORE the bytes do",
             xy=(45, 23.8), xytext=(70, 21), fontsize=7.5, color=INK,
             ha="center", va="center",
             arrowprops=dict(arrowstyle="->", color=INK, lw=1))

# =============================================================================
# PANEL B — data-plane architecture
# =============================================================================
axB = fig.add_subplot(gs[1])
axB.set_xlim(0, 100); axB.set_ylim(0, 100); axB.axis("off")
axB.set_title("B.  Recipient-group data paths:  leader RDMA-backpatch  vs  follower chain replication",
              loc="left", fontsize=12, fontweight="bold", color=INK)

# --- donor shardgroup container ---
axB.add_patch(FancyBboxPatch((3, 58), 40, 38,
              boxstyle="round,pad=0.6,rounding_size=3", linewidth=1.4,
              edgecolor=C_DONOR, facecolor="#eef4fb", zorder=1, linestyle="--"))
axB.text(5, 92.5, "DONOR shardgroup", fontsize=10, fontweight="bold", color=C_DONOR)
box(axB, 6, 78, 34, 11,
    "donor LEADER  (migrationWorker)\nsnapshot of migrating slots", C_DONOR, bold=False)
box(axB, 6, 61, 15, 9, "donor\nfollower", C_DONOR, fc="white", fs=8)
box(axB, 25, 61, 15, 9, "donor\nfollower", C_DONOR, fc="white", fs=8)
# raft AE within donor
arrow(axB, (15, 78), (13.5, 70.2), C_RAFT, lw=1.3, rad=0.0)
arrow(axB, (28, 78), (32, 70.2), C_RAFT, lw=1.3, rad=0.0)
axB.text(23, 74.5, "RAFT.MGN-LOG\nTXN_START / TXN_DONE", ha="center",
         va="center", fontsize=7, color=C_RAFT)

# --- recipient shardgroup container ---
axB.add_patch(FancyBboxPatch((52, 4), 46, 92,
              boxstyle="round,pad=0.6,rounding_size=3", linewidth=1.4,
              edgecolor=C_RECIP, facecolor="#edf7f1", zorder=1, linestyle="--"))
axB.text(54, 92.5, "RECIPIENT shardgroup", fontsize=10, fontweight="bold", color=C_RECIP)
box(axB, 55, 74, 40, 15,
    "recipient LEADER\nlanding pool → backpatch (shadow→live) → kvstore\n"
    "on BACKPATCH_DONE: chainForwardWorker\n+ MGN_INDX_UPD (raft AE)",
    C_RECIP, fs=8.2)
box(axB, 55, 44, 18, 11, "recipient F1\napply local", C_RECIP, fc="white", fs=8)
box(axB, 77, 44, 18, 11, "recipient F2 (TAIL)\napply local", C_RECIP, fc="white", fs=8)

# --- cross-group arrows ---
# RDMA bulk write donor leader -> recipient leader
arrow(axB, (40, 84.5), (55, 83), C_RDMA, lw=2.6,
      label="(1) RDMA-WRITE bulk data  [TRANSFER]", lpos=0.5, loff=(0, 2.4), fs=8)
# backpatch-status poll back (TCP, dashed)
arrow(axB, (55, 79), (40, 80.5), C_TCP, lw=1.8, dash=(0, (5, 3)),
      label="(2) BACKPATCH-STATUS poll  (TCP, donor pulls)", lpos=0.5,
      loff=(0, -2.6), fs=7.5)

# chain: leader -> F1 -> tail
arrow(axB, (66, 74), (64, 55.2), C_RDMA, lw=2.6, rad=0.0)
arrow(axB, (73, 49.5), (77, 49.5), C_RDMA, lw=2.6)
axB.text(75, 60, "RDMA chain-forward\n+ CHAIN-FORWARDED (TCP)",
         ha="center", va="center", fontsize=7.5, color=C_RDMA)
# chain-ack tail -> leader (dashed TCP)
arrow(axB, (86, 55), (80, 74), C_TCP, lw=1.8, dash=(0, (5, 3)), rad=0.25,
      label="CHAIN-ACK\n(tail → leader)", lpos=0.55, loff=(9, 0), fs=7.5)
# MGN_INDX_UPD raft AE to recipient followers
arrow(axB, (60, 74), (60, 55.2), C_RAFT, lw=1.3, rad=-0.15)
axB.text(50.5, 64, "MGN_INDX_UPD\n(raft AE)", ha="center", va="center",
         fontsize=7, color=C_RAFT, rotation=90)

# --- takeaway band ---
axB.add_patch(plt.Rectangle((3, 0.5), 95, 8, facecolor="#fbf7ec",
              edgecolor=C_RAFT, lw=1.0, zorder=1))
axB.text(50, 4.5,
         "Leader gets bytes via RDMA backpatch.  Followers get the SAME bytes via the RDMA chain.\n"
         "The raft log carries only “session-durable” metadata — never the key/value data.",
         ha="center", va="center", fontsize=8.5, color=INK)

# --- legend ---
legend_elems = [
    Line2D([0], [0], color=C_RDMA, lw=2.6, label="RDMA bulk data"),
    Line2D([0], [0], color=C_TCP, lw=1.8, ls=(0, (5, 3)), label="TCP control"),
    Line2D([0], [0], color=C_RAFT, lw=1.3, label="raft AppendEntries"),
]
axB.legend(handles=legend_elems, loc="upper center", ncol=3, frameon=True,
           fontsize=8, bbox_to_anchor=(0.5, 1.0))

fig.suptitle("AqRaft slot migration  (redisraft + aqueduct RDMA + chain replication)",
             fontsize=14, fontweight="bold", y=0.995)

import os
out = os.path.dirname(os.path.abspath(__file__))
for ext in ("pdf", "svg", "png"):
    fig.savefig(os.path.join(out, f"aqraft_migration.{ext}"),
                bbox_inches="tight", dpi=200)
print("wrote aqraft_migration.{pdf,svg,png} to", out)
