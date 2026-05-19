"""
Figure: Custom (aqueduct fork) RDMA-based slot migration protocol.

Two swim-lanes (source master, recipient migrate node), 5 protocol commands
shown left-to-right in temporal order. Arrows depict the control + data flow.
Colored side-bars show slot ownership state on each side at each phase.

Source: PHASE2_PLAN.md, PHASE2_5_PLAN.md, PHASE3_PLAN.md and
ansible/tasks/cluster/reshard_cluster_rdma.yml.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Rectangle

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.size": 9,
    "axes.linewidth": 0.6,
})

fig, ax = plt.subplots(figsize=(14, 6.5))
ax.set_xlim(0, 14)
ax.set_ylim(0, 8.5)
ax.set_aspect("equal")
ax.axis("off")

# ── Swim-lane backgrounds ────────────────────────────────────────────────
SRC_Y = 5.6   # source lane center
DST_Y = 1.6   # recipient lane center
LANE_H = 1.0

ax.add_patch(Rectangle((0.05, SRC_Y - LANE_H / 2), 13.9, LANE_H,
                       facecolor="#eef3f8", edgecolor="none", zorder=0))
ax.add_patch(Rectangle((0.05, DST_Y - LANE_H / 2), 13.9, LANE_H,
                       facecolor="#fdf0e6", edgecolor="none", zorder=0))

ax.text(0.30, SRC_Y, "SOURCE\nmaster\n(redis0)", fontsize=9.5,
        fontweight="bold", va="center", ha="left", color="#2b4f74")
ax.text(0.30, DST_Y, "RECIPIENT\nmigrate node\n(redis3)", fontsize=9.5,
        fontweight="bold", va="center", ha="left", color="#9a4a13")

# ── Time-step column positions ───────────────────────────────────────────
STEPS = [
    ("MIGRATE-PREP",  2.30, "bootstrap RDMA\ncontrol link"),
    ("RESHARD",       4.30, "register source MRs\n(1 MiB / slot)"),
    ("RESHARD-PRE",   6.30, "SETSLOT MIGRATING\n+ IMPORTING"),
    ("RESHARD-TRANSFER",  8.40, "RDMA-WRITE\nstaging → landing"),
    ("RESHARD-COMMIT",10.60, "SETSLOT NODE\n+ cluster broadcast"),
    ("(done)",       12.70, "MOVED redirects\nflip to recipient"),
]

# Vertical step markers across both lanes
for label, x, _sub in STEPS:
    ax.plot([x, x], [DST_Y - LANE_H / 2 - 0.25,
                     SRC_Y + LANE_H / 2 + 0.25],
            color="#bbbbbb", linewidth=0.6, linestyle=(0, (1, 2)), zorder=1)
    ax.text(x, SRC_Y + LANE_H / 2 + 0.95, label, fontsize=9,
            ha="center", fontweight="bold", color="#222222")
    ax.text(x, SRC_Y + LANE_H / 2 + 0.40, _sub, fontsize=7.5,
            ha="center", color="#555555")

# ── Per-step state pills on each lane ────────────────────────────────────
def pill(x, y, w, color, edge, text, txt_color):
    p = FancyBboxPatch((x - w/2, y - 0.24), w, 0.48,
                       boxstyle="round,pad=0.02,rounding_size=0.10",
                       facecolor=color, edgecolor=edge, linewidth=0.8, zorder=3)
    ax.add_patch(p)
    ax.text(x, y, text, fontsize=7.6, ha="center", va="center",
            color=txt_color, fontweight="bold")

# x positions for the five action steps (skip last "done" column for state pills)
xs = [s[1] for s in STEPS]

# SOURCE row state
pill(xs[0], SRC_Y, 1.65, "#cfe1f2", "#5d89af", "control link", "#1b3a5e")
pill(xs[1], SRC_Y, 1.65, "#9ec6e6", "#3a6a93", "MRs registered\nfor N slots", "#0f2741")
pill(xs[2], SRC_Y, 1.65, "#f6c89c", "#b56e2f", "slots MIGRATING\n(ASK-redirect)", "#522408")
pill(xs[3], SRC_Y, 1.65, "#f6c89c", "#b56e2f", "RDMA WRITE\n(burst)", "#522408")
pill(xs[4], SRC_Y, 1.65, "#cdb6e3", "#6c4a96", "ownership\nreleased", "#341c52")

# RECIPIENT row state
pill(xs[0], DST_Y, 1.65, "#fde5cd", "#c98a44", "landing bufs\npre-registered", "#522408")
pill(xs[1], DST_Y, 1.65, "#fde5cd", "#c98a44", "(idle)", "#7a5128")
pill(xs[2], DST_Y, 1.65, "#fde5cd", "#c98a44", "slots IMPORTING", "#522408")
pill(xs[3], DST_Y, 1.65, "#ffd297", "#c98a44", "DONE-SLOTS apply\n(dbAdd)", "#522408")
pill(xs[4], DST_Y, 1.65, "#cdb6e3", "#6c4a96", "ownership\nclaimed", "#341c52")

# ── Inter-lane arrows ────────────────────────────────────────────────────
def arrow(x1, y1, x2, y2, color, label=None, label_off=(0.0, 0.0),
          style="-|>", lw=1.1, mut=10, label_size=7.4):
    a = FancyArrowPatch((x1, y1), (x2, y2),
                        arrowstyle=style, mutation_scale=mut,
                        linewidth=lw, color=color, zorder=4)
    ax.add_patch(a)
    if label:
        mx, my = (x1 + x2) / 2 + label_off[0], (y1 + y2) / 2 + label_off[1]
        ax.text(mx, my, label, fontsize=label_size, color=color,
                ha="center", va="center", fontweight="bold")

# MIGRATE-PREP: 2-way handshake; recipient sends (VA, rkey) tuples back
arrow(xs[0] - 0.25, SRC_Y - 0.26, xs[0] - 0.25, DST_Y + 0.26, "#345d80",
      label="control init", label_off=(-0.70, 0))
arrow(xs[0] + 0.25, DST_Y + 0.26, xs[0] + 0.25, SRC_Y - 0.26, "#7c5025",
      label="VA, rkey", label_off=(0.80, 0))

# RESHARD-PRE: SETSLOT MIGRATING + IMPORTING — driven from ansible/orchestrator
# (driver), but shows up as cluster-state side-effects on both lanes
arrow(xs[2], SRC_Y - 0.26, xs[2], DST_Y + 0.26, "#a16524",
      label="(orchestrator-driven\nSETSLOT calls)", label_off=(1.05, 0),
      label_size=6.8)

# RESHARD-TRANSFER: BIG RDMA-WRITE arrow (bypasses CPU on recipient)
exec_arrow = FancyArrowPatch((xs[3], SRC_Y - 0.26), (xs[3], DST_Y + 0.26),
                             arrowstyle="-|>", mutation_scale=20,
                             linewidth=3.5, color="#c44400", zorder=4)
ax.add_patch(exec_arrow)
ax.text(xs[3] - 0.95, (SRC_Y + DST_Y) / 2 + 0.3,
        "RDMA WRITE",
        fontsize=8.5, color="#c44400", ha="right", va="center", fontweight="bold")
ax.text(xs[3] - 0.95, (SRC_Y + DST_Y) / 2 - 0.25,
        "zero-copy,\nno recipient CPU",
        fontsize=7.0, color="#c44400", ha="right", va="center", style="italic")

# DONE-SLOTS: small follow-up control message after the RDMA burst
arrow(xs[3] + 0.55, SRC_Y - 0.26, xs[3] + 0.55, DST_Y + 0.26, "#9a4a13",
      label="DONE-SLOTS\n(apply trigger)", label_off=(1.05, 0),
      label_size=6.8)

# RESHARD-COMMIT: SETSLOT NODE on both, then cluster-wide gossip
arrow(xs[4], SRC_Y - 0.26, xs[4], DST_Y + 0.26, "#6c4a96",
      label="SETSLOT NODE\n+ gossip", label_off=(1.05, 0),
      label_size=6.8)

# After-commit: client redirects
ax.annotate("", xy=(STEPS[5][1] + 0.6, SRC_Y),
            xytext=(STEPS[5][1] - 0.55, SRC_Y),
            arrowprops=dict(arrowstyle="-|>", color="#444444", lw=1.0))
ax.text(STEPS[5][1] + 0.75, SRC_Y + 0.10,
        "client GET k → -MOVED slot recipient:port",
        fontsize=7.4, ha="left", color="#444444", style="italic")

# ── Title & footer ───────────────────────────────────────────────────────
ax.text(7.0, 8.15, "Custom (aqueduct fork) RDMA slot-migration protocol",
        fontsize=12.5, fontweight="bold", ha="center")
ax.text(7.0, 0.35,
        "Source pre-registers staging MRs once (RESHARD) and bursts slot data via one RDMA-WRITE per slot (RESHARD-TRANSFER). "
        "Ownership flip is split out (RESHARD-PRE / RESHARD-COMMIT) so client redirects switch atomically only after the data has landed.",
        fontsize=7.8, ha="center", color="#555555", style="italic", wrap=True)

plt.tight_layout()
import os
out = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "figure_custom_migration_protocol.png")
plt.savefig(out, dpi=180, bbox_inches="tight")
print(f"wrote {out}")
