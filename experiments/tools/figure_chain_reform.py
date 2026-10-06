"""
Figure: how the recipient chain re-forms after a node dies.

(a) healthy: donor -> L -> F1 -> F2; L commits after F1's ack (L + F1 = 2 of 3).
(b) F1 dies: L drops it, forwards straight to F2, and waits for F2's ack.
(c) L dies: F1 is elected leader, the chain becomes F1 -> F2, the donor re-ships to F1.

Source: redis/src/cluster_rdma_chain.c (rdmaLeaderChainDropDeadHead, establish-time
compaction of dead followers), CRASH_SCENARIOS.md S1/S4.
"""

import os

import matplotlib.pyplot as plt
from matplotlib.patches import Circle, FancyArrowPatch

plt.rcParams.update({
    "font.family": "STIXGeneral",
    "mathtext.fontset": "stix",
    "font.size": 8,
    "pdf.fonttype": 42,
})

C_DONOR, C_RECP, C_DEAD = "#2b5d8a", "#a4531a", "#b5b5b5"
C_DATA, C_ACK, C_CRASH = "#c44400", "#2f7d32", "#c62828"
C_AE = "#8a8a8a"

X = {"D": 1.0, "L": 3.6, "F1": 6.2, "F2": 8.8}
Y0, R = 3.0, 0.72

fig, axes = plt.subplots(1, 3, figsize=(7.16, 2.0))


def node(ax, k, text, color, dead=False):
    ax.add_patch(Circle((X[k], Y0), R, fc="white" if not dead else "#f2f2f2",
                        ec=C_DEAD if dead else color, lw=1.4, zorder=3))
    ax.text(X[k], Y0, text, ha="center", va="center", fontsize=8.5,
            color=C_DEAD if dead else color, fontweight="bold", zorder=4)
    if dead:
        ax.plot(X[k], Y0, marker="X", ms=16, color=C_CRASH, mec="white",
                mew=1.0, alpha=0.9, zorder=5)


def link(ax, a, b, color, lw=1.6, rad=0.0, ls="-", label=None, ly=0.0):
    ax.add_patch(FancyArrowPatch((X[a], Y0), (X[b], Y0), arrowstyle="-|>",
                                 mutation_scale=9, lw=lw, color=color, ls=ls,
                                 connectionstyle=f"arc3,rad={rad}",
                                 shrinkA=R * 19, shrinkB=R * 19, zorder=2))
    if label:
        ax.text((X[a] + X[b]) / 2, Y0 + ly, label, ha="center", va="center",
                fontsize=7, color=color)


def ae(ax, leader, follower, rad):
    """Leader's AppendEntries to a follower, and the reply that carries the ack."""
    link(ax, leader, follower, C_AE, lw=0.7, rad=rad * 0.55, ls=(0, (1, 1.2)))
    link(ax, follower, leader, C_ACK, lw=1.1, rad=-rad, ls=(0, (2, 1)))
    d = abs(X[follower] - X[leader])
    ax.text((X[leader] + X[follower]) / 2, Y0 - max(rad * d / 2, R) - 0.55,
            "AE reply + ack", ha="center", va="center", fontsize=7, color=C_ACK)


def panel(ax, title, caption):
    ax.set_xlim(0, 9.8)
    ax.set_ylim(0, 6.2)
    ax.set_aspect("equal")
    ax.axis("off")
    ax.text(4.9, 5.9, title, ha="center", va="center", fontsize=8.5, fontweight="bold")
    ax.text(4.9, 0.25, caption, ha="center", va="center", fontsize=7.2,
            color="#333333", linespacing=1.1)


# (a) healthy
ax = axes[0]
panel(ax, "(a) healthy", "$F_1$ acks on its AppendEntries reply;\ncommit once $L$ + $F_1$ (2 of 3) hold it")
for k, t, c in [("D", "D", C_DONOR), ("L", "L", C_RECP),
                ("F1", "$F_1$", C_RECP), ("F2", "$F_2$", C_RECP)]:
    node(ax, k, t, c)
link(ax, "D", "L", C_DATA, lw=2.2, label="RDMA", ly=0.55)
link(ax, "L", "F1", C_DATA)
link(ax, "F1", "F2", C_DATA, lw=1.1)
ae(ax, "L", "F1", 0.6)

# (b) follower F1 dies
ax = axes[1]
panel(ax, "(b) follower $F_1$ fails", "$L$ skips $F_1$, forwards to $F_2$,\n"
      "commits after $F_2$'s ack")
node(ax, "D", "D", C_DONOR)
node(ax, "L", "L", C_RECP)
node(ax, "F1", "$F_1$", C_RECP, dead=True)
node(ax, "F2", "$F_2$", C_RECP)
link(ax, "D", "L", C_DATA, lw=2.2, label="RDMA", ly=0.55)
link(ax, "L", "F2", C_DATA, rad=-0.45, label="re-forward", ly=1.65)
ae(ax, "L", "F2", 0.45)

# (c) recipient leader L dies
ax = axes[2]
panel(ax, "(c) leader $L$ fails", "$F_1$ is elected leader, chain is $F_1 \\to F_2$,\n"
      "donor re-ships to $F_1$")
node(ax, "D", "D", C_DONOR)
node(ax, "L", "L", C_RECP, dead=True)
node(ax, "F1", "$L'$", C_RECP)
ax.text(X["F1"] + 0.35, Y0 + R + 0.35, "(was $F_1$)", ha="center", va="center",
        fontsize=6.5, color=C_RECP)
node(ax, "F2", "$F_2$", C_RECP)
link(ax, "D", "F1", C_DATA, lw=2.2, rad=-0.45, label="re-ship", ly=1.65)
link(ax, "F1", "F2", C_DATA)
ae(ax, "F1", "F2", 0.6)

fig.subplots_adjust(left=0.01, right=0.99, top=0.99, bottom=0.01, wspace=0.08)
out_dir = os.path.dirname(os.path.abspath(__file__))
for ext in ("pdf", "png"):
    out = os.path.join(out_dir, f"figure_chain_reform.{ext}")
    fig.savefig(out, dpi=300, bbox_inches="tight", pad_inches=0.03)
    print(f"wrote {out}")
