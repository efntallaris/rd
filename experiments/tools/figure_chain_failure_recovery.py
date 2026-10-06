"""
Figure: AqRaft migration with chain replication and where crashes are recovered.

One donor session: donor leader D ships to recipient leader L, L forwards down the
chain L -> F1 -> F2, F1's ack (L + F1 = majority) lets L commit INDX_UPD. Crashes
before INDX_UPD are repaired by the donor re-shipping; after it the data is durable.
"""

import os

import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Rectangle

plt.rcParams.update({
    "font.family": "STIXGeneral",
    "mathtext.fontset": "stix",
    "font.size": 8,
    "pdf.fonttype": 42,
})

C_DONOR, C_RECP = "#2b5d8a", "#a4531a"
C_DATA, C_ACK, C_CRASH = "#c44400", "#2f7d32", "#c62828"

fig, ax = plt.subplots(figsize=(7.16, 2.5))
ax.axis("off")
ax.set_xlim(0, 100)
ax.set_ylim(-6.5, 36)

LANES = [("D", 32, "Donor leader", C_DONOR),
         ("L", 21, "Recipient leader", C_RECP),
         ("F1", 11, "Follower $F_1$", C_RECP),
         ("F2", 2, "Follower $F_2$", C_RECP)]
Y = {k: y for k, y, *_ in LANES}
X0, X1 = 14, 99.5
for k, y, name, col in LANES:
    ax.plot([X0, X1], [y, y], color=col, lw=0.8)
    ax.text(X0 - 1, y, name, ha="right", va="center", color=col, fontweight="bold")


def arrow(x1, k1, x2, k2, color="#444444", lw=0.8, ls="-"):
    s = -1 if Y[k2] < Y[k1] else 1
    ax.add_patch(FancyArrowPatch((x1, Y[k1] + 1.5 * s), (x2, Y[k2] - 1.5 * s),
                                 arrowstyle="-|>", mutation_scale=7, lw=lw,
                                 color=color, linestyle=ls, zorder=4))


def logbox(x, k, text):
    w = 0.95 * len(text) + 1.8
    ax.add_patch(FancyBboxPatch((x - w / 2, Y[k] - 1.4), w, 2.8,
                                boxstyle="round,pad=0.05,rounding_size=0.5",
                                fc="#262626", ec="none", zorder=6))
    ax.text(x, Y[k], text, ha="center", va="center", color="white",
            fontsize=6.8, family="monospace", zorder=7)


def label(x, y, text, color="#444444", ha="left"):
    ax.text(x, y, text, ha=ha, va="center", color=color, fontsize=7.2, zorder=8)


# Normal path
logbox(20, "D", "TXN_START")
arrow(24, "D", 26, "L")
logbox(31, "L", "RECP_TXN_START")
arrow(39, "D", 41, "L", C_DATA, lw=2.0)
label(41.6, 26.5, "RDMA write", C_DATA)
arrow(42.5, "L", 44.5, "F1", C_DATA, lw=1.5)
label(45.2, 16.0, "chain", C_DATA)
arrow(45.5, "F1", 47.5, "F2", C_DATA, lw=1.1)
arrow(59, "F1", 61, "L", C_ACK, lw=1.0, ls=(0, (2, 1)))
label(61.6, 16.0, "ack", C_ACK)
logbox(66, "L", "INDX_UPD")
logbox(79.5, "L", "RECP_TXN_DONE")
arrow(87, "L", 88.5, "D")
logbox(91, "D", "TXN_DONE")

# Durability split at INDX_UPD
ax.add_patch(Rectangle((X0, -5.8), 66 - X0, 3.4, fc="#f3dcdc", ec="none"))
ax.add_patch(Rectangle((66, -5.8), X1 - 66, 3.4, fc="#dcefdc", ec="none"))
ax.plot([66, 66], [-5.8, Y["L"] - 1.4], color="#555555", lw=0.6, ls=(0, (2, 2)))
label((X0 + 66) / 2, -4.1, "not yet durable: the donor re-ships", "#7a1f1f", "center")
label((66 + X1) / 2, -4.1, "durable on a majority", "#1f5a22", "center")


# Crashes
def crash(x, k, text, dy, ha="left", dx=1.2):
    ax.plot(x, Y[k], marker="X", ms=8, color=C_CRASH, mec="white", mew=0.8, zorder=10)
    label(x + dx, Y[k] + dy, text, C_CRASH, ha)


crash(47, "D", r"$\bf{S2}$ donor leader: new donor leader resumes", 2.8)
crash(46, "L", r"$\bf{S1}$ recipient leader: $F_1$ takes over, donor re-ships", 3.3)
crash(50, "F1", r"$\bf{S4}$ follower: chain skips $F_1$", -2.8)
crash(97.5, "D", r"$\bf{S5}$ after done: no-op", 2.8, "right", dx=1.5)

out_dir = os.path.dirname(os.path.abspath(__file__))
for ext in ("pdf", "png"):
    out = os.path.join(out_dir, f"figure_chain_failure_recovery.{ext}")
    fig.savefig(out, dpi=300, bbox_inches="tight", pad_inches=0.03)
    print(f"wrote {out}")
