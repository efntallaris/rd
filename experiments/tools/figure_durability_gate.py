"""
Figure: commit-on-timeout vs commit-on-ack for the migration durability entry.

(a) Before: the recipient leader waited up to 5 s for the chain ack, then logged
    RECP_DURABLE anyway. The follower's ack came after its merge of the batch, later
    than 5 s, so in every clean run RECP_DURABLE committed while only L held the batch.
    A leader crash in that window loses data the log says is durable.
(b) After: the follower acks as soon as the batch lands in its pool (on its
    AppendEntries reply) and L logs RECP_DURABLE only after that ack. With no ack, L
    re-forms the chain instead of committing.

Source: CRASH_SCENARIOS.md (EMPIRICAL: invariant violated in the clean baseline),
FAULT_TOLERANCE_STATUS.md §1b-1c (Part A real-ack gating),
redis/src/cluster_rdma_chain.c (ack on landing).
"""

import os

import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, Rectangle

plt.rcParams.update({
    "font.family": "STIXGeneral",
    "mathtext.fontset": "stix",
    "font.size": 8,
    "pdf.fonttype": 42,
})

C_RECP, C_DATA, C_ACK, C_BAD = "#a4531a", "#c44400", "#2f7d32", "#c62828"

fig, ax = plt.subplots(figsize=(7.16, 2.7))
ax.axis("off")
ax.set_xlim(0, 100)
ax.set_ylim(-3, 44)

X0, X1 = 14, 99


def lanes(yl, yf):
    for y, name in [(yl, "Leader $L$"), (yf, "Follower $F_1$")]:
        ax.plot([X0, X1], [y, y], color=C_RECP, lw=0.8, zorder=1)
        ax.text(X0 - 1, y, name, ha="right", va="center", color=C_RECP,
                fontweight="bold")


def arrow(x1, y1, x2, y2, color, lw=1.0, ls="-"):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>",
                                 mutation_scale=7, lw=lw, color=color,
                                 linestyle=ls, zorder=4))


def box(x, y, text):
    ax.text(x, y, text, ha="center", va="center", color="white", fontsize=6.8,
            family="monospace", zorder=7,
            bbox=dict(boxstyle="round,pad=0.3,rounding_size=0.3", fc="#262626", ec="none"))


def bar(x1, x2, y, text, fc):
    ax.add_patch(Rectangle((x1, y - 1.1), x2 - x1, 2.2, fc=fc,
                           ec="none", zorder=2))
    ax.text((x1 + x2) / 2, y, text, ha="center", va="center", fontsize=6.8,
            color="#4a2408", zorder=3)


def text(x, y, s, color="#333333", ha="left", size=7.2, **kw):
    ax.text(x, y, s, ha=ha, va="center", fontsize=size, color=color, zorder=8, **kw)


# (a) before: commit on timeout
LA, FA = 33, 24
ax.text(0.5, 42.5, "(a) before: commit on a 5 s timeout", fontsize=8.5,
        fontweight="bold", va="center")
lanes(LA, FA)
arrow(18, LA - 1.3, 20, FA + 1.3, C_DATA, lw=1.8)
text(20.6, 28.5, "chain forward", C_DATA)
bar(20, 76, FA, "receive + merge the batch, then ack", "#ecd6c4")
# 5 s timer
ax.annotate("", xy=(58, LA + 3.2), xytext=(18, LA + 3.2),
            arrowprops=dict(arrowstyle="|-|", lw=0.7, color="#555555",
                            shrinkA=0, shrinkB=0, mutation_scale=2))
text(38, LA + 5.0, "wait for ack, at most 5 s", "#555555", ha="center")
box(61.5, LA, "RECP_DURABLE")
text(61.5, LA - 3.3, "fired anyway", C_BAD, ha="center", style="italic")
arrow(77, FA + 1.3, 79, LA - 1.3, C_ACK, lw=1.0, ls=(0, (2, 1)))
text(79.6, 28.5, "ack (too late)", C_ACK)
# vulnerability window
ax.add_patch(Rectangle((65.5, LA - 1.6), 13.5, 3.2, fc=C_BAD, alpha=0.18,
                       ec="none", zorder=1))
ax.text(81.0, LA + 3.6, "only $L$ holds the batch;\na crash of $L$ here loses it",
        ha="left", va="center", fontsize=6.8, color=C_BAD, linespacing=1.0)

# (b) after: commit on the real ack
LB, FB = 10, 1
ax.text(0.5, 18.5, "(b) after: commit on the real ack", fontsize=8.5,
        fontweight="bold", va="center")
lanes(LB, FB)
arrow(18, LB - 1.3, 20, FB + 1.3, C_DATA, lw=1.8)
text(20.6, 5.5, "chain forward", C_DATA)
bar(20, 31, FB, "land in pool", "#dcb393")
bar(31, 76, FB, "merge (background)", "#f5e9de")
arrow(32, FB + 1.3, 34, LB - 1.3, C_ACK, lw=1.0, ls=(0, (2, 1)))
text(34.6, 5.5, "ack on AE reply", C_ACK)
box(42.5, LB, "RECP_DURABLE")
text(47.5, LB + 2.9, "a majority ($L$ + $F_1$) holds the batch", "#1f5a22")
text(47.5, LB - 3.0, "no ack $\\Rightarrow$ re-form the chain, never commit on a timeout",
     "#444444", style="italic", size=6.8)

out_dir = os.path.dirname(os.path.abspath(__file__))
for ext in ("pdf", "png"):
    out = os.path.join(out_dir, f"figure_durability_gate.{ext}")
    fig.savefig(out, dpi=300, bbox_inches="tight", pad_inches=0.03)
    print(f"wrote {out}")
