#!/usr/bin/env python3
"""Sequence diagrams of roll-forward recovery in the 3 -> 6 scale-out: what the donor leader and
the recipient leader do after (A) the recipient leader, (B) the donor leader dies mid-round.
Steps and times are read from the server logs of two runs of 2026-10-03
(/tmp/lincheck/scaleout3to6/final6/S1_qyes_20261003_162215 and S2_qyes_20261003_162739).
usage: recovery_diagrams.py <out_dir>"""
import sys, textwrap
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

INK, MUTED, DEAD = "#1a1a1a", "#666666", "#c0392b"
DONOR, RECIP, FOLL = "#fbe9d7", "#dcebf7", "#eeeeee"
EDGE = {DONOR: "#c96a12", RECIP: "#1f6fad", FOLL: "#888888"}
X = [2.3, 5.9, 9.5, 13.1]
NOTE_W, WRAP = 3.2, 38
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9})


def draw(title, subtitle, lanes, events, out):
    # first pass: heights
    y, rows = 0.0, []
    for e in events:
        if e[0] == "note":
            lines = textwrap.wrap(e[3], WRAP)
            h = 0.30 * len(lines) + 0.22
        elif e[0] in ("msg", "data"):
            lines, h = None, 0.62 + (0.42 if len(e) > 5 and e[5] else 0)
        elif e[0] == "crash":
            lines, h = None, 0.55
        rows.append((y, h, lines))
        y += h + 0.20
    total = y
    fig, ax = plt.subplots(figsize=(15.2, 2.3 + 0.50 * total))
    ax.set_xlim(0, 15.2); ax.set_ylim(total + 0.3, -2.45); ax.axis("off")
    ax.text(0.15, -2.32, title, fontsize=13.5, weight="bold", color=INK, va="top")
    ax.text(0.15, -1.72, subtitle, fontsize=9.5, color=MUTED, va="top")
    dead_at = {}
    for (yy, h, _), e in zip(rows, events):
        if e[0] == "crash": dead_at[e[2]] = yy + h / 2
    for i, (name, col) in enumerate(lanes):
        ax.add_patch(FancyBboxPatch((X[i] - 1.45, -1.0), 2.9, 0.8, boxstyle="round,pad=0.03",
                                    fc=col, ec=EDGE[col], lw=1.1))
        ax.text(X[i], -0.6, name, ha="center", va="center", fontsize=9.5, color=INK, weight="bold", linespacing=1.15)
        ax.plot([X[i], X[i]], [-0.18, dead_at.get(i, total + 0.1)], color=EDGE[col], lw=1.0, ls=(0, (4, 3)), zorder=1)
    for (yy, h, lines), e in zip(rows, events):
        t = e[1]
        if t: ax.text(0.15, yy + h / 2, t, ha="left", va="center", fontsize=9, color=MUTED, family="DejaVu Sans Mono")
        if e[0] == "note":
            i = e[2]; col = lanes[i][1]
            ax.add_patch(FancyBboxPatch((X[i] - NOTE_W / 2, yy), NOTE_W, h, boxstyle="round,pad=0.02",
                                        fc=col, ec=EDGE[col], lw=0.9, zorder=3))
            ax.text(X[i], yy + h / 2, "\n".join(lines), ha="center", va="center", fontsize=8.6, color=INK,
                    linespacing=1.25, zorder=4)
        elif e[0] in ("msg", "data"):
            a, b, label = e[2], e[3], e[4]
            reply = e[5] if len(e) > 5 else None
            ya = yy + 0.38
            lw, colr = (3.2, "#444444") if e[0] == "data" else (1.2, INK)
            ax.annotate("", xy=(X[b], ya), xytext=(X[a], ya),
                        arrowprops=dict(arrowstyle="-|>", lw=lw, color=colr, shrinkA=0, shrinkB=0), zorder=3)
            ax.text((X[a] + X[b]) / 2, ya - 0.08, label, ha="center", va="bottom", fontsize=8.6, color=INK,
                    bbox=dict(fc="white", ec="none", pad=1.2), zorder=4)
            if reply:
                yb = ya + 0.44
                ax.annotate("", xy=(X[a], yb), xytext=(X[b], yb),
                            arrowprops=dict(arrowstyle="-|>", lw=1.0, color=MUTED, ls=(0, (3, 2)), shrinkA=0, shrinkB=0), zorder=3)
                ax.text((X[a] + X[b]) / 2, yb - 0.08, reply, ha="center", va="bottom", fontsize=8.6, color=MUTED,
                        bbox=dict(fc="white", ec="none", pad=1.2), zorder=4)
        elif e[0] == "crash":
            i = e[2]; yc = yy + h / 2
            ax.plot([X[i] - 0.22, X[i] + 0.22], [yc - 0.2, yc + 0.2], color=DEAD, lw=2.6, zorder=5)
            ax.plot([X[i] - 0.22, X[i] + 0.22], [yc + 0.2, yc - 0.2], color=DEAD, lw=2.6, zorder=5)
            ax.text(X[i] + 0.4, yc, e[3], ha="left", va="center", fontsize=9, color=DEAD, weight="bold")
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    print("wrote", out)


out = sys.argv[1]

draw("A. The recipient leader dies during a round (pair A, slots 910-1364)",
     "Times are seconds after the kill, from one run on 3 Oct 2026. Thick arrows carry the round's data (455 slots, 954 MB).",
     [("Donor leader\nsg1 on redis0", DONOR), ("Old recipient leader\nsg4 on redis3", RECIP),
      ("New recipient leader\nsg4 on redis5", RECIP), ("Recipient follower\nsg4 on redis4", FOLL)],
     [("note", "-0.09", 0, "Logs 'round started' in the donor group's Raft log and redirects client writes for the 455 slots to the recipient group."),
      ("msg", "-0.08", 0, 1, "open the round; ask for landing buffers"),
      ("crash", " 0.00", 1, "killed"),
      ("note", "+1.54", 2, "Wins the election. Finds in its Raft log a round that was opened but never recorded as held, so it starts recovery."),
      ("msg", "+1.55", 2, 3, "what do you hold for 910-1364?", "0 of 455 slots"),
      ("note", "+1.55", 2, "Checks its own memory: it also holds 0 of 455 slots. Nothing of this round had landed."),
      ("msg", "+1.56", 2, 0, "sg4 has a new leader for this range"),
      ("note", "+1.56", 0, "Records the new recipient leader for the range."),
      ("note", "+3.92", 0, "Its request to the dead leader gives up after 4 s. The round has failed on the old link."),
      ("msg", "+4.00", 0, 2, "what do you hold for 910-1364?", "durable 0, landed-not-committed 0, missing 455"),
      ("note", "+4.02", 0, "Restarts the round under a new id on a new RDMA link. It will send only the missing slots (here all 455)."),
      ("note", "+4.07", 2, "Logs 'round opened' again and registers landing buffers. Builds the replication chain without the dead node."),
      ("data", "+4.09", 0, 2, "RDMA copy of 455 slots (0.35 s)"),
      ("data", "+4.34", 2, 3, "replicate the round (0.33 s)"),
      ("note", "+4.70", 2, "Leader and one follower hold the round: that is a majority of the group."),
      ("note", "+5.15", 2, "Finishes merging into its keyspace, skipping keys that clients wrote in the meantime (1272 of 8479). Logs 'round held'."),
      ("msg", "+5.18", 2, 0, "round held"),
      ("note", "+5.18", 0, "Logs 'round done' and narrows its own range to 1365-5460. The next round proceeds normally."),
      ], out + "/recovery_A_recipient_leader_crash.png")

draw("B. The donor leader dies during a round (pair A, slots 0-454)",
     "Times are seconds after the kill, from one run on 3 Oct 2026. Thick arrows carry the round's data (455 slots, 954 MB).",
     [("Old donor leader\nsg1 on redis0", DONOR), ("New donor leader\nsg1 on redis2", DONOR),
      ("Recipient leader\nsg4 on redis3", RECIP), ("Recipient follower\nsg4 on redis4", FOLL)],
     [("data", "-0.4", 0, 2, "RDMA copy in progress"),
      ("note", "-0.38", 2, "Has 342 of 455 slots so far and is already forwarding them to its follower."),
      ("crash", " 0.00", 0, "killed"),
      ("note", " 0.00", 2, "The copy stops. It keeps serving the client writes that were redirected to it for these slots, and does not commit the partial round."),
      ("note", "+1.83", 1, "Wins the election. Finds 'round started' with no 'round done' in its Raft log, so it starts recovery."),
      ("note", "+1.86", 1, "Logs the round again under a new id, re-applies the write redirect, and opens a new RDMA link to the recipient leader."),
      ("msg", "+1.86", 1, 2, "what do you hold for 0-454?", "durable 0, landed-not-committed 0, missing 455"),
      ("note", "+1.86", 2, "The new session for the same slots supersedes the old one: it abandons the old forward (stalled at 342 of 455) and never commits it."),
      ("note", "+2.14", 2, "Registers a new landing pool (0.26 s) and logs 'round opened' for the new session."),
      ("data", "+2.16", 1, 2, "RDMA copy of all 455 slots (0.35 s)"),
      ("data", "+2.41", 2, 3, "replicate the round (0.33 s)"),
      ("note", "+2.79", 2, "Leader and one follower hold the round: a majority."),
      ("note", "+3.27", 2, "Finishes merging, skipping keys that clients wrote since the redirect (6276 of 8245 here, after 3 s of writes). Logs 'round held'."),
      ("msg", "+3.28", 2, 1, "round held"),
      ("note", "+3.28", 1, "Logs 'round done' and narrows its own range to 455-5460. The next round proceeds normally."),
      ], out + "/recovery_B_donor_leader_crash.png")
