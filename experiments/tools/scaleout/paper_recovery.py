#!/usr/bin/env python3
"""Paper figure of one crash run of the 3 -> 6 scale-out: recovery timeline over one pair's
migration, with YCSB throughput below on the same time axis.

usage: paper_recovery.py <Sx_recovery.json> <out.pdf|out.png> [width_in] [pair]
  <Sx_recovery.json>  written by recovery_breakdown.py next to its figure
  width_in            3.33 (default) = one column of a two-column paper; >= 4 = full text width
  pair                A|B|C: the pair whose rounds are drawn (default: the pair with a lost or
                      re-shipped round, else the pair of the first crashed group, else A)

Rows are built from what the run contains, so every scenario works:
  one row per group that lost its leader (no leader; for a recipient, the index update of the
  durable data, after which it serves), a Recovery row with the recovery steps that happened
  (plan = probe peers + re-home / resume plan; chain rebuild; a stalled forward to a dead
  follower and its re-forward), then the pair's rounds: copy, replication to F1, index update.
Drawn at print size: the text sizes below are the sizes on paper. Grayscale; red marks the fault."""
import sys, json, re, math
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch, FancyBboxPatch

src, out = sys.argv[1], sys.argv[2]
WIDTH = float(sys.argv[3]) if len(sys.argv) > 3 else 3.33
d = json.load(open(src))
rows, sc = d["rows"], d.get("scenario", "")
FIRST_KIND = (d.get("faults") or [{"kind": "kill"}])[0]["kind"]   # kill, or partition / freeze
GROUP_PAIR = {"sg1": "A", "sg4": "A", "sg2": "B", "sg5": "B", "sg3": "C", "sg6": "C"}
CRASH_LABEL = {"S1": "recipient leader crash", "S2": "donor leader crash", "S3": "donor follower crash",
               "S4": "recipient follower crash", "S5": "donor + recipient leader crash",
               "S6": "recipient host crash", "S7": "donor host crash",
               "S8": "donor leader crash (after registration)",
               "S9": "donor A + recipient B leader crash",
               "S10": "recipient leader crash (no majority)", "S11": "recipient leader crash (majority)",
               "S12": "recipient leader crash (data on F1)"}
CRASH_SHORT = {"S5": "two leader crashes", "S9": "two leader crashes", "S6": "host crash", "S7": "host crash",
               "S8": "donor leader crash", "S10": "no majority", "S11": "majority", "S12": "data on F1"}


def find(pat):
    """[(name, a, b)] of the phase rows matching pat, with both ends known."""
    return [(n, a, b) for n, a, b in rows if re.search(pat, n) and a is not None and b is not None]


# ---- what happened -------------------------------------------------------------------------------
kills = [t for h, t in d.get("kills", [])] or [0.0]
dead = []                                   # groups that lost their leader: (sg, no-leader start, leader at)
for n, a, b in find(r"failure detection"):
    g = n.split(":")[0]
    el = find(r"^%s: election \(" % g)
    dead.append((g, a, el[0][2] if el else b))
idx_dur = {n.split(" ")[0]: (a, b) for n, a, b in find(r"INDEX UPDATE of durable data")}
rec = []                                    # recovery steps: (start, end, long label, short label, fill)
plan = find(r"probe peers") + find(r"donor decides")
if plan:
    a, b = min(p[1] for p in plan), max(p[2] for p in plan)
    if b - a >= 0.03:                       # a donor's own resume plan takes ~1 ms: nothing to draw
        rec.append((a, b, "refetch group",
                    "refetch group", "ctrl"))
for n, a, b in find(r"rebuild replication chain"):
    if b - a >= 0.03:
        rec.append((a, b, "chain reconstruction", "chain reconstruction", "chain"))
for n, a, b in find(r"forward to the dead F1"):
    rec.append((a, b, "forward to dead follower (stalls)", "stalled forward", "chain"))
for n, a, b in find(r"re-forward the round"):
    rec.append((a, b, "re-forward to the other follower", "re-forward", "ctrl"))
served_rows = find(r"served again \(median key\)|first key served again")
served = max(b for n, a, b in served_rows) if served_rows else None   # every affected key class back

allr = d.get("rounds") or []
PAIR = sys.argv[4] if len(sys.argv) > 4 else (
    next((r["pair"] for r in allr if r["reship"] or not r["ok"]), None)
    or (GROUP_PAIR.get(dead[0][0]) if dead else None) or "A")
rounds = [r for r in allr if r["pair"] == PAIR]
num, k, lost_k = [], 0, None                # round numbers: a lost round and its re-ship share one
for r in rounds:
    if r["reship"] and r["ok"]:
        if lost_k is None:   # the lost round never started copying (donor died before): count it here
            k += 1; lost_k = k
        num.append(lost_k)
    else:
        k += 1; num.append(k)
        if not r["ok"]:
            lost_k = k
# what the refetch found: where the round the crash interrupted was afterwards (a note on the
# Recipient recovery row, so the reader sees whether the round came from a replica or the donor)
RF = (d.get("refetch") or {}).get(PAIR)
RF_NOTE = None
if RF:
    k0_ = kills[0]
    rn = next((n for r, n in zip(rounds, num) if not r["ok"]), None) or next(
        (n for r, n in zip(rounds, num) if r["copy"][0] < k0_ and (r.get("done") or 1e9) > k0_), None) or 1
    ARW = r"$\rightarrow$"
    kind = RF["kind"]
    if kind == "none":
        RF_NOTE = ("no live replica has round %d %s donor resends" % (rn, ARW)) if RF.get("leader") else \
                  ("recipient lacks round %d %s donor resends" % (rn, ARW))
    elif kind == "self":
        RF_NOTE = "new leader (was F1) has round %d %s no resend" % (rn, ARW)
    elif kind == "peer":
        RF_NOTE = "a follower has round %d %s fetched from it, no resend" % (rn, ARW)
    elif kind == "durable":
        RF_NOTE = "round %d already committed in the group %s no resend" % (rn, ARW)
    elif kind == "partial":
        RF_NOTE = "recipient has %d/%d slots of round %d %s donor resends the rest" % (
            RF.get("durable", 0), RF.get("durable", 0) + RF.get("missing", 0), rn, ARW)
RF_SHORT = None
if RF_NOTE:
    RF_SHORT = {"none": ("no replica has round %d" if RF.get("leader") else "recipient lacks round %d") % rn
                        + "\n" + ARW + " donor resends",
                "self": "new leader has round %d\n%s no resend" % (rn, ARW),
                "peer": "a follower has round %d\n%s fetched from it" % (rn, ARW),
                "durable": "round %d committed\n%s no resend" % (rn, ARW),
                "partial": "recipient has part of round %d\n%s donor resends the rest" % (rn, ARW)}.get(RF["kind"])
yc = d.get("ycsb_kops") or []
pair_end = max([r["done"] for r in rounds if r.get("done") is not None] + [kills[-1] + 1.0])
pair_start = min([r["copy"][0] for r in rounds] + [kills[0]])

# ---- style ---------------------------------------------------------------------------------------
INK, MUTED, AXIS = "#111111", "#555555", "#8a8a8a"
RED = "#c0392b"    # the fault: the crash, the time without a leader, the lost round (the only color)
G = {"ctrl": "#d4d4d4", "chain": "#f2f2f2", "copy": "#3d3d3d", "repl": "#7f7f7f", "idx": "#bdbdbd",
     "fill": "#e2e2e2", "band": "#f7f7f5", "vgrid": "#e6e6e6"}
# Round shades (grayscale; red stays reserved for the fault): (fill, text color) for rounds
# 1, 2, 3, 4 -- dark to light, the same shade for a round's copy, replication and index update.
ROUND_COL = [("#2e2e2e", "white"), ("#646464", "white"), ("#a3a3a3", INK), ("#d6d6d6", INK)]
COMPACT = WIDTH < 4.0
FS = 6.5 if COMPACT else 6     # annotations (pt)
FT = 7.5 if COMPACT else 7     # row and axis labels (pt); tick numbers 0.5 pt smaller
plt.rcParams.update({"font.family": ["Times New Roman", "Times", "STIXGeneral"], "mathtext.fontset": "stix",
                     "font.size": FT, "axes.edgecolor": AXIS, "axes.linewidth": 0.45,
                     "xtick.direction": "out", "ytick.direction": "out", "xtick.color": AXIS,
                     "ytick.color": AXIS, "xtick.labelcolor": INK, "ytick.labelcolor": INK,
                     "xtick.labelsize": FT - 0.5, "ytick.labelsize": FT, "hatch.linewidth": 0.5,
                     "pdf.fonttype": 42, "ps.fonttype": 42})

import os as _os, glob as _glob, matplotlib.font_manager as _fm
if _os.environ.get("CMU_DIR"):     # Computer Modern (CMU Serif) for every label, LaTeX-style math
    for _f in _glob.glob(_os.environ["CMU_DIR"] + "/*.ttf"): _fm.fontManager.addfont(_f)
    plt.rcParams.update({"font.family": ["CMU Serif"], "mathtext.fontset": "cm"})
# ---- rows, top to bottom: (key, label, height in row units) ----------------------------------------
lanes = []                                  # recovery steps in non-overlapping lanes
for it in sorted(rec):
    for ln in lanes:
        if ln[-1][1] + 0.02 <= it[0]:
            ln.append(it); break
    else:
        lanes.append([it])
# The same rows, in the same order and height, in every figure (empty when the scenario has
# nothing for them), so figures of different scenarios line up side by side:
#   Recipient / Donor: groups without a leader (red), the new recipient leader's index update of
#     its durable data (tick), followers down (red: kill -> restart, or to the end), frozen, or
#     refilling after a restart (grey: restart -> back in sync with the leader), a partition
#   Recovery: plan, chain rebuild, stalled forward / re-forward (up to 2 lanes)
#   Copy / Replication / Index update: the pair's rounds
REC_H = 1.3
layout = [("donor", "Donor", 1.0), ("recip", "Recipient", 1.0), ("rec", "Recipient\nrecovery" if COMPACT else "Recipient recovery", REC_H),
          ("copy", "Recipient (L)\ntransfer" if COMPACT else "Recipient (L) transfer", 1.0),
          ("idx", "Index\nupdate" if COMPACT else "Index update", 1.0),
          ("repl", "Chain\nreplication" if COMPACT else "Chain replication", 1.0),
          ("async", "Async chain\nreplication" if COMPACT else "Async chain replication", 1.0)]
RECIP_HOSTS, DONOR_HOSTS = ("redis3", "redis4", "redis5"), ("redis0", "redis1", "redis2")
side_of = lambda h: "recip" if h in RECIP_HOSTS else "donor"
faults = d.get("faults", [])
side_items = {"recip": [], "donor": []}   # (start, end, kind, label)
for g, a, b in dead:
    side_items["recip" if g in ("sg4", "sg5", "sg6") else "donor"].append(
        (a, b, "noleader", ("" if g in ("sg4", "sg1") else g + " ") + "no leader %.2f s" % (b - a)))
for f in faults:
    if f["kind"] == "partition" and f.get("secs"):
        for sd in ("recip", "donor"):
            side_items[sd].append((f["t"], f["t"] + f["secs"], "fault", "partition %.1f s" % f["secs"]))
    elif f["kind"] == "freeze" and f.get("secs"):
        side_items[side_of(f["host"])].append((f["t"], f["t"] + f["secs"], "fault",
                                               "%s frozen %.1f s" % (f["host"], f["secs"])))
    elif f["kind"] == "kill":
        # a leader's kill is its group's no-leader box; a follower's is drawn down until its restart
        if any(abs(a - f["t"]) < 0.05 for g, a, b in dead):
            continue
        # the CURRENT leader killed again (C11: the leader elected after the first kill): another
        # no-leader interval, until the group's next election
        lead = None
        for g in sorted({x[0] for x in d.get("leaders", [])}):
            if f.get("sg") and g != f["sg"]:
                continue
            prev = [x for x in d["leaders"] if x[0] == g and x[2] <= f["t"]]
            if prev and max(prev, key=lambda x: x[2])[1] == f["host"]:
                lead = g
        if lead:
            nxt = [x[2] for x in d["leaders"] if x[0] == lead and x[2] > f["t"]]
            if nxt:
                b_ = min(nxt)
                side_items["recip" if lead in ("sg4", "sg5", "sg6") else "donor"].append(
                    (f["t"], b_, "noleader", "no leader %.2f s" % (b_ - f["t"])))
                continue
        rs = [x["t"] for x in faults if x["kind"] == "restart" and x["host"] == f["host"] and x["t"] > f["t"]]
        end = min(rs) if rs else None
        side_items[side_of(f["host"])].append((f["t"], end, "down", "%s down" % f["host"]))
for f in faults:
    if f["kind"] == "restart":
        ss = [x["t"] for x in faults if x["kind"] == "insync" and x["host"] == f["host"] and x["t"] > f["t"]]
        if ss:
            side_items[side_of(f["host"])].append((f["t"], min(ss), "refill",
                                                   "%s refill %.0f s" % (f["host"], min(ss) - f["t"])))
Y, edges, top = {}, [], 0.0
for key, lab, h in layout:                  # y grows downward in this loop, flipped below
    Y[key] = top + h / 2; edges.append((top, top + h)); top += h
TOTAL = top
Y = {k: TOTAL - v for k, v in Y.items()}
edges = [(TOTAL - b, TOTAL - a) for a, b in edges]
LEG_IN = 0.30 if COMPACT else 0.22   # legend band above the top panel (in)
HEIGHT = (0.95 if COMPACT else 0.9) + 0.33 * TOTAL + (0.25 if COMPACT else 0.2) + LEG_IN
H = 0.56

fig, (ax, ay) = plt.subplots(2, 1, figsize=(WIDTH, HEIGHT), sharex=True,
                             gridspec_kw={"height_ratios": [0.33 * TOTAL + 0.15, 0.8 if COMPACT else 0.75]})
x0 = min(-0.8 if COMPACT else -1.0, pair_start - 0.1)
x1 = max(pair_end + (0.25 if COMPACT else 0.7), max(kills) + 4.0,   # >= 4 s after the crash: comparable figures
         max([b + 0.5 for it in side_items.values() for a, b, k, lab in it if b is not None] or [0]))


L_IN, R_IN = (1.02 if COMPACT else 1.45), 0.05                 # left/right margins (in), see subplots_adjust
IN_PER_S = (WIDTH - L_IN - R_IN) / (x1 - x0)                    # inches per second on the time axis
# margins now (not at the end): the bars below are sized in inches from the axes geometry
fig.subplots_adjust(left=L_IN / WIDTH, right=1 - R_IN / WIDTH, top=1 - (0.03 + LEG_IN) / HEIGHT,
                    bottom=(0.36 if COMPACT else 0.31) / HEIGHT, hspace=0.12)
ax.set_xlim(x0, x1); ax.set_ylim(0, TOTAL)
_pos = ax.get_position()
ASPECT = (TOTAL / (_pos.height * HEIGHT)) * IN_PER_S   # row units per inch / seconds per inch
GAP_X = 0.012 / IN_PER_S                                # a thin white gap between touching boxes
RND_X = 0.016 / IN_PER_S                                # corner rounding (same in every figure)


def text_w(txt, fs):
    """Approximate printed width (in) of the longest line of txt at fs pt (Times: ~0.40 em/char)."""
    return max(len(t) for t in txt.split("\n")) * fs * 0.40 / 72


def fits(txt, fs, a, b, pad=0.04, h=None):
    """txt fits inside a box from a to b (s), with pad (in) to spare; and in height h (row units)."""
    if text_w(txt, fs) + pad > (b - a) * IN_PER_S:
        return False
    return h is None or (txt.count("\n") + 1) * fs * 1.05 / 72 <= h * IN_PER_S / ASPECT + 0.01


def short_of(lab):
    """The short form of a label: its duration ("... 0.59 s" -> "0.59 s"), else its last word."""
    m = re.search(r"([\d.]+ s)$", lab)
    return m.group(1) if m else lab.split()[-1]


def bar(y, a, b, fill, txt="", txtcol=INK, h=H, hatch=None, edge=None, lw=0.0, z=2, fs=None, ls=0.85):
    """A box from a to b (s) on row y: slightly rounded corners, and a hair of white on each side
    so boxes that touch stay apart."""
    g = GAP_X if (b - a) > 3 * GAP_X else 0.0
    w = max(b - a - g, 1e-6)
    r = min(RND_X, w / 2, (h / 2) / ASPECT)
    ax.add_patch(FancyBboxPatch((a + g / 2, y - h / 2), w, h,
                                boxstyle="round,pad=0,rounding_size=%.6g" % r, mutation_aspect=ASPECT,
                                facecolor=fill, hatch=hatch, zorder=z,
                                edgecolor=edge if edge else (MUTED if hatch else fill),
                                linewidth=lw if (edge or hatch) else 0))
    if txt:
        ax.text((a + b) / 2, y, txt, ha="center", va="center", fontsize=fs or FS, color=txtcol, zorder=z + 1,
                linespacing=ls)


# grid: light alternating row bands, vertical lines at every second through both panels
for i, (lo, hi) in enumerate(edges):
    if i % 2 == 0:
        ax.axhspan(lo, hi, color=G["band"], lw=0, zorder=0)

# the new recipient leader's index update of its durable data, then serves: a tick, and a note that
# shortens itself to the free space before the next item of the row
idx_notes = []   # (tick x, note, note end x)
for g, a, b in dead:
    if g in idx_dur:
        ia, ib = idx_dur[g]
        ms = max(1, (ib - ia) * 1e3)
        nxt = min([it[0] for it in side_items["recip"] if it[0] > ia + 0.01] + [x1])
        note = next((t for t in (("index update (%.0f ms), then serve" if COMPACT else
                                  "index update of durable data (%.0f ms), then serve clients") % ms,
                                 "index update %.0f ms" % ms, "%.0f ms" % ms)
                     if text_w(t, FS) / IN_PER_S + 0.12 <= nxt - ia), "")
        idx_notes.append((ia, note, ia + 0.08 + text_w(note, FS) / IN_PER_S if note else ia))
# The index update of the durable data takes ~1 ms right at the end of the time without a leader:
# not drawn (the faulty phase ends where the new leader serves).
idx_notes = []
# Recipient / Donor rows: items in up to two lanes when they overlap in time
for sd, items in side_items.items():
    items = sorted((a, (b if b is not None else x1), k, lab) for a, b, k, lab in items)
    rows_ = []
    for it in items:
        for ln in rows_:
            if ln[-1][1] + 0.02 <= it[0]:
                ln.append(it); break
        else:
            rows_.append([it])
    nl = max(1, len(rows_))
    lh = 1.0 / nl
    for li, ln in enumerate(rows_):
        y = Y[sd] + 0.5 - lh * (li + 0.5)
        h = min(H, lh * 0.82)
        for a, b, kind, lab in ln:
            if kind == "refill":
                bar(y, a, b, G["fill"], h=h, edge=MUTED, lw=0.4)
                col = INK
            else:   # no leader, down, frozen, partition: the fault, red
                bar(y, a, b, "white", hatch="/////", edge=RED, lw=0.6, h=h)
                col = INK   # label text black (red text on the red hatching was hard to read)
            # the label inside the box if it fits there (full, on two lines, or short), else the
            # short form next to the box where the lane is free -- never across the box's edge
            f0 = FS if nl == 1 else FS - 0.8
            sh = short_of(lab)
            two = lab.rsplit(" ", 2)[0] + "\n" + " ".join(lab.rsplit(" ", 2)[1:]) if lab.count(" ") >= 2 else None
            cands = [(lab, f0)] + ([(two, f0)] if two else []) + [(sh, f0), (sh, f0 - 1.0)]
            pick = next(((t, f) for t, f in cands if fits(t, f, a, b, pad=0.06, h=h)), None)
            if pick:
                ax.text((a + b) / 2, y, pick[0], fontsize=pick[1], ha="center", va="center", color=col,
                        zorder=4, linespacing=0.85,
                        bbox=dict(boxstyle="square,pad=0.05", fc="white", ec="none") if kind != "refill" else None)
            else:
                wsh = text_w(sh, f0 - 1.0) / IN_PER_S + 0.08
                nxt = min([c for c, _, _, _ in ln if c > a] + [x1] +
                          ([n0 for n0, _, _ in idx_notes if n0 >= b - 0.01] if sd == "recip" else []))
                prv = max([d_ for _, d_, _, _ in ln if d_ < a] + [x0])
                if nxt - b > wsh:
                    ax.text(b + 0.04, y, sh, fontsize=f0 - 1.0, ha="left", va="center", color=col, zorder=4)
                elif a - prv > wsh:
                    ax.text(a - 0.04, y, sh, fontsize=f0 - 1.0, ha="right", va="center", color=col, zorder=4)
            if kind == "refill":
                ax.plot([b] * 2, [y - h / 2, y + h / 2], color=INK, lw=1.2, zorder=4)   # in sync
for ia, note, _ in idx_notes:
    y = Y["recip"]
    ax.plot([ia] * 2, [y - H / 2, y + H / 2], color=INK, lw=1.6, zorder=4)
    if note:
        ax.text(ia + 0.08, y, note, fontsize=FS, color=INK, va="center")
# recovery steps, one lane each, labels inside and durations to the right
RF_DONE = False
# re-pack the recovery steps now that label widths are known: a step's label sits to the right of
# its bar, so the next step on the same line must start after that label, not just after the bar
def _step_end(it):
    a, b, long_lab, short_lab, fill = it
    lab = short_lab if COMPACT else long_lab
    if fits(lab, FS - 0.3, a, b):
        return b + 0.06 + text_w("0.00 s", FS - 0.3) / IN_PER_S
    return b + 0.06 + text_w(lab + " 0.00 s", FS - 0.3) / IN_PER_S
lanes = []
for it in sorted(rec):
    for ln in lanes:
        if _step_end(ln[-1]) + 0.04 <= it[0]:
            ln.append(it); break
    else:
        lanes.append([it])
if lanes:
    n_l, rh = len(lanes), REC_H
    lane_h = rh / max(2, n_l)
    for li, ln in enumerate(lanes):
        y = Y["rec"] + rh / 2 - lane_h * (li + 0.5)
        for a, b, long_lab, short_lab, fill in ln:
            lab = short_lab if COMPACT else long_lab
            inside = fits(lab, FS - 0.3, a, b)
            bar(y, a, b, G[fill], lab if inside else "", h=min(0.5, lane_h * 0.78), edge=MUTED, lw=0.4,
                fs=FS - 0.3)
            txt = ("%.2f s" if inside else lab + " %.2f s") % (b - a)
            if RF_NOTE and long_lab == "refetch group" and not COMPACT:
                txt += ": " + RF_NOTE
                RF_DONE = True
            ax.text(b + 0.06, y, txt, fontsize=FS - 0.3, color=INK, va="center")
if RF_SHORT and COMPACT:      # column figure: no room beside the refetch bar -- two lines on the
    # Recipient row, right of the no-leader interval
    # right of the recipient group's no-leader bar (and of its label when it sits outside the bar)
    nx = RF["t"] + 0.08
    for g, a, b in dead:
        if g in ("sg4", "sg5", "sg6") and GROUP_PAIR.get(g) == PAIR:
            dl = "%.2f s" % (b - a)
            e = b + text_w(dl, FS) / IN_PER_S + 0.16      # past the bar and a label beside it
            nx = max(nx, e)
    ax.text(nx, Y["recip"], RF_SHORT, fontsize=FS - 1.0, color=INK, va="center", linespacing=1.1)
elif RF_NOTE and not RF_DONE:   # no refetch bar (the donor asked the recipient, ~1 ms): the note alone
    li = len(lanes) if len(lanes) < 2 else 0
    lane_h = REC_H / 2
    y = Y["rec"] + REC_H / 2 - lane_h * (li + 0.5)
    ax.text(RF["t"], y, "refetch: " + RF_NOTE, fontsize=FS - 0.3, color=INK, va="center")
# the pair's rounds: copy, replication to F1, index update
for r, n in zip(rounds, num):
    ca, cb = r["copy"]
    if cb is None:           # its donor died mid-copy: the copy ends at that kill
        cb = next((kt for kt in sorted(kills) if kt > ca), ca + 0.05)
    if not r["ok"]:          # a round its crash cut short: its own round color up to the crash,
        # and a red x where each of its bars meets the crash line (transfer, index update, chain
        # replication alike)
        kt = next((t for t in sorted(kills) if t >= ca - 1e-6), cb)
        rc, rt = ROUND_COL[(n - 1) % len(ROUND_COL)]
        # drawn at its true length (a round cut right after it started is a sliver); its number
        # only when it fits
        ce = min(cb, kt)
        # the donor sending it, the recipient leader receiving it (a round cut in its setup moved
        # no byte yet: the donor row only)
        rws = ("donor",) if r.get("setup") else ("donor", "copy")
        for row in rws:
            bar(Y[row], ca, ce, rc, str(n) if fits(str(n), FS - 1.0, ca, ce, pad=0.01) else "",
                txtcol=rt, fs=FS - 1.0)
        # an x only where a bar meets the crash line: a copy that had finished gets none
        cut = [Y[w] for w in rws] if ce >= kt - 0.02 else []
        held = r.get("held")
        for key in ("idx", "repl"):
            a0 = r[key][0]
            if a0 is None and held:   # landed, waiting to be forwarded to F1 when the leader died
                a0 = held[0]
                lab = "held" if fits("held", FS - 1.0, a0, kt, pad=0.16) else ""
                bar(Y[key], a0, kt, rc, lab, txtcol=rt, fs=FS - 1.0)
                cut.append(Y[key])
            elif a0 is not None and a0 < kt:
                bar(Y[key], a0, kt, rc)
                cut.append(Y[key])
        for yy in cut:
            ax.plot([kt], [yy], marker="x", color=RED, ms=6.0 if not COMPACT else 5.2, mew=2.0, zorder=6, clip_on=False)
        continue
    # each bar carries its duration (copy: round number above it), when it fits the bar
    # the duration at one size, or one step smaller, before it is left out
    DFS = FS - 1.0
    lab = "%d\n%.2f s" % (n, cb - ca)
    if not fits(lab, DFS, ca, cb, pad=0.015, h=H):
        DFS = FS - 1.8
        if not fits(lab, DFS, ca, cb, pad=0.015, h=H):
            lab, DFS = str(n), FS - 1.0
    # one color per round, the same for its copy, replication and index update
    rc, rt = ROUND_COL[(n - 1) % len(ROUND_COL)]
    # the transfer of the round: the donor sending it (Donor row) and the recipient leader
    # receiving it (Recipient (L) transfer row) -- the same RDMA writes, seen from each side
    for row in ("donor", "copy"):
        if r["reship"]:
            # the retransfer: the color of the round it sends again (the one the crash cut), no outline
            bar(Y[row], ca, cb, rc, lab, txtcol=rt, z=3, fs=DFS, ls=1.35)
        else:
            bar(Y[row], ca, cb, rc, lab, txtcol=rt, fs=DFS, ls=1.35)
    ad = r.get("adopted")
    for key, (a, b), col, tc in (("repl", r["repl"], rc, rt), ("idx", r["idx"], rc, rt)):
        if a is not None and b is None and ad:
            # the old leader died before finishing this step for the round (S12: its index update):
            # cut at the crash, like a lost round
            kt = next((t for t in sorted(kills) if t >= a - 1e-6), None)
            if kt is not None and kt > a:
                bar(Y[key], a, kt, col)
                ax.plot([kt], [Y[key]], marker="x", color=RED, ms=6.0 if not COMPACT else 5.2, mew=2.0,
                        zorder=6, clip_on=False)
            continue
        if a is None or b is None:
            continue
        d = "%.2f s" % (b - a)
        f_ = next((f for f in (FS - 1.0, FS - 1.8) if fits(d, f, a, b, pad=0.015)), None)
        bar(Y[key], a, b, col, d if f_ else "", txtcol=tc, fs=f_ or DFS)
    # the rest of the chain after the majority hop (F1 -> F2), when the round had one; if F1 held
    # the round for a while before sending it, that wait as a thin line with its length
    aa, ab = (r.get("async") or [None, None])
    # F1 holding the round for F2 when the leader died (S12: the test hook held F1's forward
    # across the crash): the hold up to the crash, cut there, like S10's held forward
    aw = r.get("async_wait")
    if aw and aw[0] is not None:
        kw = next((t for t in sorted(kills) if aw[0] < t < aw[1]), None)
        if kw is not None:
            bar(Y["async"], aw[0], kw, rc, "held" if fits("held", FS - 1.0, aw[0], kw, pad=0.16) else "",
                txtcol=rt, fs=FS - 1.0)
            ax.plot([kw], [Y["async"]], marker="x", color=RED, ms=6.0 if not COMPACT else 5.2, mew=2.0,
                    zorder=6, clip_on=False)
    if aa is not None and ab is not None and ab > aa:
        d = "%.2f s" % (ab - aa)
        # (a bar running past the right edge: label only what is visible)
        f_ = next((f for f in (FS - 1.0, FS - 1.8) if fits(d, f, aa, min(ab, x1), pad=0.015)), None)
        bar(Y["async"], aa, ab, rc, d if f_ and ab <= x1 else "", txtcol=rt, fs=f_ or DFS)
    if ad:
        # finished from a follower's copy after the crash: F1's forward to the new leader (chain
        # replication) and the new leader's index update -- no transfer from the donor
        # labelled with who did it: before the crash the round went leader -> F1 and the old leader
        # indexed it; these are the rest of the chain for the same round (F1 -> F2, and F1 and F2
        # indexing it), which would otherwise read as the same step running twice
        for key, who in (("repl", "F1\u2192F2"), ("idx", "F1, F2")):
            if key not in ad:
                continue
            a, b = ad[key]
            if a is None or b is None or b <= a:
                continue
            lab2 = "%s\n%.2f s" % (who, b - a)
            f_ = next((f for f in (FS - 1.0, FS - 1.8) if fits(lab2, f, a, b, pad=0.015, h=H)), None)
            if f_ is None:
                lab2 = who
                f_ = next((f for f in (FS - 1.0, FS - 1.8) if fits(lab2, f, a, b, pad=0.01)), None)
            bar(Y[key], a, b, rc, lab2 if f_ else "", txtcol=rt, fs=f_ or DFS, ls=1.35)
for kt in kills:
    for a_ in (ax, ay):
        # behind the boxes and their labels in the top panel; over the throughput frame below
        a_.axvline(kt, color=RED, ls=(0, (3, 2)), lw=0.8, zorder=1.5 if a_ is ax else 2.5)
ax.set_yticks([Y[k] for k, _, _ in layout])
ax.set_yticklabels([lab for _, lab, _ in layout], linespacing=0.85, fontsize=FT)
ax.set_ylim(0, TOTAL)   # no labels above the rows
ax.tick_params(axis="y", length=0, pad=2)
# the time ticks of the bottom panel, without numbers
ax.tick_params(axis="x", length=2.5, width=0.45, color="#000000", labelbottom=False)
ax.set_ylabel("Phase", fontsize=FT, color=INK, labelpad=2)
# legend: what the marks mean, one band above the top panel
leg = [Patch(facecolor="white", edgecolor=RED, hatch="/////", lw=0.6, label="faulty phase"),
       Line2D([], [], color=RED, ls=(0, (3, 2)), lw=0.8, label="crash"),
       Line2D([], [], color=RED, ls="none", marker="x", ms=5.5, mew=1.8, label="cut by the crash"),
       Patch(facecolor=G["ctrl"], edgecolor=MUTED, lw=0.4, label="recovery step" if not COMPACT else "recovery"),
       Patch(facecolor=G["fill"], edgecolor=MUTED, lw=0.4, label="replica refill" if not COMPACT else "refill"),
       Line2D([], [], color=INK, marker="|", ls="none", ms=6, mew=1.4, label="replica in sync")]
fig.legend(handles=leg, loc="upper center", ncol=4 if COMPACT else 7, fontsize=FS - 0.5, frameon=False,
           bbox_to_anchor=(0.5, 1.0), handlelength=1.6, handleheight=0.8,
           columnspacing=0.8, handletextpad=0.4, borderaxespad=0.1, labelspacing=0.25)
FRAME = dict(color="#000000", lw=0.6)    # borders: each panel in a thin black box, flush with the data
for s_ in ax.spines.values():
    s_.set_edgecolor(FRAME["color"]); s_.set_linewidth(FRAME["lw"])

# throughput: black line (no fill); the client outage as a dimension line
pts = [(t, v) for t, v in yc if x0 - 1 <= t <= x1 + 1]
tx, vy = [t for t, v in pts], [v for t, v in pts]
ay.plot(tx, vy, color=INK, lw=0.9, marker="o", ms=2.6, mfc="white", mec=INK, mew=0.7, zorder=3)
if served is not None:
    ay.annotate("", xy=(kills[0], 385), xytext=(served, 385), zorder=4,
                arrowprops=dict(arrowstyle="<->", lw=0.6, color=INK, shrinkA=0, shrinkB=0))
    ay.plot([served, served], [0, 400], color=INK, lw=0.5, zorder=2)
    # the outage as its duration only, centred under the arrow (2026-11-06)
    ay.text((kills[0] + served) / 2, 368, "%.2f s" % (served - kills[0]), fontsize=FS, color=INK,
            va="top", ha="center")
ay.set_xlim(x0, x1); ay.set_ylim(0, 440); ay.set_yticks([0, 150, 300])
ay.set_ylabel("YCSB\nKops/s", fontsize=FT, color=INK, linespacing=0.9, labelpad=2)
ay.set_xlabel("Time after the %s (s)" % ("crash" if FIRST_KIND == "kill" else "fault"), fontsize=FT, color=INK,
              labelpad=1)
ay.tick_params(axis="both", length=2.5, width=0.45, pad=1.5, labelsize=FT - 0.5)
for s_ in ay.spines.values():
    s_.set_edgecolor(FRAME["color"]); s_.set_linewidth(FRAME["lw"])
ay.tick_params(axis="both", color=FRAME["color"])
# tight_layout does not handle the shared-x grid: explicit margins (inches)
fig.subplots_adjust(left=L_IN / WIDTH, right=1 - R_IN / WIDTH, top=1 - (0.03 + LEG_IN) / HEIGHT,
                    bottom=(0.36 if COMPACT else 0.31) / HEIGHT, hspace=0.12)   # the two frames divide the panels
# Both axis titles at the same x, inside the left margin (the longest row label, "Recipient (L)
# transfer", used to push them past the figure edge).
_axw = WIDTH - L_IN - R_IN
for a_ in (ax, ay):
    a_.yaxis.set_label_coords(-(L_IN - 0.24) / _axw, 0.5)   # 0.24 in: room for a 2-line title
fig.savefig(out, dpi=400)
if out.endswith(".pdf"):
    fig.savefig(out[:-4] + ".png", dpi=400)
print("wrote %s (%s, pair %s, %.2f x %.2f in)" % (out, sc, PAIR, WIDTH, HEIGHT))
