#!/usr/bin/env python3
"""Migration phase Gantt with the recipient BACKPATCH broken into its sub-phases:
MERGE (pool workers) / CHAIN-FWD (RDMA replicate to followers) / COMMIT (raft).
X=time(s), Y=phase lanes, bars=donor sessions sg<donor>.<round>."""
import re, sys
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

expdir = Path(sys.argv[1]); out = Path(sys.argv[2])
DONORS = [("sg1", "redis0", "sg1"), ("sg2", "redis1", "sg2"), ("sg3", "redis2", "sg3")]
TS = re.compile(r"(\d{2}):(\d{2}):(\d{2})\.(\d+)")
def secs(line):
    m = TS.search(line); h, mi, s, ms = map(int, m.groups())
    return h*3600 + mi*60 + s + ms/1000.0

# ---- donor phases (PREP / REGISTERING / FLIPPING / TRANSFER) ----------------
def parse_donor(path):
    sessions, cur = [], None
    for ln in path.read_text(errors="ignore").splitlines():
        if "state=PREP" in ln:
            if cur: sessions.append(cur)
            cur = {"PREP": [secs(ln), None]}
        elif cur is None: continue
        elif "state=REGISTERING" in ln:
            cur["PREP"][1] = secs(ln); cur["REGISTERING"] = [secs(ln), None]
        elif "state=FLIPPING" in ln:
            t = secs(ln); prev = "REGISTERING" if "REGISTERING" in cur else "PREP"
            cur[prev][1] = t; cur["FLIPPING"] = [t, None]
        elif "state=TRANSFER" in ln:
            t = secs(ln); cur["FLIPPING"][1] = t; cur["TRANSFER"] = [t, None]
        elif "state=BACKPATCH" in ln:
            cur["TRANSFER"][1] = secs(ln)
        elif "DONE n_slots" in ln:
            cur["_done"] = secs(ln); sessions.append(cur); cur = None
    if cur: sessions.append(cur)
    return sessions

rows = []
for sg, host, suf in DONORS:
    p = expdir / "logs" / host / "tmp" / "redis_logs" / f"{host}_{suf}.log"
    if not p.exists(): continue
    for ri, s in enumerate(parse_donor(p), 1):
        rows.append([sg, ri, s])
rows.sort(key=lambda r: r[2]["PREP"][0])      # time order == recipient session order

# ---- recipient sub-phases (time-ordered, zipped to donor sessions) ---------
rlog = (expdir / "logs" / "redis3" / "tmp" / "redis_logs" / "redis3_sg4.log").read_text(errors="ignore").splitlines()
def times(pred):
    return [secs(l) for l in rlog if pred(l)]
bp_init = times(lambda l: "DONE-SLOTS-INIT: batch" in l and "total_slots" in l)
mg_done = times(lambda l: "backpatch-merge: batch DONE" in l)
ch_wrote = times(lambda l: "CHAIN: sess=" in l and " wrote " in l and " bytes" in l)
commit  = times(lambda l: "RECP_TXN_DONE logged" in l)
# sessions that took the cold ibv_reg_mr fallback (time-ordered)
cold = times(lambda l: "src pool not pre-registered" in l)
# Pipelined run? Then the forward starts during the transfer/merge (per-slot),
# so draw CHAIN from the session start (bp_init) to chain_wrote to show its TRUE
# overlapping span — not just the [merge_done -> chain_wrote] exposed tail.
PIPELINED = any("pipelined per-slot)" in l for l in rlog)

for i, r in enumerate(rows):
    if i < len(bp_init):
        r[2]["MERGE"]    = [bp_init[i], mg_done[i]]
        r[2]["CHAIN"]    = [bp_init[i] if PIPELINED else mg_done[i], ch_wrote[i]]
        r[2]["COMMIT"]   = [ch_wrote[i], commit[i]]
        r[2]["_cold"]    = any(abs(c - bp_init[i]) < (mg_done[i]-bp_init[i]+1.5) and bp_init[i] <= c <= commit[i] for c in cold)

t0 = min(s["PREP"][0] for _,_,s in rows)
flip0 = min((s["FLIPPING"][0] for sg,_,s in rows if sg=="sg1"), default=t0)
done_last = max(s["COMMIT"][1] for _,_,s in rows if "COMMIT" in s)

PHASES = ["PREP","REGISTERING","FLIPPING","TRANSFER","MERGE","CHAIN","COMMIT"]
LANE_Y = {ph: i for i, ph in enumerate(reversed(PHASES))}
SG_COLOR = {"sg1":"#1f77b4","sg2":"#ff7f0e","sg3":"#2ca7a5"}
LABEL = {"PREP":"CONNECT","REGISTERING":"REGISTER",
         "FLIPPING":"CH_OWNSHIP","TRANSFER":"TRANSFER",
         "MERGE":"BACKPATCH","CHAIN":"CHAIN-REPLICATION",
         "COMMIT":"DONE COMMIT"}

import matplotlib.patheffects as pe
from matplotlib.patches import FancyBboxPatch
plt.rcParams.update({"font.family": "serif",
                     "font.serif": ["cmr10", "CMU Serif", "Computer Modern Roman", "DejaVu Serif"],
                     "mathtext.fontset": "cm", "axes.formatter.use_mathtext": True,
                     "axes.unicode_minus": False, "axes.edgecolor": "#444",
                     "svg.fonttype": "none"})
# Two-tone academic palette: round 1 = dark navy, round 2 = light blue.
NAVY = "#1f4e79"; LIGHT = "#b9cfe7"; COLD_EC = "#c0392b"
ROUND_COLOR = {1: NAVY, 2: LIGHT}
N = len(PHASES); span = (done_last - t0)
# Wide-and-short, thin bars, tight rows (reference proportions).
FIG_W, FIG_H = 14.0, 3.3
fig, ax = plt.subplots(figsize=(FIG_W, FIG_H))
fig.patch.set_facecolor("white"); ax.set_facecolor("white")
BAR_H = 0.46
xmax = span + 0.45; xmin = -0.25

# donor / recipient divider + side labels (clean, on white)
div = (LANE_Y["TRANSFER"] + LANE_Y["MERGE"]) / 2.0
ax.axhline(div, color="#dcdcdc", lw=1.0, zorder=1)
ax.text(xmin+0.16, (LANE_Y["TRANSFER"]+LANE_Y["PREP"])/2, "DONOR", rotation=90,
        ha="center", va="center", fontsize=8.5, color="#8a8a8a")
ax.text(xmin+0.16, (LANE_Y["COMMIT"]+LANE_Y["MERGE"])/2, "RECIPIENT", rotation=90,
        ha="center", va="center", fontsize=8.5, color="#8a8a8a")

# --- migration window edges + span arrow with the migration time
for x in (flip0-t0, done_last-t0):
    ax.axvline(x, color="#bbb", ls=(0,(4,3)), lw=1.0, zorder=2)
_yarr = N - 0.34
ax.annotate("", xy=(done_last-t0, _yarr), xytext=(flip0-t0, _yarr),
            arrowprops=dict(arrowstyle="<->", color="#444", lw=1.2), zorder=6)
ax.text((flip0-t0+done_last-t0)/2, _yarr+0.05,
        f"migration time = {done_last-flip0:.2f} s", ha="center", va="bottom",
        fontsize=10, color="#222", zorder=6)

# --- bars (sharp) + labels
for sg, ri, s in rows:
    for ph in PHASES:
        if ph not in s or s[ph][1] is None: continue
        a, b = s[ph]; y = LANE_Y[ph]; w = b-a
        is_cold = (ph == "CHAIN" and s.get("_cold"))
        ec = COLD_EC if is_cold else "white"
        # min rendered width so sub-pixel phases (FLIPPING ~12ms, DONE COMMIT
        # ~4ms) stay visible; the in/over-bar label always shows the TRUE ms.
        draw_w = w if w >= 0.05 else 0.05
        ax.barh(y, draw_w, left=a-t0, height=BAR_H, color=ROUND_COLOR[ri],
                edgecolor=ec, linewidth=2.4 if is_cold else 0.8, zorder=4)
        dlabel = f"{w:.2f}s" if w >= 1 else f"{w*1000:.0f}ms"
        txtcol = "white" if ri == 1 else "#16334f"
        if w > 0.22:
            ax.text((a-t0)+w/2, y, f"{sg[-1]}.{ri}\n{dlabel}", ha="center", va="center",
                    fontsize=7, color=txtcol, zorder=6, linespacing=1.0, fontweight="bold")
        elif w > 0.05:
            ax.text((a-t0)+w/2, y+BAR_H/2+0.06, dlabel, ha="center", va="bottom",
                    fontsize=6, color="#777", zorder=6, rotation=90)

# panel label, top-left (echoes the reference's "0.1 MOp/s" style)
_wl = "workloadb" if "workloadb" in str(expdir) else "workloada"
ax.text(0.0, 1.02, _wl, transform=ax.transAxes, ha="left", va="bottom",
        fontsize=11, fontweight="bold", color="#222")

ax.set_yticks(list(LANE_Y.values()))
ax.set_yticklabels([LABEL[ph] for ph in reversed(PHASES)], fontsize=9.5)
ax.tick_params(axis="y", length=0)
ax.tick_params(axis="x", length=3, color="#bbb")
ax.set_xlabel("Time (seconds)", fontsize=10, labelpad=2)
ax.tick_params(axis="x", pad=2)
ax.set_xlim(xmin, xmax); ax.set_ylim(-0.30, N-0.30)
ax.set_xticks(range(0, int(span)+1))
ax.grid(axis="x", ls=":", color="#d2d2d2", lw=0.8, zorder=0)
ax.set_axisbelow(True)
for sp in ("top","right","left"): ax.spines[sp].set_visible(False)
ax.spines["bottom"].set_color("#bbb")

# Legend intentionally omitted.

# Fixed margins (no tight-bbox crop) so the saved image keeps the golden-ratio canvas.
fig.subplots_adjust(left=0.135, right=0.99, top=0.90, bottom=0.13)
fig.savefig(out, dpi=140, facecolor="white")
import PIL.Image as _I; _w,_h = _I.open(out).size
print(f"wrote {out}  ({_w}x{_h}, ratio={_w/_h:.3f})")
