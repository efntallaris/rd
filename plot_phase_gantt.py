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

for i, r in enumerate(rows):
    if i < len(bp_init):
        r[2]["MERGE"]    = [bp_init[i], mg_done[i]]
        r[2]["CHAIN"]    = [mg_done[i], ch_wrote[i]]
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
SG_COLOR = {"sg1": "#2f6690", "sg2": "#e08a3c", "sg3": "#2c9c8f"}  # refined palette
DONOR_PH = {"PREP","REGISTERING","FLIPPING","TRANSFER"}
N = len(PHASES); span = (done_last - t0)
PHI = (1 + 5 ** 0.5) / 2          # golden ratio ≈ 1.618
FIG_H = 8.0
fig, ax = plt.subplots(figsize=(FIG_H * PHI, FIG_H))
fig.patch.set_facecolor("white"); ax.set_facecolor("white")
BAR_H = 0.56
xmax = span + 0.45; xmin = -0.25

# --- lane backgrounds: subtle donor(cool)/recipient(warm) tint + alternating shade
for ph in PHASES:
    y = LANE_Y[ph]
    base = "#eef3f7" if ph in DONOR_PH else "#faf3ec"
    ax.axhspan(y-0.5, y+0.5, color=base, zorder=0)
# donor / recipient divider + side labels
div = (LANE_Y["TRANSFER"] + LANE_Y["MERGE"]) / 2.0
ax.axhline(div, color="#c8c8c8", lw=1.1, ls=(0,(6,4)), zorder=1)
ax.text(xmin+0.05, (LANE_Y["TRANSFER"]+LANE_Y["PREP"])/2, "DONOR", rotation=90,
        ha="right", va="center", fontsize=8.5, fontweight="bold", color="#5a7d97")
ax.text(xmin+0.05, (LANE_Y["COMMIT"]+LANE_Y["MERGE"])/2, "RECIPIENT", rotation=90,
        ha="right", va="center", fontsize=8.5, fontweight="bold", color="#b07433")

# --- migration window band + span arrow with the migration time
ax.axvspan(flip0-t0, done_last-t0, color="#3a3a3a", alpha=0.05, zorder=1)
for x in (flip0-t0, done_last-t0):
    ax.axvline(x, color="#888", ls=(0,(4,3)), lw=1.1, zorder=2)
_yarr = N - 0.36
ax.annotate("", xy=(done_last-t0, _yarr), xytext=(flip0-t0, _yarr),
            arrowprops=dict(arrowstyle="<->", color="#555", lw=1.3), zorder=6)
ax.text((flip0-t0+done_last-t0)/2, _yarr+0.04,
        f"migration time = {done_last-flip0:.2f} s", ha="center", va="bottom",
        fontsize=10, color="#333", zorder=6)

# --- bars (rounded) + labels
def rbar(x, y, w, color, ec, lw, alpha):
    ax.barh(y, w, left=x, height=BAR_H, color=color, edgecolor=ec,
            linewidth=lw, alpha=alpha, zorder=4)
STROKE = [pe.withStroke(linewidth=1.6, foreground="#1b1b1b", alpha=0.55)]
for sg, ri, s in rows:
    for ph in PHASES:
        if ph not in s or s[ph][1] is None: continue
        a, b = s[ph]; y = LANE_Y[ph]; w = b-a
        is_cold = (ph == "CHAIN" and s.get("_cold"))
        ec = "#d62728" if is_cold else "white"
        rbar(a-t0, y, w, SG_COLOR[sg], ec, 2.4 if is_cold else 0.8, 0.96 if ri==1 else 0.6)
        dlabel = f"{w:.2f}s" if w >= 1 else f"{w*1000:.0f}ms"
        if w > 0.22:
            ax.text((a-t0)+w/2, y, f"{sg[-1]}.{ri}\n{dlabel}", ha="center", va="center",
                    fontsize=7, color="white", zorder=6, linespacing=1.0,
                    fontweight="bold", path_effects=STROKE)
        elif w > 0.05:
            ax.text((a-t0)+w/2, y+BAR_H/2+0.06, dlabel, ha="center", va="bottom",
                    fontsize=6, color="#555", zorder=6, rotation=90)

ax.set_yticks(list(LANE_Y.values()))
ax.set_yticklabels([LABEL[ph] for ph in reversed(PHASES)], fontsize=9.5, fontweight="bold")
ax.tick_params(axis="y", length=0)
ax.set_xlabel("Time (seconds)", fontsize=10)
_wl = "workloadb" if "workloadb" in str(expdir) else "workloada"
ax.set_xlim(xmin, xmax); ax.set_ylim(-0.55, N-0.30)
ax.set_xticks(range(0, int(span)+1))
ax.grid(axis="x", ls=":", color="#cfcfcf", lw=0.8, zorder=0)
ax.set_axisbelow(True)
for sp in ("top","right","left"): ax.spines[sp].set_visible(False)
ax.spines["bottom"].set_color("#bbb")

leg = [Patch(facecolor=SG_COLOR[s], label=f"{s}  (redis{ {'sg1':0,'sg2':1,'sg3':2}[s] })") for s in ("sg1","sg2","sg3")]
leg += [Patch(facecolor="#666", alpha=0.96, label="round 1"),
        Patch(facecolor="#666", alpha=0.6, label="round 2"),
        Patch(facecolor="#bbb", edgecolor="#d62728", linewidth=2.2, label="cold ibv_reg_mr (chain pool)")]
ax.legend(handles=leg, ncol=6, loc="upper center", bbox_to_anchor=(0.5,-0.085),
          frameon=False, fontsize=8.5, handlelength=1.4, columnspacing=1.6)

# Fixed margins (no tight-bbox crop) so the saved image keeps the golden-ratio canvas.
fig.subplots_adjust(left=0.135, right=0.99, top=0.975, bottom=0.115)
fig.savefig(out, dpi=140, facecolor="white")
import PIL.Image as _I; _w,_h = _I.open(out).size
print(f"wrote {out}  ({_w}x{_h}, ratio={_w/_h:.3f}, phi={PHI:.3f})")
