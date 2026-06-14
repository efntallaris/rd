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
# First migrated data actually LANDS when the donor's first RDMA chunk completes
# (DONE-SLOTS-CHUNK seq=0) — NOT at DONE-SLOTS-INIT, which only arms the pipeline
# before any bytes exist. BACKPATCH cannot begin merging before this point.
first_chunk = times(lambda l: "DONE-SLOTS-CHUNK" in l and " seq=0 " in l)
mg_done = times(lambda l: "backpatch-merge: batch DONE" in l)
ch_wrote = times(lambda l: "CHAIN: sess=" in l and " wrote " in l and " bytes" in l)
commit  = times(lambda l: "RECP_TXN_DONE logged" in l)
# The REAL chain-replication start: when the forwarder posts its first RDMA WRITE
# to F1 (after F1's pool is ready + a snapshot is captured). This is the accurate
# anchor — NOT "chain established", which logs only when the establish THREAD
# finishes (after the slow downstream follower's CHAIN-PREP) and is unrelated to
# when the leader->F1 forward actually begins. The first-post times already
# reflect the single-wire serialization (a session waits for g_chain_forward_mu).
ch_firstpost = times(lambda l: "forward FIRST-POST" in l)
# Per-round chain wire-up time (fallback only, for logs without FIRST-POST).
estab = times(lambda l: "chain established: sess=" in l and "sess=9000" not in l)
# sessions that took the cold ibv_reg_mr fallback (time-ordered)
cold = times(lambda l: "src pool not pre-registered" in l)
# Pipelined run? Then the forward overlaps transfer/merge (per-slot) instead of
# running as a serial tail after merge_done.
PIPELINED = any("pipelined per-slot)" in l for l in rlog)

prev_chain_end = 0.0   # leader->F1 wire is one QP, serialized across sessions
for i, r in enumerate(rows):
    if i < len(bp_init):
        ri = r[1]
        # BACKPATCH: first chunk landed -> merge done (the real data-movement span).
        m_start = first_chunk[i] if i < len(first_chunk) else bp_init[i]
        r[2]["MERGE"]    = [m_start, mg_done[i]]
        # CHAIN start = the REAL forward first-post (accurate; already reflects the
        # single-wire serialization). Fall back to the old estab/merge anchors only
        # for logs that predate the FIRST-POST marker.
        if i < len(ch_firstpost):
            c_start = ch_firstpost[i]
        elif PIPELINED:
            wire = estab[ri-1] if 0 <= ri-1 < len(estab) else m_start
            c_start = max(max(wire, m_start), prev_chain_end)
        else:
            c_start = max(mg_done[i], prev_chain_end)
        r[2]["CHAIN"]    = [c_start, ch_wrote[i]]
        prev_chain_end   = max(prev_chain_end, ch_wrote[i])
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

# --- per-chunk start times, PER PHASE: transfer (DONE-SLOTS-CHUNK landing),
# backpatch (PERCHUNK BACKPATCH), chain (PERCHUNK CHAIN). Keyed (sg, round, seq).
PORT_SG = {"08000": "sg1", "08001": "sg2", "08002": "sg3"}
_tr_re = re.compile(r"DONE-SLOTS-CHUNK: from AQRAFT_(\d+)_\S* mig_id=(\d+) seq=(\d+)")
_bp_re = re.compile(r"PERCHUNK BACKPATCH sess=(\d+) seq=(\d+)")
_ch_re = re.compile(r"PERCHUNK CHAIN sess=(\d+) slot0=(-?\d+) seq=(\d+)")
def _slot_sg(s0): return "sg1" if s0 < 5461 else ("sg2" if s0 < 10922 else "sg3")
tr_ck = {}   # (sg, round, seq) -> transfer landing time
ch_ck = {}   # (sg, round, seq) -> chain start time
bp_raw = []  # (round, seq, time) — donor inferred by nearest transfer landing
for l in rlog:
    m = _tr_re.search(l)
    if m:
        sg = PORT_SG.get(m.group(1))
        if sg: tr_ck[(sg, int(m.group(2)), int(m.group(3)))] = secs(l)
        continue
    m = _ch_re.search(l)
    if m:
        ch_ck[(_slot_sg(int(m.group(2))), int(m.group(1)), int(m.group(3)))] = secs(l)
        continue
    m = _bp_re.search(l)
    if m: bp_raw.append((int(m.group(1)), int(m.group(2)), secs(l)))
# backpatch log lacks the donor → match each to the sg with the nearest transfer landing
bp_ck = {}
for (rd, seq, t) in bp_raw:
    best, bd = None, 1e9
    for sg in ("sg1", "sg2", "sg3"):
        tt = tr_ck.get((sg, rd, seq))
        if tt is not None and abs(t - tt) < bd: bd, best = abs(t - tt), sg
    if best: bp_ck[(best, rd, seq)] = t
PHASE_CK = {"TRANSFER": tr_ck, "MERGE": bp_ck, "CHAIN": ch_ck}

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
            ax.text((a-t0)+w/2, y+BAR_H/2+0.06, f"{sg[-1]}.{ri} {dlabel}", ha="center", va="bottom",
                    fontsize=6, color="#777", zorder=6, rotation=90)

# --- per-chunk start ticks, drawn in EACH phase's OWN lane at that phase's
# per-chunk start time (transfer landing / backpatch start / chain start). They
# line up vertically across the three lanes because the per-chunk data shows the
# three coincide within ~1 ms — each chunk is backpatched + forwarded the instant
# it lands.
_nck = 0
for sg, ri, s in rows:
    for ph in ("TRANSFER", "MERGE", "CHAIN"):
        if ph not in s or s[ph][1] is None: continue
        y = LANE_Y[ph]; ckmap = PHASE_CK[ph]; seq = 0
        while (sg, ri, seq) in ckmap:
            t = ckmap[(sg, ri, seq)]
            # grey: on TRANSFER lane this is the chunk LANDING (transfer complete);
            # on MERGE/CHAIN it is the chunk's processing START — they coincide,
            # which is the point. Transfer START is drawn separately in green below.
            ax.plot([t-t0, t-t0], [y-BAR_H/2, y+BAR_H/2], ls=(0, (1, 1.2)),
                    color="#6a6a6a", lw=0.7, alpha=0.95, zorder=6)
            _nck += 1; seq += 1

# --- transfer START per chunk (green): the donor streams continuously, so chunk
# N starts the instant chunk N-1 lands; chunk 0 starts at the session transfer
# begin (TRANSFER bar's left edge). The gap from a green tick to the next grey
# tick on the TRANSFER lane is that chunk's ~127 ms wire time.
y = LANE_Y["TRANSFER"]; _nst = 0; _GRN = "#1e8449"
for sg, ri, s in rows:
    if "TRANSFER" not in s or s["TRANSFER"][1] is None: continue
    seq = 0
    while (sg, ri, seq) in tr_ck:
        start = s["TRANSFER"][0] if seq == 0 else tr_ck[(sg, ri, seq-1)]
        x = start - t0
        # solid green line, taller than the bar, with a down-triangle cap on top —
        # makes each chunk's transfer START pop out from the grey landing ticks.
        ax.plot([x, x], [y-BAR_H/2, y+BAR_H/2+0.10], ls="-",
                color=_GRN, lw=1.4, alpha=0.95, zorder=8, solid_capstyle="butt")
        ax.plot([x], [y+BAR_H/2+0.10], marker="v", color=_GRN, markersize=4.5,
                zorder=9, clip_on=False)
        _nst += 1; seq += 1
if _nck:
    ax.text(xmax, LANE_Y["TRANSFER"]+BAR_H/2+0.16,
            "▼ green solid = transfer START · grey dotted = chunk landed / backpatch+chain start",
            ha="right", va="bottom", fontsize=5.5, color="#555", alpha=0.9)

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
