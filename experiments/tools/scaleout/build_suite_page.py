#!/usr/bin/env python3
"""One HTML page for a crash-suite experiment: the suite table with keys and GB moved per run, the
paper figure of every scenario, and (optionally) every run of every batch.

usage: build_suite_page.py <experiment dir> <out dir> [<lincheck out root>=<label>=<what changed> ...]
  writes <out dir>/index.html and <out dir>/figs/paper_<S>_recovery.png (run figures.sh first).
  Each extra argument is one batch of runs (an OUT_ROOT of run_all.sh, e.g. /tmp/lincheck/n10m_fix3_...)
  listed under "All runs", in the order given.
e.g. build_suite_page.py experiments/2026-10-22_scaleout_3to6_crash_recovery_10m /tmp/page \
       "/tmp/lincheck/n10m_suite_20261022_175901=Overnight suite=baseline build"
"""
import csv, glob, html, os, re, shutil, struct, sys
E = os.path.abspath(sys.argv[1])
OUT = os.path.abspath(sys.argv[2])
SLOT_B = 2 * 1024 * 1024            # one landing block per slot
SLOTS_TOTAL, SLOTS_A = 3 * 2730, 2730

# scenario -> (title, model, recovery path, note)
M = {
 "HEALTHY": ("No fault", "base", "–", ""),
 "C1": ("Donor leader, after START, before prepare", "fs", "Donor resume, re-ship", ""),
 "C2": ("Donor leader, during the RDMA writes", "fs", "Donor resume", ""),
 "C3": ("Donor leader, after durable, before DONE", "fs", "Resume finds the round durable", ""),
 "C4": ("Recipient leader, session open not committed", "fs", "Re-drive", ""),
 "C5": ("Recipient leader, round on the leader only", "fs", "Re-home, range missing, re-ship", ""),
 "C6": ("Recipient leader, round on F1; F2 elected", "fs", "New leader fetches from a replica", "Election rigged (F1's timeout 2 s); a test hook holds F1's forward 6 s."),
 "C7": ("Recipient leader, after RECP_DURABLE", "fs", "Re-home, round durable", ""),
 "C8": ("Recipient follower F1", "fs", "Chain re-forms", ""),
 "C9": ("Donor follower (control)", "fs", "None needed", ""),
 "C10": ("Both leaders at the same instant", "fs", "Two resumes", ""),
 "C11": ("Recipient leader twice, first restarted", "out", "Restarted node not refilled", "Two of three recipient replicas fail: beyond f = 1, and needs a restart."),
 "C12": ("F1 then F2, each restarted", "out", "Restarted node not refilled; Raft catch-up stalls the leader", "Restart and rejoin are outside fail-stop."),
 "C13": ("Recipient leader killed and restarted", "out", "Restarted node not refilled", "Restart and rejoin are outside fail-stop."),
 "C14": ("Recipient follower F1 frozen 3 s", "fs", "Chain routes around it, catches it up", ""),
 "C15": ("TCP partition donor ↔ recipient, 5 s", "net", "Round durable during the partition; next round waits for the heal", "RDMA queue pairs stay up; only TCP is cut."),
 "C16": ("No fault; timeouts 150 / 20 / 50 ms", "net", "–", "No fault, so there is no recovery timeline: the run checks that very short timeouts do not disturb the migration."),
 "C17": ("Recipient leader, round on F1; F1 elected", "fs", "Held round applied on promotion", "Election rigged (F2's timeout 2 s); test hook as in C6."),
 "S1": ("Recipient leader, at the 4th chunk", "fs", "Re-home, re-ship", ""),
 "S2": ("Donor leader, during the transfer (= C2)", "fs", "Donor resume", ""),
 "S3": ("Donor follower (= C9)", "fs", "None needed", ""),
 "S4": ("Recipient follower (= C8)", "fs", "Chain re-forms", ""),
 "S5": ("Both leaders (= C10)", "fs", "Two resumes", ""),
 "S8": ("Donor leader, after block registration", "fs", "Donor resume", ""),
 "S9": ("Donor leader of A + recipient leader of B", "fs", "Two independent resumes", ""),
}
TAG = {"base": ("Baseline", "tag-base"), "fs": ("Fail-stop", "tag-fs"), "net": ("Network / timeouts", "tag-net"),
       "out": ("Beyond fail-stop", "tag-out")}
ORDER = ["HEALTHY"] + ["C%d" % i for i in range(1, 18)] + ["S1", "S2", "S3", "S4", "S5", "S8", "S9"]

rows = {r["scenario"]: r for r in csv.DictReader(open(E + "/figures/suite_table.csv"))}
when = {}
for l in open(E + "/data/summary.txt"):
    m = re.match(r"(\S+)T(\d\d:\d\d)\S* (\S+) q=", l)
    if m: when[m.group(3)] = (m.group(1), m.group(2))

def keys_moved(sc):
    try: t = open(E + "/data/%s/lincheck/postcheck.txt" % sc).read()
    except OSError: return None, None
    m = re.search(r"POSTCHECK donors.*", t)
    if not m: return None, None
    ks = [int(x) for x in re.findall(r"\((\d+) keys\)", m.group(0))]
    return (sum(ks), ks[0]) if ks else (None, None)

def png_size(p):
    with open(p, "rb") as f: h = f.read(24)
    return struct.unpack(">II", h[16:24])

gb = lambda b: b / 1e9
os.makedirs(OUT + "/figs", exist_ok=True)
data, tbl = [], []
for sc in ORDER:
    r = rows.get(sc)
    if not r: continue
    title, model, path, note = M[sc]
    k_all, k_a = keys_moved(sc)
    resent = int(r["re-sent slots"] or 0)
    d = dict(sc=sc, title=title, model=model, path=path, note=note, ok=r["correct"], k_all=k_all, k_a=k_a,
             resent=resent, resent_gb=gb(resent * SLOT_B), moved_gb=gb((SLOTS_TOTAL + resent) * SLOT_B),
             noleader=r["no leader (s)"] or "–", unavail=r["unavail (s)"] if r["unavail (s)"] not in ("", "-") else "–",
             mig=r["migration (s)"].replace("(-", "(−"), when=when.get(sc, ("", "")))
    fig = E + "/figures/paper_%s_recovery.png" % sc
    if os.path.exists(fig) and sc not in ("HEALTHY", "C16"):
        shutil.copy(fig, OUT + "/figs/paper_%s_recovery.png" % sc); d["fig"] = png_size(fig)
    data.append(d)

e = html.escape
fmtk = lambda k: "–" if k is None else "{:,}".format(k)
def res(ok): return '<span class="res res-%s">%s</span>' % ("pass" if ok == "PASS" else "fail", e(ok))

trs = []
for d in data:
    lab, cls = TAG[d["model"]]
    trs.append('<tr class="g-%s"><th scope="row"><a href="#%s">%s</a></th><td>%s</td><td><span class="tag %s">%s</span></td><td>%s</td>'
        '<td class="num">%s</td><td class="num">%.1f</td><td class="num">%s</td><td class="num">%s</td><td class="num">%s</td>'
        '<td class="num">%s</td><td>%s</td><td class="num dim">%s</td></tr>' % (
        d["model"], d["sc"], d["sc"], e(d["title"]), cls, lab, res(d["ok"]), fmtk(d["k_all"]), d["moved_gb"],
        e(d["noleader"]), e(d["unavail"]), e(d["mig"]),
        "0" if not d["resent"] else "%d · %.1f GB" % (d["resent"], d["resent_gb"]), e(d["path"]),
        "%s %s" % (d["when"][0][5:], d["when"][1])))

def card(d):
    meta = []
    if d["unavail"] != "–": meta.append("clients unavailable <b>%s s</b>" % e(d["unavail"]))
    meta.append("migration <b>%s s</b>" % e(d["mig"]))
    meta.append("moved <b>%s keys · %.1f GB</b>" % (fmtk(d["k_all"]), d["moved_gb"]))
    if d["resent"]: meta.append("re-sent <b>%d slots · %.1f GB</b>" % (d["resent"], d["resent_gb"]))
    img = ""
    if "fig" in d:
        w, h = d["fig"]
        img = '<div class="paper"><img src="figs/paper_%s_recovery.png" alt="Recovery timeline of %s: %s" loading="lazy" width="%d" height="%d"></div>' % (
            d["sc"], d["sc"], e(d["title"]), w, h)
    nt = '<p class="note">%s</p>' % e(d["note"]) if d["note"] else ""
    return ('<figure class="card" id="%s"><figcaption><span class="sid">%s</span>%s<span class="when">run %s %s UTC</span>'
            '<span class="ftitle">%s</span></figcaption>%s<p class="meta">%s</p><p class="path">%s</p>%s</figure>') % (
        d["sc"], d["sc"], res(d["ok"]), d["when"][0][5:], d["when"][1], e(d["title"]), img, " · ".join(meta), e(d["path"]), nt)

groups = [("fs", "Fail-stop scenarios", "A crashed node stays down."),
          ("net", "Network and timeouts", "No crash: a TCP partition between the two leaders, and very short timeouts."),
          ("out", "Beyond fail-stop: restart and rejoin", "Each restarts a crashed recipient node, which the fail-stop model does not cover. Lincheck passes; postcheck fails because the restarted replica is not fully refilled.")]
secs = "".join('<section class="group" id="g-%s"><h2>%s</h2><p class="lede">%s</p><div class="grid">%s</div></section>' % (
    g, t, l, "".join(card(d) for d in data if d["model"] == g)) for g, t, l in groups)

n = len(data); npass = sum(d["ok"] == "PASS" for d in data)
nfs = [d for d in data if d["model"] != "out"]; fs_pass = sum(d["ok"] == "PASS" for d in nfs)
ks = [d["k_all"] for d in data if d["k_all"]]; ka = [d["k_a"] for d in data if d["k_a"]]
kmin, kmax = min(ks), max(ks)
base_gb, a_gb = gb(SLOTS_TOTAL * SLOT_B), gb(SLOTS_A * SLOT_B)


# ---- every run of every batch, straight from /tmp/lincheck ----
import datetime, gzip
BATCHES = [a.split("=", 2) for a in sys.argv[3:]]   # [root, label, what changed]
def _ts(l):
    m = re.search(r"(\d+ \w+ \d{4} [\d:.]+)", l)
    return datetime.datetime.strptime(m.group(1), "%d %b %Y %H:%M:%S.%f").timestamp()
def run_stats(d):
    ev, resent = [], 0
    for f in glob.glob(d + "/logs/redis*/redis*_sg[123].log"):
        a = f.endswith("_sg1.log")
        for l in open(f, errors="ignore"):
            m = re.search(r"resume-plan: .*re-shipping (\d+)", l)
            if m: resent += int(m.group(1))
            if a:
                m = re.search(r"MGN_(TXN_START|TXN_DONE) applied: payload=\"sess=\d+(?: slots=(\d+)-)?", l)
                if m: ev.append((_ts(l), m.group(1), m.group(2)))
    ev.sort()
    st = sorted({s for _, k, s in ev if k == "TXN_START"}, key=int)
    gaps = []
    for a_, b_ in zip(st, st[1:]):
        tn = min(t for t, k, s in ev if k == "TXN_START" and s == b_)
        dn = [t for t, k, _ in ev if k == "TXN_DONE" and t < tn]
        if dn: gaps.append(tn - max(dn))
    full = None
    for l in open(d + "/playbook.log", errors="ignore"):
        m = re.search(r"FULL .*:\s+([\d.]+)s", l)
        if m: full = float(m.group(1)); break
    k = None
    try:
        m = re.search(r"POSTCHECK donors.*", open(d + "/postcheck.txt").read())
        k = sum(int(x) for x in re.findall(r"\((\d+) keys\)", m.group(0))) if m else None
    except OSError: pass
    return (max(gaps) if gaps else None), full, k, resent
allruns = []
for b, name, what in BATCHES:
    root = b
    res_ = {}
    if os.path.exists(root + "/summary.txt"):
        for l in open(root + "/summary.txt"):
            m = re.match(r"(\S+)T(\d\d:\d\d)\S* (\S+) q=\S+ rc=\d+ OVERALL: (\w+)", l)
            if m: res_[m.group(3)] = (m.group(4), m.group(2))
    for sc in ORDER:
        ds = sorted(glob.glob("%s/%s_q*" % (root, sc)))
        if not ds or sc not in res_: continue
        g, f, k, rs = run_stats(ds[-1])
        allruns.append(dict(batch=name, what=what, sc=sc, ok=res_[sc][0], t=res_[sc][1], gap=g, full=f, k=k, rs=rs))

css = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "suite_page.css")).read()

cur = None; rr = []
for r in allruns:
    if r["batch"] != cur:
        cur = r["batch"]
        rr.append('<tr class="bh"><th colspan="8" scope="colgroup">%s <span class="dim">· %s</span></th></tr>' % (e(cur), e(r["what"])))
    rr.append('<tr><th scope="row">%s</th><td class="num dim">%s</td><td>%s</td><td class="num">%s</td><td class="num">%s</td>'
              '<td class="num">%s</td><td class="num">%.1f</td><td class="num">%s</td></tr>' % (
        r["sc"], r["t"], res(r["ok"]), "–" if r["full"] is None else "%.2f" % r["full"],
        "–" if r["gap"] is None else "%.2f" % r["gap"], fmtk(r["k"]), gb((SLOTS_TOTAL + r["rs"]) * SLOT_B),
        "0" if not r["rs"] else "%d · %.1f GB" % (r["rs"], gb(r["rs"] * SLOT_B))))
runs_sec = '' if not allruns else ('<section class="group" id="runs" aria-labelledby="r-h"><h2 id="r-h">All runs</h2>'
  '<p class="lede">Every run, batch by batch. The table and figures above take each scenario\'s latest run of the main suite (the probe test is kept apart).</p>'
  '<div class="tablewrap"><table class="runs"><thead><tr><th>Run</th><th>UTC</th><th>Correct</th><th>Migration (s)</th>'
  '<th>Longest wait between pair A rounds (s)</th><th>Keys moved</th><th>Data moved (GB)</th><th>Re-sent</th></tr></thead><tbody>%s</tbody></table></div>'
  '<ul class="notes"><li>Longest wait: from a round\'s TXN_DONE to the next round\'s TXN_START on the donor group of pair A. In a fault-free round it is about 0.02 s.</li>'
  '<li>Data moved: 8,190 slots × 2 MiB, plus 2 MiB for every re-sent slot.</li></ul></section>') % "".join(rr)
page = """<title>Scale-out Crash Suite</title>
<meta name="claude:viewport-fit-sides" content="cover">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Source+Serif+4:opsz,wght@8..60,400;8..60,600&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap">
<style>%(css)s</style>
<div class="wrap">
<header>
  <div class="eyebrow">AqRaft · 3 → 6 scale-out · crash recovery</div>
  <h1>Crash recovery of the 3 → 6 scale-out on 10M keys</h1>
  <p class="intro">Every recovery rule of the migration protocol, exercised by one fault injected into pair A (donor sg1 → recipient sg4) during the scale-out. Each run is checked for linearizability on a 200-key probe and, afterwards, for a complete migration with identical replicas and no missing key. Times are seconds after the fault.</p>
  <ul class="setup"><li>10M keys · 3 donor groups</li><li>4 rounds × 682 slots</li><li>8-slot chunks</li><li>election timeout 500 ms</li><li>YCSB 50/50, uniform, 2 × 400 threads</li><li>one run per scenario</li></ul>
</header>
<section class="summary" aria-label="Summary">
  <div class="stat"><b>%(n)d</b><span>runs: HEALTHY, C1–C17, S1–S5, S8, S9</span></div>
  <div class="stat"><b>%(kall)s</b><span>keys moved per run (%(kmin)s–%(kmax)s), half of the 10M</span></div>
  <div class="stat"><b>%(bgb).1f GB</b><span>copied over RDMA per run: 8,190 slots × 2 MiB, plus any re-sent rounds</span></div>
  <div class="stat"><b>%(fsp)d / %(fsn)d</b><span>fail-stop and network scenarios pass every check</span></div>
  <div class="stat bad"><b>%(nout)d</b><span>restart scenarios fail postcheck (outside fail-stop)</span></div>
</section>
<section class="group" aria-labelledby="d-h">
  <h2 id="d-h">What one run moves</h2>
  <div class="tablewrap"><table class="small">
  <thead><tr><th>Pair</th><th>Slots</th><th>Keys moved</th><th>Copied over RDMA</th><th>Per round</th></tr></thead>
  <tbody>
  <tr><th scope="row">A · sg1 → sg4</th><td class="num">0–2729 (2,730)</td><td class="num">%(ka)s</td><td class="num">%(agb).2f GB</td><td class="num">682 slots · 1.43 GB</td></tr>
  <tr><th scope="row">B · sg2 → sg5</th><td class="num">5461–8190 (2,730)</td><td class="num">≈ 1.66M</td><td class="num">%(agb).2f GB</td><td class="num">682 slots · 1.43 GB</td></tr>
  <tr><th scope="row">C · sg3 → sg6</th><td class="num">10922–13651 (2,730)</td><td class="num">≈ 1.67M</td><td class="num">%(agb).2f GB</td><td class="num">682 slots · 1.43 GB</td></tr>
  <tr class="total"><th scope="row">All three</th><td class="num">8,190</td><td class="num">%(kall)s</td><td class="num">%(bgb).2f GB</td><td class="num">12 rounds</td></tr>
  </tbody></table></div>
  <ul class="notes">
    <li>Keys moved: the post-check's count of the migrated ranges on the recipients after each run, %(kmin)s to %(kmax)s keys (YCSB updates only, no inserts, so the count barely changes).</li>
    <li>Copied over RDMA: each slot travels as one 2 MiB landing block (1 GB = 10⁹ bytes; %(bgb).2f GB = 16.0 GiB). The records in it are smaller: about 1 KB each (YCSB, 10 fields × 100 B), so about %(rec).1f GB of record data.</li>
    <li>A re-sent round (682 slots) copies another 1.43 GB; it is added to “Data moved” in the table.</li>
  </ul>
</section>
<section class="group" aria-labelledby="t-h">
  <h2 id="t-h">All scenarios</h2>
  <div class="tablewrap"><table>
  <thead><tr><th>Run</th><th>Fault</th><th>Model</th><th>Correct</th><th>Keys moved</th><th>Data moved (GB)</th><th>No leader (s)</th><th>Clients unavailable (s)</th><th>Migration (s)</th><th>Re-sent</th><th>Recovery path</th><th>Run (UTC)</th></tr></thead>
  <tbody>%(trs)s</tbody></table></div>
  <ul class="notes">
    <li>Correct = lincheck (linearizable probe history) and postcheck (migration complete, replicas identical, none of the 10M keys missing).</li>
    <li>Migration: first TXN_START to last TXN_DONE over all three pairs; in brackets, the difference to HEALTHY.</li>
    <li>C11–C13 restart a crashed node. The new leader only refills rounds committed in its own term, so the restarted replica misses older rounds; in C12 the Raft catch-up of a far-behind follower also stalls the leader.</li>
  </ul>
</section>
%(secs)s
%(runs)s
<footer>Data and figures: <code>experiments/2026-10-22_scaleout_3to6_crash_recovery_10m/</code> · figures by <code>experiments/tools/scaleout/paper_recovery.py</code> (3.33 in, one column of a two-column paper).</footer>
</div>
""" % dict(css=css, n=n, kall="%.2fM" % (sum(ks) / len(ks) / 1e6), kmin="{:,}".format(kmin), kmax="{:,}".format(kmax),
           bgb=base_gb, agb=a_gb, ka="%.2fM" % (sum(ka) / len(ka) / 1e6), fsp=fs_pass, fsn=len(nfs), nout=n - len(nfs),
           rec=sum(ks) / len(ks) * 1000 / 1e9, trs="".join(trs), secs=secs, runs=runs_sec)
open(OUT + "/index.html", "w").write(page)
print("wrote %s/index.html: %d scenarios, %d PASS, %d runs listed" % (OUT, n, npass, len(allruns)))
