#!/usr/bin/env python3
"""
build_artifact.py — self-contained HTML artifact from an analyzed crash campaign.

Usage:
  python3 build_artifact.py [RESULTS_DIR] [OUT_HTML]

Reads RESULTS_DIR/metrics.json + RESULTS_DIR/<S>/fig.png (from analyze_crash.py),
embeds the figures as data-URIs, writes a theme-aware standalone page.
Defaults: RESULTS_DIR=/tmp/crash_inject/campaign  OUT=RESULTS_DIR/crash_artifact.html
"""
import os, sys, json, base64

RES = sys.argv[1] if len(sys.argv) > 1 else "/tmp/crash_inject/campaign"
OUT = sys.argv[2] if len(sys.argv) > 2 else os.path.join(RES, "crash_artifact.html")
M = json.load(open(os.path.join(RES, "metrics.json")))
ORDER = [s for s in ("HEALTHY","S1","S2","S3","S4","S5") if s in M]

def b64(path):
    return base64.b64encode(open(path, "rb").read()).decode() if os.path.exists(path) else ""

def verdict(S, r):
    if S == "HEALTHY": return "BASELINE"
    ok = (r.get("crash", 0) == 0) and (r.get("upd_err") in (0, None)) and (r.get("plateau", 0) > r.get("pre", 1))
    return "PASS" if ok else "CHECK"

def cell(x, fmt="{:,.0f}"):
    return fmt.format(x) if isinstance(x, (int, float)) else "—"

rows = "\n".join(
    f'<tr><td class="s">{S}</td><td>{M[S]["name"]}</td>'
    f'<td><span class="v {verdict(S,M[S]).lower()}">{verdict(S,M[S])}</span></td>'
    f'<td class="n">{M[S]["rise"]:+.1f}%</td><td class="n">{cell(M[S]["plateau"])}</td>'
    f'<td class="n">{cell(M[S]["t2f"])}{"s" if isinstance(M[S]["t2f"],(int,float)) else ""}</td>'
    f'<td class="n">{cell(M[S]["crash"])}</td><td class="n">{cell(M[S]["upd_err"])}</td>'
    f'<td class="n">{cell(M[S]["dbsize"])}</td></tr>'
    for S in ORDER)

cards = "\n".join(
    f'<div class="card"><h3>{S} — {M[S]["name"]}</h3>'
    f'<p class="tgt">{M[S]["target"]}</p>'
    f'<img src="data:image/png;base64,{b64(os.path.join(RES,S,"fig.png"))}" alt="{S}"></div>'
    for S in ORDER)

n_pass = sum(1 for S in ORDER if verdict(S, M[S]) == "PASS")
n_crash = sum(M[S].get("crash", 0) for S in ORDER)

html = f"""<title>AqRaft crash campaign</title>
<style>
:root{{--bg:#fff;--fg:#1a1a1a;--mut:#666;--line:#e2e2e2;--card:#fafafa;--good:#2b8a3e;--bad:#c92a2a;--accent:#1971c2}}
@media(prefers-color-scheme:dark){{:root{{--bg:#16181c;--fg:#e8e8e8;--mut:#9aa0a8;--line:#2a2d33;--card:#1d2025;--good:#51cf66;--bad:#ff6b6b;--accent:#4dabf7}}}}
:root[data-theme=dark]{{--bg:#16181c;--fg:#e8e8e8;--mut:#9aa0a8;--line:#2a2d33;--card:#1d2025;--good:#51cf66;--bad:#ff6b6b;--accent:#4dabf7}}
:root[data-theme=light]{{--bg:#fff;--fg:#1a1a1a;--mut:#666;--line:#e2e2e2;--card:#fafafa;--good:#2b8a3e;--bad:#c92a2a;--accent:#1971c2}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--fg);font:15px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}}
.wrap{{max-width:1040px;margin:0 auto;padding:32px 22px 72px}}
h1{{font-size:26px;margin:0 0 4px}}h2{{font-size:19px;margin:32px 0 12px;padding-bottom:6px;border-bottom:1px solid var(--line)}}
h3{{font-size:15px;margin:0 0 2px}}.sub{{color:var(--mut);font-size:14px;margin-bottom:18px}}
.hero{{display:flex;gap:14px;flex-wrap:wrap;margin:18px 0}}
.kpi{{flex:1;min-width:150px;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:15px 17px}}
.kpi .v{{font-size:24px;font-weight:700}}.kpi .l{{color:var(--mut);font-size:13px;margin-top:3px}}
table{{border-collapse:collapse;width:100%;font-size:13.5px;margin:8px 0;display:block;overflow-x:auto}}
th,td{{padding:8px 10px;text-align:left;border-bottom:1px solid var(--line);white-space:nowrap}}
th{{font-weight:600;color:var(--mut);font-size:12px;text-transform:uppercase;letter-spacing:.03em}}
td.n{{text-align:right;font-variant-numeric:tabular-nums}}td.s{{font-weight:700;color:var(--accent)}}
.v{{font-weight:700;font-size:12px;padding:1px 7px;border-radius:5px}}
.v.pass,.v.baseline{{color:var(--good)}}.v.check{{color:var(--bad)}}
.card{{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:13px;margin:14px 0;overflow-x:auto}}
.card img{{width:100%;max-width:100%;display:block;margin-top:6px}}.tgt{{color:var(--mut);font-size:13px;margin:0}}
</style>
<div class="wrap">
<h1>AqRaft reshard — crash-scenario campaign</h1>
<div class="sub">Fault injection during an RDMA cluster reshard · donors sg1/2/3 → recipient sg4 · one figure per scenario</div>
<div class="hero">
<div class="kpi"><div class="v">{len(ORDER)}</div><div class="l">scenarios run</div></div>
<div class="kpi"><div class="v good">{n_pass}</div><div class="l">PASS (0 crash · UPDATE-err 0 · plateau &gt; baseline)</div></div>
<div class="kpi"><div class="v {'good' if n_crash==0 else 'bad'}">{n_crash}</div><div class="l">crash signatures across all runs</div></div>
</div>
<h2>Results</h2>
<table>
<tr><th>run</th><th>scenario</th><th>verdict</th><th>rise</th><th>plateau</th><th>recovery</th><th>crash</th><th>upd-err</th><th>recipient dbsize</th></tr>
{rows}
</table>
<p class="sub"><b>rise</b> = post-migration plateau ÷ pre-migration baseline (both vary run-to-run; compare <b>plateaus</b>).
<b>recovery</b> = seconds after migration start to reach 95% of plateau. <b>PASS</b> = 0 crash signatures, 0 YCSB UPDATE errors, plateau above the pre-migration baseline.</p>
<h2>Per-scenario timelines</h2>
{cards}
</div>"""
open(OUT, "w").write(html)
print(f"wrote {OUT} ({len(html):,} bytes) for {ORDER}")
