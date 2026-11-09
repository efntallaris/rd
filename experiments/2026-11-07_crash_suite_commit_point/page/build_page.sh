#!/bin/bash
# Page assets from the commit-point experiment folder: breakdowns (as rendered by figures.sh),
# throughput/latency with the 100G data sizes, the figures-only page, the standalone HTML + zip.
S="$(cd "$(dirname "$0")" && pwd)"   # this folder: all100/ (page + figs), files.json
H=/users/entall/rd/experiments/2026-11-07_crash_suite_commit_point
A=$S/all100/figs; ST=$H/figures/.tputlat_stage
CASES="S1 S2 S5 S8 S9 S10 S11 S12"
for s in $CASES; do
  for x in "recovery_wide.png:100g_wide.png" "recovery_wide.pdf:100g_wide.pdf" "recovery.pdf:100g_col.pdf"; do
    cp $H/figures/paper_${s}_${x%%:*} $A/paper_${s}_${x##*:}
  done
done
for s in HEALTHY $CASES; do
  sudo env CMU_DIR=/users/entall/rd/experiments/tools/fonts/cmu GRAY=1 BLOCK_MIB=12.1862 python3 \
    /users/entall/rd/experiments/2026-10-02_scaleout_3_to_6/plot_scaleout_ycsb.py $ST/ycsb $ST/lin $A/tputlat_${s}_100g.png $s >/dev/null 2>&1
done
sudo env CMU_DIR=/users/entall/rd/experiments/tools/fonts/cmu GRAY=1 BLOCK_MIB=12.1862 python3 \
  /users/entall/rd/experiments/2026-10-02_scaleout_3_to_6/plot_scaleout_ycsb.py $ST/ycsb $ST/lin $A/tputlat_all_100g.png >/dev/null 2>&1
sudo chown -R $(id -u) $S/all100
cd $S/all100 && python3 - <<'PY'
import io,re,base64,zipfile,glob
p="aqraft_100g.html"; s=io.open(p,encoding="utf-8").read()
head=s[:s.index('<div class="fig"><p class="lab">No fault</p>')]
SC=[("HEALTHY","No fault"),("S1","S1 &middot; recipient leader"),("S2","S2 &middot; donor leader (no majority)"),
    ("S5","S5 &middot; both leaders"),("S8","S8 &middot; donor leader after registration"),("S9","S9 &middot; two leaders, two pairs"),
    ("S10","S10 &middot; no majority: recipient leader, round only on it"),
    ("S11","S11 &middot; majority: recipient leader after RECP_DURABLE"),
    ("S12","S12 &middot; majority: recipient leader, round on F1 not F2")]
parts=[]
for k,t in SC:
    imgs=[] if k=="HEALTHY" else [f'<img src="figs/paper_{k}_100g_wide.png" loading="lazy" alt="{k} recovery breakdown">']
    imgs.append(f'<img src="figs/tputlat_{k}_100g.png" loading="lazy" alt="{k} throughput and latency">')
    parts.append(f'<div class="fig"><p class="lab">{t}</p><div class="pair">'+"".join(imgs)+'</div></div>')
s=head+"\n".join(parts)+"\n</div>\n"
io.open(p,"w",encoding="utf-8").write(s)
e=re.sub(r'src="(figs/[^"]+\.png)"',lambda m:'src="data:image/png;base64,%s"'%base64.b64encode(open(m.group(1),"rb").read()).decode(),s)
doc='<!doctype html>\n<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">\n'+e.replace('</style>','</style>\n</head><body>',1)+'</body></html>\n'
open("aqraft_100g_standalone.html","w",encoding="utf-8").write(doc)
z=zipfile.ZipFile("../aqraft_100g.zip","w",zipfile.ZIP_DEFLATED)
z.write("aqraft_100g_standalone.html","aqraft_100g/aqraft_100g.html")
for f in sorted(glob.glob("figs/*_100g*")):
    if not re.search(r"_S[67]_", f): z.write(f,"aqraft_100g/"+f)
z.close()
print(s.count("<img"),"images on the page")
PY
cp $S/all100/aqraft_100g_standalone.html /users/entall/rd/aqraft_100g.html && cp $S/aqraft_100g.zip /users/entall/rd/aqraft_100g.zip
echo "page built $(date -u +%T)"

# fresh figure paths every build (browsers cache figs/<name>.png across republishes); the
# Artifact publish must pass files.json as `files` (new paths -> local figs, old paths -> null)
V=v$(date +%H%M%S)
sed -i -E "s#src=\"figs/(v[0-9]+/)?#src=\"figs/$V/#g" $S/all100/aqraft_100g.html
python3 - "$S" <<'PY'
import json, re, sys
S = sys.argv[1]
h = open(S + "/all100/aqraft_100g.html").read()
used = sorted(set(re.findall(r'src="(figs/v\d+/[^"]+)"', h)))
m = {u: "figs/" + u.split("/")[-1] for u in used}
try:
    for u in json.load(open(S + "/files.json")):
        if u not in m and u.startswith("figs/v"): m[u] = None
except Exception: pass
json.dump(m, open(S + "/files.json", "w"))
print("figure paths", used[0].split("/")[1] if used else "?", "->", len(used), "files; files.json written")
PY
