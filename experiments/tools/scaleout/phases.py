import re,sys
d=sys.argv[1]
ev=[]
for h,sg in (("redis0","sg1"),("redis1","sg2"),("redis2","sg3")):
    for l in open(f"{d}/{h}/{h}_{sg}.log",errors="ignore"):
        m=re.search(r"(\d\d:\d\d:\d\d\.\d+) [*#] (.*)",l)
        if not m: continue
        t=m.group(1); x=m.group(2); k=None
        if "RDMA MIGRATE worker: started id=" in x: k="start"
        elif "state=TRANSFER" in x: k="transfer"
        elif "TRANSFER (overlap) finished" in x: k="copied"
        elif "MGN_TXN_DONE applied" in x: k="done"
        if k:
            hh,mm,ss=t.split(":"); ev.append((int(hh)*3600+int(mm)*60+float(ss),sg,k))
ev.sort(); cur={}; rows=[]; last=None; gaps=[]
for t,sg,k in ev:
    if k=="start":
        if last is not None: gaps.append(t-last)
        cur={"start":t,"sg":sg}
    elif cur and sg==cur["sg"]:
        cur[k]=t
        if k=="done": rows.append((cur["transfer"]-cur["start"],cur["copied"]-cur["transfer"],cur["done"]-cur["copied"])); last=t
n=len(rows); print("transfers",n)
for i,name in enumerate(("setup","copy","commit wait")): print("%-12s avg %.3f s  total %5.2f s"%(name,sum(r[i] for r in rows)/n,sum(r[i] for r in rows)))
print("%-12s avg %.3f s  total %5.2f s"%("driver gap",sum(gaps)/len(gaps),sum(gaps)))
