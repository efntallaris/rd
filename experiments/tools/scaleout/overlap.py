import re,sys
d=sys.argv[1]; ev=[]
for h,sg in (("redis0","sg1"),("redis1","sg2"),("redis2","sg3")):
    for l in open(f"{d}/{h}/{h}_{sg}.log",errors="ignore"):
        m=re.search(r"(\d\d):(\d\d):(\d\d\.\d+) [*#] RDMA MIGRATE worker: (id=\d+ state=TRANSFER|TRANSFER \(overlap\) finished|started id)",l)
        if m:
            t=int(m.group(1))*3600+int(m.group(2))*60+float(m.group(3)); k="S" if "state=TRANSFER" in m.group(4) else ("E" if "finished" in m.group(4) else "D")
            ev.append((t,sg,k))
ev.sort(); t0=ev[0][0]
cop=[]; cur={}
for t,sg,k in ev:
    if k=="S": cur[sg]=t
    elif k=="E": cop.append((cur.pop(sg),t,sg))
cop.sort()
ov=sum(max(0,cop[i][1]-cop[i+1][0]) for i in range(len(cop)-1)); idle=sum(max(0,cop[i+1][0]-cop[i][1]) for i in range(len(cop)-1))
print("copies %d  copy time %.2f s  span %.2f s  overlap between consecutive copies %.3f s  idle between copies %.2f s (avg %.3f)"%(len(cop),sum(e-s for s,e,_ in cop),cop[-1][1]-cop[0][0],ov,idle,idle/(len(cop)-1)))
