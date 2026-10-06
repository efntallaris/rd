import re,sys,datetime
d,f=sys.argv[1],sys.argv[2]; a0,a1=float(sys.argv[3]),float(sys.argv[4])
def ts(s): return datetime.datetime.strptime(s,"%d %b %Y %H:%M:%S.%f").timestamp()
ev=[]
for n in ("redis0_sg1","redis1_sg2","redis2_sg3"):
    h=n.split("_")[0]
    for l in open(f"{d}/logs/{h}/{n}.log",errors="ignore"):
        m=re.match(r"\d+:\w (\d\d \w\w\w \d{4} \d\d:\d\d:\d\d\.\d+) [*#] RDMA MIGRATE worker: (started id=(\d+)|id=(\d+) state=(TRANSFER|BACKPATCH)|id=(\d+) DONE)",l)
        if m: ev.append((ts(m.group(1)),"ABC"[int(n[-1])-1],"start" if m.group(3) else (m.group(5) or "DONE")))
t0=min(t for t,_,k in ev if k=="start")
rows=[l.split() for l in open(f)]
rows=[(float(r[0]),[int(x) for x in r[1:]]) for r in rows if len(r)==7]
print("time   sg1  sg2  sg3  sg4  sg5  sg6  total  events")
prev=None
for (ta,a),(tb,b) in zip(rows,rows[1:]):
    t=tb-t0
    if not a0<=t<=a1 or tb-ta<=0 or min(a)<0 or min(b)<0: continue
    v=[(y-x)/(tb-ta)/2/1e3 for x,y in zip(a,b)]
    e=" ".join("%s:%s"%(p,k) for tt,p,k in sorted(ev) if ta-t0<tt-t0<=t)
    print("%+5.1f  "%t+" ".join("%4.0f"%x for x in v)+"  %5.0f  %s"%(sum(v),e))
