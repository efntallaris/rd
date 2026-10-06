import re,sys,glob,os,datetime,collections
root=sys.argv[1]
PAIR={"sg1":"A","sg2":"B","sg3":"C"}
def ts(s): return datetime.datetime.strptime(s,"%d %b %Y %H:%M:%S.%f").timestamp()
for sc in sys.argv[2:]:
    d=glob.glob(f"{root}/{sc}_*")[0]
    k0=min(float(m.group(1)) for l in open(d+"/run.log") for m in [re.search(r"t_kill=([\d.]+)",l)] if m)
    ev=[]
    for f in glob.glob(d+"/logs/redis*/*.log"):
        host,sg=os.path.basename(f)[:-4].split("_")
        if sg not in PAIR: continue
        for l in open(f,errors="ignore"):
            m=re.match(r"\d+:\w (\d\d \w\w\w \d{4} \d\d:\d\d:\d\d\.\d+) [*#] RDMA MIGRATE worker: (started id=(\d+)|id=(\d+) state=(TRANSFER|BACKPATCH)|id=(\d+) (DONE|FAILED))",l)
            if m:
                i=m.group(3) or m.group(4) or m.group(6); k="start" if m.group(3) else (m.group(5) or m.group(7))
                ev.append((ts(m.group(1))-k0,PAIR[sg],i,k,host))
    ev.sort(); cur={}; rows=[]
    for t,p,i,k,h in ev:
        e=cur.setdefault((p,i),{"p":p,"i":i,"h":h}); e[k]=t
    print("==",sc)
    prev_end=None
    for e in sorted(cur.values(),key=lambda e:e.get("start",0)):
        s=e.get("start"); 
        if s is None: continue
        end=e.get("DONE",e.get("FAILED"))
        print("  %+7.2f pair %s id=%-18s on %s: wait-before %5.2f  setup %5.2f  copy %5.2f  commit %5.2f  -> %s"%(s,e["p"],e["i"],e["h"],(s-prev_end) if prev_end is not None else 0,
              e.get("TRANSFER",s)-s if "TRANSFER" in e else float("nan"), e.get("BACKPATCH",0)-e.get("TRANSFER",0) if "BACKPATCH" in e and "TRANSFER" in e else float("nan"),
              (end-e["BACKPATCH"]) if end is not None and "BACKPATCH" in e else float("nan"), "DONE" if "DONE" in e else ("FAILED" if "FAILED" in e else "no end (leader died / superseded)")))
        if end is not None: prev_end=end
