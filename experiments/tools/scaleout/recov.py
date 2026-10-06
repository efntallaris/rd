import re,sys,glob,os,datetime,collections
root,snap=sys.argv[1],sys.argv[2]
PAIR={"sg1":"A","sg2":"B","sg3":"C","sg4":"A","sg5":"B","sg6":"C"}
def ts(s): return datetime.datetime.strptime(s,"%d %b %Y %H:%M:%S.%f").timestamp()
for sc in ["S1","S2","S3","S4","S5","S6","S7","S8","S9"]:
    d=glob.glob(f"{root}/{sc}_*")[0]
    kills=[(float(m.group(2)),m.group(1)) for l in open(d+"/run.log") for m in [re.search(r"KILLED (\S+) .*t_kill=([\d.]+)",l)] if m]
    k0=min(k for k,_ in kills); kl=max(k for k,_ in kills)
    lead=[]; tr={}; rec=collections.Counter(); starts=[]
    for f in glob.glob(d+"/logs/redis*/*.log"):
        n=os.path.basename(f)[:-4]; host,sg=n.split("_")
        for l in open(f,errors="ignore"):
            m=re.match(r"\d+:\w (\d\d \w\w\w \d{4} \d\d:\d\d:\d\d\.\d+) [*#] (.*)",l)
            if not m: continue
            x=m.group(2)
            if "now a leader" in x:
                t=ts(m.group(1))
                if t>k0-0.5: lead.append((t-k0,sg,host))
            elif "RDMA MIGRATE worker: started id=" in x:
                i=re.search(r"id=(\d+)",x).group(1); starts.append((ts(m.group(1)),sg,i)); tr.setdefault((sg,i),{})["start"]=ts(m.group(1))
            elif "TRANSFER (overlap) finished" in x: pass
            elif re.search(r"worker: id=(\d+) DONE",x):
                i=re.search(r"id=(\d+)",x).group(1); tr.setdefault((sg,i),{})["done"]=ts(m.group(1))
            elif re.search(r"worker: id=(\d+) state=BACKPATCH",x):
                i=re.search(r"id=(\d+)",x).group(1); tr.setdefault((sg,i),{})["copied"]=ts(m.group(1))
            for key,pat in (("donor re-home","re-shipped slots"),("resume-plan","resume-plan"),("MGN-RECOVER","MGN-RECOVER"),("chain member excluded","excluding from chain"),("catch-up","catch-up: sess"),("stall-abort","STALL-ABORT")):
                if pat in x and ts(m.group(1))>k0: rec[key]+=1
    # state of every pair at the first kill
    st={}
    for (sg,i),e in tr.items():
        if "start" in e and e["start"]<=k0:
            p=PAIR[sg]
            if e.get("done",1e18)>k0: st[p]="copying" if e.get("copied",1e18)>k0 else "committing"
    starts.sort(); after=[s for s in starts if s[0]>k0]
    prev=max([e.get("done",0) for e in tr.values() if e.get("done",0)<=k0]+[max([s[0] for s in starts if s[0]<=k0],default=k0)])
    gaps=[]; last=k0
    for t,sg,i in after: gaps.append((t-last,PAIR[sg],i)); last=t
    w=float(re.search(r"FULL.*?:\s+([\d.]+)s",open(f"{snap}/scaleout3to6_crash_{sc.lower()}/migration_window.txt").read()).group(1))
    # ycsb
    tot=collections.Counter(); seen=set()
    for c in ("ycsb0","ycsb1"):
        for l in open(f"{snap}/scaleout3to6_crash_{sc.lower()}/ycsb/{c}/tmp/ycsb_output_{c}"):
            m=re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d):\d+ (\d+) sec: \d+ operations; ([\d.]+) current",l)
            if m and (c,m.group(2)) not in seen:
                seen.add((c,m.group(2))); t=datetime.datetime.strptime(m.group(1)+" +0000","%Y-%m-%d %H:%M:%S %z").timestamp(); tot[int(t-k0)]+=float(m.group(3))
    pre=sum(tot[s] for s in range(-6,-1))/5
    low=[s for s in range(0,40) if tot[s]<0.5*pre]; back=next((s for s in range(max(low,default=0),60) if tot[s]>=0.9*pre),None)
    print(f"== {sc}: kill {', '.join('%s +%.2f'%(h,k-k0) for k,h in kills)}; pairs in flight at kill: {st or 'none'}")
    print("   elections: "+("; ".join("%s -> %s at +%.2f s"%(sg,h,t) for t,sg,h in sorted(lead)) or "none"))
    big=sorted(gaps,reverse=True)[:2]
    print("   migration: window %.1f s (fault-free 9.4); longest pauses before a transfer start: %s; next start after kill +%.2f s"%(w,", ".join("%.2f s (pair %s)"%(g,p) for g,p,_ in big),after[0][0]-k0 if after else -1))
    print("   recovery actions: "+(", ".join("%s x%d"%kv for kv in rec.items()) or "none"))
    print("   clients: before %.0fk ops/s, minimum %.0fk at +%ds, %d s below half, back to 90%% at +%s s"%(pre/1e3,min(tot[s] for s in range(0,30))/1e3,min(range(0,30),key=lambda s:tot[s]),len(low),back))
