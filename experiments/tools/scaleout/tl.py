import re,sys,glob,os,datetime
d=sys.argv[1]; span=float(sys.argv[2]) if len(sys.argv)>2 else 6
kills=[float(m.group(1)) for l in open(d+"/run.log") for m in [re.search(r"KILLED.*t_kill=([\d.]+)",l)] if m]
k0=min(kills)
pat=re.compile(sys.argv[3] if len(sys.argv)>3 else r"now a leader|now a follower|unresponsive|REHOME|re-home|re-ship|resume-plan|MGN-RECOVER|RESUME-STATUS|TXN_DONE applied|TXN_START applied|RECP_TXN_START applied|INDX_UPD applied|re-form|catch-up|CATCH|repair|STALL|superseded|broken|promot|FAILED|failed|timeout|NARROW applied|started id=|state=TRANSFER|TRANSFER \(overlap\) finished")
ev=[]
for f in glob.glob(d+"/logs/redis*/*.log"):
    n=os.path.basename(f)[:-4]
    for l in open(f,errors="ignore"):
        m=re.match(r"\d+:\w (\d\d \w\w\w \d{4} \d\d:\d\d:\d\d\.\d+) [*#] (.*)",l)
        if not m or not pat.search(m.group(2)): continue
        t=datetime.datetime.strptime(m.group(1),"%d %b %Y %H:%M:%S.%f").timestamp()   # local tz
        if -float(sys.argv[4] if len(sys.argv)>4 else 0.6)<=t-k0<=span: ev.append((t-k0,n,m.group(2)[:150]))
ev.sort()
print("kills at +%s s"%", +".join("%.2f"%(k-k0) for k in kills))
for t,n,x in ev: print("%+7.3f %-11s %s"%(t,n,x))
