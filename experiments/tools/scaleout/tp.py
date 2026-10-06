import re,sys,datetime,collections
snap,d,sc=sys.argv[1],sys.argv[2],sys.argv[3]
k0=min(float(m.group(1)) for l in open(d+"/run.log") for m in [re.search(r"t_kill=([\d.]+)",l)] if m)
for c in ("ycsb0","ycsb1"):
    seen=set(); out=[]
    for l in open(f"{snap}/scaleout3to6_crash_{sc.lower()}/ycsb/{c}/tmp/ycsb_output_{c}"):
        m=re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d):(\d+) (\d+) sec: \d+ operations; ([\d.]+) current.*?READ AverageLatency\(us\)=([\d.]+)\](?:.*?UPDATE AverageLatency\(us\)=([\d.]+))?",l)
        if m and m.group(3) not in seen:
            seen.add(m.group(3)); t=datetime.datetime.strptime(m.group(1)+" +0000","%Y-%m-%d %H:%M:%S %z").timestamp()+int(m.group(2))/1000-k0
            if -2<t<14: out.append("%+.1f:%.0fk"%(t,float(m.group(4))/1e3))
    print(c," ".join(out))
