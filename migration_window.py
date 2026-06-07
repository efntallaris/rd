#!/usr/bin/env python3
"""
migration_window.py <expdir>

Prints the COLD-EXCLUDED migration window for an aqraft reshard run:

    sg1 FLIPPING (first donor ownership flip, AFTER its one-time cold RDMA
    registration) -> last donor DONE (migration fully complete).

This excludes sg1/self's one-time PREP + REGISTERING (ibv_reg_mr of the big-MR
pool), which only happens on the first round and isn't migration work. Also
reports the FULL window (first MGN_TXN_START -> last MGN_TXN_DONE) and the cold
tax (the difference) for context.

Donor-order- and round-count-agnostic: scans every donor shardgroup log and
takes the earliest `state=FLIPPING` and the latest `DONE n_slots`.
"""
import re, sys, glob, os

def _s(t):
    h, m, c = t.split(':'); return int(h)*3600 + int(m)*60 + float(c)

def main(expdir):
    # Donor worker phases live in the sg1/sg2/sg3 leader logs (not sg4/recipient).
    logs = [f for f in glob.glob(f"{expdir}/logs/*/tmp/redis_logs/*_sg*.log")
            if "_sg4" not in os.path.basename(f)]
    flips, dones, txstart, txdone = [], [], [], []
    for f in logs:
        for line in open(f, errors="replace"):
            t = re.match(r'.*?(\d\d:\d\d:\d\d\.\d+)', line)
            if not t:
                continue
            ts = t.group(1)
            if "RDMA MIGRATE worker:" in line and "state=FLIPPING" in line:
                flips.append(ts)
            elif "RDMA MIGRATE worker:" in line and "DONE n_slots" in line:
                dones.append(ts)
            elif "MGN_TXN_START applied" in line:
                txstart.append(ts)
            elif "MGN_TXN_DONE applied" in line:
                txdone.append(ts)
    if not (flips and dones):
        print(f"migration_window: no donor FLIPPING/DONE events under {expdir}")
        return 1
    f0 = min(flips, key=_s); d1 = max(dones, key=_s)
    out = []
    out.append("==================== MIGRATION WINDOW ====================")
    out.append(f"  COLD-EXCLUDED  (sg1 FLIPPING -> last donor DONE) : {_s(d1)-_s(f0):6.2f}s   [{f0} -> {d1}]")
    if txstart and txdone:
        t0 = min(txstart, key=_s); t1 = max(txdone, key=_s)
        out.append(f"  FULL           (first TXN_START -> last TXN_DONE): {_s(t1)-_s(t0):6.2f}s   [{t0} -> {t1}]")
        out.append(f"  cold tax (sg1 PREP+REGISTERING, excluded)       : {_s(f0)-_s(t0):6.2f}s")
    out.append("=========================================================")
    txt = "\n".join(out)
    print(txt)
    try:
        with open(f"{expdir}/migration_window.txt", "w") as fh:
            fh.write(txt + "\n")
    except Exception:
        pass
    return 0

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: migration_window.py <expdir>"); sys.exit(2)
    sys.exit(main(sys.argv[1]))
