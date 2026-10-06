import sys,collections
def rd(f):
    d={}
    for l in open(f):
        h,c,v=l.split(); d[(h,c)]=int(v)
    return d
a,b=rd(sys.argv[1]),rd(sys.argv[2])
cs=["out_of_sequence","packet_seq_err","local_ack_timeout_err","rx_discards_phy","rx_out_of_buffer","tx_global_pause","rx_global_pause"]
print("%-7s"%""+" ".join("%12s"%c[:12] for c in cs)+"   rx_GB   tx_GB")
for h in ["redis%d"%i for i in range(6)]:
    print("%-7s"%h+" ".join("%12d"%(b[(h,c)]-a[(h,c)]) for c in cs)+"  %6.1f  %6.1f"%((b[(h,"rx_bytes_phy")]-a[(h,"rx_bytes_phy")])/1e9,(b[(h,"tx_bytes_phy")]-a[(h,"tx_bytes_phy")])/1e9))
