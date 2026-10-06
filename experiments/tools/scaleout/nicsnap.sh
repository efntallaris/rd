#!/bin/bash
# nicsnap.sh <file>: RoCE + port counters of redis0..5
for h in redis0 redis1 redis2 redis3 redis4 redis5; do
  sudo ssh $h 'cd /sys/class/infiniband/mlx5_3/ports/1/hw_counters; for c in out_of_sequence packet_seq_err local_ack_timeout_err; do echo "'$h' $c $(cat $c)"; done; ethtool -S ens1f1np1 | grep -E " (rx_discards_phy|rx_out_of_buffer|tx_global_pause|rx_global_pause|rx_bytes_phy|tx_bytes_phy):" | sed "s/^ */'$h' /; s/://"'
done > $1
