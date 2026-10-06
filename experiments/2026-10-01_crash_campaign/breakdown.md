# 30M crash campaign — client performance and time breakdown

Seconds are relative to the migration start (first donor `PREP`). Baseline = mean throughput 25–8 s before the migration. "after" = 20–100 s after the last `TXN_DONE`.

## Client impact

| run | kill (s) | migration (s) | base Kops/s | min | after | s <50% | s <90% | back to 90% after kill (s) | Kops lost | READ avg µs pre / worst / after | UPDATE avg µs pre / worst / after | errors R/U |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No fault | — | 6.79 | 151.3 | 138.8 | 169.0 | 0 | 0 | — | 12.5 | 714 / 831 / 628 | 1921 / 2043 / 1729 | 0 / 0 |
| S1 recipient leader | +2.46 (redis3) | 14.64 | 161.6 | 0.0 | 185.2 | 4 | 6 | 5.6 | 751.6 | 669 / 766 / 579 | 1796 / 169880 / 1567 | 646 / 0 |
| S2 donor leader (mid-transfer) | +0.71 (redis0) | 9.89 | 150.1 | 22.8 | 172.5 | 2 | 3 | 3.0 | 283.9 | 716 / 3267 / 619 | 1940 / 11989 / 1692 | 4339 / 0 |
| S3 donor follower | +1.09 (redis1) | 6.7 | 160.5 | 141.5 | 173.0 | 0 | 1 | 0.0 | 37.2 | 674 / 831 / 615 | 1810 / 1999 / 1689 | 0 / 0 |
| S4 recipient follower | +0.82 (redis4) | 9.87 | 154.8 | 147.4 | 177.2 | 0 | 0 | 0.0 | 8.3 | 698 / 760 / 601 | 1876 / 1969 / 1644 | 0 / 0 |
| S5 donor leader after its transfer | +3.85 (redis0) | 6.81 | 150.0 | 31.7 | 173.7 | 1 | 1 | 1.8 | 128.2 | 739 / 1107 / 613 | 1942 / 7851 / 1680 | 1484 / 0 |

## Warm-up dip before the migration

| run | first warm-up event (s) | lowest Kops/s in the 10 s before the migration |
|---|---|---|
| No fault | -3.51 | 48.0 |
| S1 recipient leader | -3.45 | 45.3 |
| S2 donor leader (mid-transfer) | -3.44 | 53.7 |
| S3 donor follower | -3.45 | 55.9 |
| S4 recipient follower | -3.45 | 50.5 |
| S5 donor leader after its transfer | -3.48 | 51.9 |

## Phase timeline per donor (s since migration start)

**No fault**

| donor | TXN_START | FLIPPING | TRANSFER | transfer finished | BACKPATCH | TXN_DONE |
|---|---|---|---|---|---|---|
| sg1 | -0.0 | 0.01 | 0.05 | 1.07 | 1.07 | 2.99 |
| sg2 | 1.08 | 1.09 | 1.12 | 2.25 | 2.25 | 4.84 |
| sg3 | 2.27 | 2.28 | 2.31 | 3.32 | 3.32 | 6.79 |

recipient: chain acks at [1.36, 2.52, 3.61]; range commits (`RECP_TXN_DONE`) {'0-1364': 2.98, '5461-6825': 4.83, '10922-12286': 6.79}

**S1 recipient leader** — redis3 killed at +2.46 s

| donor | TXN_START | FLIPPING | TRANSFER | transfer finished | BACKPATCH | TXN_DONE |
|---|---|---|---|---|---|---|
| sg1 | -0.01 | 0.01 | 0.08 | 1.1 | 1.1 | 6.78 |
| sg2 | 1.11 | 1.12 | 1.15 | 2.3 | 2.3 | 9.75 |
| sg2(re-ship) | 6.58 | 6.61 | 6.66 | 7.71 | 7.71 | 9.75 |
| sg3 | 2.31 | 2.31 | 2.35 | 4.03 | — | 14.64 |
| sg3(re-ship) | 10.0 | 11.51 | 11.53 | 12.54 | 12.54 | 14.64 |

recipient: chain acks at [1.38, 7.99, 12.84]; range commits (`RECP_TXN_DONE`) {'0-1364': 6.71, '5461-6825': 9.74, '10922-12286': 14.64}

**S2 donor leader (mid-transfer)** — redis0 killed at +0.71 s

| donor | TXN_START | FLIPPING | TRANSFER | transfer finished | BACKPATCH | TXN_DONE |
|---|---|---|---|---|---|---|
| sg1 | -0.0 | 0.01 | 0.05 | — | — | 4.85 |
| sg1(re-ship) | 1.25 | 1.25 | 1.3 | 3.11 | 3.11 | 4.84 |
| sg2 | — | — | — | — | — | 6.66 |
| sg2(re-ship) | 2.58 | 3.11 | 3.16 | 4.34 | 4.34 | 6.66 |
| sg3 | — | — | — | — | — | 9.89 |
| sg3(re-ship) | 5.61 | 6.51 | 6.55 | 7.56 | 7.56 | 9.89 |

recipient: chain acks at [3.38, 4.61, 7.87]; range commits (`RECP_TXN_DONE`) {'0-1364': 4.84, '5461-6825': 6.65, '10922-12286': 9.88}

**S3 donor follower** — redis1 killed at +1.09 s

| donor | TXN_START | FLIPPING | TRANSFER | transfer finished | BACKPATCH | TXN_DONE |
|---|---|---|---|---|---|---|
| sg1 | -0.0 | 0.01 | 0.08 | 1.11 | 1.11 | 2.92 |
| sg2 | 1.12 | 1.13 | 1.16 | 2.62 | 2.62 | 4.74 |
| sg3 | 2.63 | 2.64 | 2.67 | 3.67 | 3.67 | 6.7 |

recipient: chain acks at [1.4, 2.88, 3.95]; range commits (`RECP_TXN_DONE`) {'0-1364': 2.92, '5461-6825': 4.74, '10922-12286': 6.7}

**S4 recipient follower** — redis4 killed at +0.82 s

| donor | TXN_START | FLIPPING | TRANSFER | transfer finished | BACKPATCH | TXN_DONE |
|---|---|---|---|---|---|---|
| sg1 | -0.01 | 0.01 | 0.08 | 1.11 | 1.11 | 6.19 |
| sg2 | 1.12 | 1.13 | 1.16 | 2.29 | 2.29 | 9.87 |
| sg3 | 2.3 | 2.31 | 2.34 | 3.35 | 3.35 | 7.99 |

recipient: chain acks at [4.44, 5.43, 6.44]; range commits (`RECP_TXN_DONE`) {'0-1364': 6.19, '10922-12286': 7.99, '5461-6825': 9.87}

**S5 donor leader after its transfer** — redis0 killed at +3.85 s

| donor | TXN_START | FLIPPING | TRANSFER | transfer finished | BACKPATCH | TXN_DONE |
|---|---|---|---|---|---|---|
| sg1 | -0.01 | 0.01 | 0.09 | 1.12 | 1.12 | 2.99 |
| sg2 | 1.12 | 1.13 | 1.17 | 2.31 | 2.31 | 4.84 |
| sg3 | 2.33 | 2.33 | 2.37 | 3.37 | 3.37 | 6.81 |

recipient: chain acks at [1.4, 2.58, 3.64]; range commits (`RECP_TXN_DONE`) {'0-1364': 2.98, '5461-6825': 4.84, '10922-12286': 6.8}

