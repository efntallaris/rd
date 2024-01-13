#!/bin/bash
modprobe -v qedr
modprobe rdma_cm
modprobe ib_uverbs
modprobe rdma_ucm
modprobe ib_umad
