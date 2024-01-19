#!/bin/bash
# RDMA stack modules
sudo modprobe rdma_cm
sudo modprobe ib_uverbs
sudo modprobe rdma_ucm
sudo modprobe ib_umad
sudo modprobe ib_ipoib
# RDMA devices low-level drivers
sudo modprobe mlx4_ib
sudo modprobe mlx4_en
