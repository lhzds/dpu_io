#!/bin/bash

sudo systemctl enable opensmd
sudo systemctl restart opensmd

sudo ip link set ib1 up
sudo ip addr add 192.168.1.21/24 dev ib1
ip link show 
