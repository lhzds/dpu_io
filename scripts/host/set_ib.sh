#!/bin/bash

sudo ip link set ibs21f1 up
sudo ip addr add 192.168.1.20/24 dev ibs21f1
ip link show 
