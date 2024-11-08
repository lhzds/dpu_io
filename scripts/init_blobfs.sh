SCRIPT_DIR=$(dirname "$(realpath "$0")")
sudo $SCRIPT_DIR/../deps/spdk/test/blobfs/mkfs/mkfs $SCRIPT_DIR/../deps/spdk/json_configs/nvme.json Nvme0n1

# sudo gdb --args /home/olh/repos/pulseDB/pulsedb/build/linux/x86_64/debug/blobfs_async  -b Nvme0n1 -j /home/olh/repos/pulseDB/pulsedb/deps/spdk/json_configs/nvme.json
sudo /home/olh/repos/pulseDB/pulsedb/build/linux/x86_64/debug/blobfs_async  -b Nvme0n1 -j /home/olh/repos/pulseDB/pulsedb/deps/spdk/json_configs/nvme.json
