import os
import subprocess
from time import sleep
from tqdm import tqdm

host_exe = os.path.expanduser("~/pulseDB/pulsedb/build/linux/x86_64/release/dma_with_io_host")
dpu_exe = os.path.expanduser("~/pulsedb/build/linux/arm64/release/dma_with_io_dpu")

output_dir = os.path.expanduser("~/dma_with_io")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

dpu_username = "olh"
dpu_ip = "192.168.100.2"
repeat_time = 10000

# msg size
kb = 1024
msizes = []
t = 1 * kb
while t <= 128 * kb:
    msizes.append(t)
    t *= 2

print(msizes)
with tqdm(total=len(msizes), desc="dma with io exp") as pbar:
    for msize in msizes:
        dpu_command = ' '.join([
            dpu_exe,
            "-b", str(msize),
            "-r", str(repeat_time),
            "-o", output_dir
        ])
        ssh_command = [
            "ssh", "-o", "StrictHostKeyChecking=no", f"{dpu_username}@{dpu_ip}", f"{dpu_command}"
        ]
        host_command = [
            host_exe
        ]
        
        p1 = subprocess.Popen(ssh_command)
        sleep(2)
        # p2 = subprocess.Popen(host_command)
        p2 = subprocess.Popen(host_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        p1.wait()
        # p2.wait()
        pbar.update(1)
