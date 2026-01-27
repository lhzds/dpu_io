import json
import os
import subprocess
from pathlib import Path
from time import sleep

from tqdm import tqdm


def load_config():
    config_path = Path(__file__).with_name("config.json")
    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def expand_path(path):
    return os.path.expanduser(path)


cfg = load_config()
host_exe = expand_path(cfg["host_exe"])
dpu_exe = expand_path(cfg["dpu_exe"])
output_dir = expand_path(cfg["output_dir"])
dpu_username = cfg["dpu_username"]
dpu_ip = cfg["dpu_ip"]
repeat_time = cfg["repeat_time"]
start_sleep_sec = cfg.get("start_sleep_sec", 2)
msizes = cfg["msg_sizes"]

host_args = cfg["host_args"]
dpu_args = cfg["dpu_args"]

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

with tqdm(total=len(msizes), desc="comch_lat exp") as pbar:
    for msize in msizes:
        dpu_command = [
            dpu_exe,
            "-b", str(msize),
            "-r", str(repeat_time),
            "-o", output_dir,
            "--server_name", dpu_args["server_name"],
            "--dev_pci", dpu_args["dev_pci"],
            "--dev_rep_pci", dpu_args["dev_rep_pci"],
            "--dma_buf_max_size", str(dpu_args["dma_buf_max_size"]),
        ]
        ssh_command = [
            "ssh", "-o", "StrictHostKeyChecking=no",
            f"{dpu_username}@{dpu_ip}", " ".join(dpu_command)
        ]
        host_command = [
            host_exe,
            "--server_name", host_args["server_name"],
            "--dev_pci", host_args["dev_pci"],
            "--dma_buf_max_size", str(host_args["dma_buf_max_size"]),
        ]

        p1 = subprocess.Popen(ssh_command)
        sleep(start_sleep_sec)
        subprocess.Popen(host_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        p1.wait()
        pbar.update(1)
