import json
import os
import shlex
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


def resolve_path(path):
    expanded = expand_path(path)
    p = Path(expanded)
    if not p.is_absolute():
        p = Path(__file__).parent / p
    return str(p.resolve())


def infer_project_dir_from_exe(exe_path):
    if not exe_path:
        return None
    expanded = expand_path(exe_path)
    marker = "/build/"
    idx = expanded.find(marker)
    if idx == -1:
        return None
    return expanded[:idx]


def parse_build_mode(config, key="build_mode", default="release"):
    value = config.get(key, default)
    if not isinstance(value, str):
        raise ValueError(f"{key} must be 'release' or 'debug'")
    mode = value.lower()
    if mode not in ("release", "debug"):
        raise ValueError(f"{key} must be 'release' or 'debug'")
    return mode


cfg = load_config()
host_project_dir = resolve_path(cfg.get("host_project_dir", "../.."))
host_target = cfg.get("host_target", "bench_comch_lat_host")
dpu_target = cfg.get("dpu_target", "bench_comch_lat_dpu")
dpu_project_dir = cfg.get("dpu_project_dir")
if dpu_project_dir is None:
    dpu_project_dir = infer_project_dir_from_exe(cfg.get("dpu_exe"))
if dpu_project_dir is None:
    raise ValueError("dpu_project_dir is required (or provide legacy dpu_exe)")
dpu_project_dir = expand_path(dpu_project_dir)
output_dir = expand_path(cfg["output_dir"])
dpu_username = cfg["dpu_username"]
dpu_ip = cfg["dpu_ip"]
repeat_time = cfg["repeat_time"]
start_sleep_sec = cfg.get("start_sleep_sec", 2)
msizes = cfg["msg_sizes"]

host_args = cfg["host_args"]
dpu_args = cfg["dpu_args"]
default_build_mode = parse_build_mode(cfg)
host_build_mode = parse_build_mode(cfg, "host_build_mode", default_build_mode)
dpu_build_mode = parse_build_mode(cfg, "dpu_build_mode", default_build_mode)


if not os.path.exists(output_dir):
    os.makedirs(output_dir)

subprocess.run(["xmake", "f", "-P", host_project_dir, "-m", host_build_mode], check=True)
ssh_config_command = [
    "ssh", "-o", "StrictHostKeyChecking=no",
    f"{dpu_username}@{dpu_ip}",
    shlex.join(["xmake", "f", "-P", dpu_project_dir, "-m", dpu_build_mode])
]
subprocess.run(ssh_config_command, check=True)

with tqdm(total=len(msizes), desc="comch_lat exp") as pbar:
    for msize in msizes:
        dpu_command = [
            "xmake",
            "run",
            "-P", dpu_project_dir,
            dpu_target,
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
            f"{dpu_username}@{dpu_ip}", shlex.join(dpu_command)
        ]
        host_command = [
            "xmake",
            "run",
            "-P", host_project_dir,
            host_target,
            "--server_name", host_args["server_name"],
            "--dev_pci", host_args["dev_pci"],
            "--dma_buf_max_size", str(host_args["dma_buf_max_size"]),
        ]

        p1 = subprocess.Popen(ssh_command)
        sleep(start_sleep_sec)
        subprocess.Popen(host_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        p1.wait()
        pbar.update(1)
