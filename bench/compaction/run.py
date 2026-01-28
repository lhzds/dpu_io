import json
import os
import subprocess
from pathlib import Path


def load_config():
    config_path = Path(__file__).with_name("config.json")
    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def expand_path(path):
    return os.path.expanduser(path)


cfg = load_config()
host_exe = expand_path(cfg["host_exe"])


def get_thread_values(config):
    if "threads" not in config:
        raise ValueError("threads must be provided as a list")
    thread_values = config["threads"]
    if not isinstance(thread_values, list):
        raise ValueError("threads must be a list of integers")
    if not thread_values:
        raise ValueError("threads list must not be empty")
    for value in thread_values:
        if not isinstance(value, int) or value <= 0:
            raise ValueError(f"invalid threads value: {value}")
    return thread_values


thread_values = get_thread_values(cfg)
base_cmd = [
    host_exe,
    "-n", cfg["backend_name"],
    "-r", cfg["dev_pci"],
    "-q", str(cfg["qp_num"]),
    "-x", str(cfg["req_rb_cap"]),
    "-y", str(cfg["resp_rb_cap"]),
    "--slot_pool_size", str(cfg["slot_pool_size"]),
    "-f", str(cfg["files_per_thread"]),
    "--wait_every_files", str(cfg["wait_every_files"]),
    "-s", str(cfg["file_size"]),
    "-w", str(cfg["write_size"]),
    "-g", str(cfg["range_sync_size"]),
    "--start_file_number", str(cfg["start_file_number"]),
    "-b", str(cfg["file_size_base"]),
    "-i", str(cfg["stats_interval_ms"]),
]

if cfg.get("wait_before", False):
    base_cmd.append("--wait_before")
if cfg.get("use_fsync", False):
    base_cmd.append("--use_fsync")

extra_args = cfg.get("extra_args", [])
if extra_args:
    base_cmd.extend(extra_args)

for threads in thread_values:
    cmd = base_cmd + ["-t", str(threads)]
    subprocess.run(cmd, check=True)
