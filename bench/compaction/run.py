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

cmd = [
    host_exe,
    "-n", cfg["backend_name"],
    "-r", cfg["dev_pci"],
    "-q", str(cfg["qp_num"]),
    "-x", str(cfg["req_rb_cap"]),
    "-y", str(cfg["resp_rb_cap"]),
    "-t", str(cfg["threads"]),
    "-f", str(cfg["files_per_thread"]),
    "--wait_every_files", str(cfg["wait_every_files"]),
    "-s", str(cfg["file_size"]),
    "-w", str(cfg["write_size"]),
    "-g", str(cfg["range_sync_size"]),
    "--start_file_number", str(cfg["start_file_number"]),
    "-b", str(cfg["file_size_base"]),
    "-m", str(cfg["base_size_multiplier"]),
    "-i", str(cfg["stats_interval_ms"]),
]

if cfg.get("wait_before", False):
    cmd.append("--wait_before")
if cfg.get("use_fsync", False):
    cmd.append("--use_fsync")

extra_args = cfg.get("extra_args", [])
if extra_args:
    cmd.extend(extra_args)

subprocess.run(cmd, check=True)
