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
    "--sst_dir", expand_path(cfg["sst_dir"]),
    "--base_pool_size", str(cfg["base_pool_size"]),
    "--big_pool_cnt", str(cfg["big_pool_cnt"]),
    "--big_pool_size", str(cfg["big_pool_size"]),
    "--file_size_base", str(cfg["file_size_base"]),
    "--sst_pad_len", str(cfg["sst_pad_len"]),
    "--queue_depth", str(cfg["queue_depth"]),
    "-f", str(cfg["files_per_thread"]),
    "-s", str(cfg["file_size"]),
    "-w", str(cfg["write_size"]),
    "-g", str(cfg["range_sync_size"]),
    "--start_file_number", str(cfg["start_file_number"]),
    "--stats_interval_ms", str(cfg["stats_interval_ms"]),
    "--delete_batch_size", str(cfg["delete_batch_size"]),
]

if cfg.get("wait_before", False):
    base_cmd.append("--wait_before")
if cfg.get("use_fsync", False):
    base_cmd.append("--use_fsync")
if cfg.get("create_ssts", False):
    base_cmd.append("--create_ssts")
if cfg.get("delete_after_sync", True):
    base_cmd.append("--delete_after_sync")

extra_args = cfg.get("extra_args", [])
if extra_args:
    base_cmd.extend(extra_args)

for threads in thread_values:
    cmd = base_cmd + ["-t", str(threads)]
    subprocess.run(cmd, check=True)
