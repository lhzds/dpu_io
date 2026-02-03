import json
import os
import re
import subprocess
from pathlib import Path


def load_config():
    config_path = Path(__file__).with_name("config.json")
    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def expand_path(path):
    return os.path.expanduser(path)


def resolve_path(path):
    expanded = expand_path(path)
    if os.path.isabs(expanded):
        return expanded
    return str(Path(__file__).parent / expanded)


def next_run_id(output_dir, job_name):
    if not output_dir.exists():
        return 1
    pattern = re.compile(rf"^{re.escape(job_name)}_run(\d+)_t\d+\.log$")
    max_id = 0
    for entry in output_dir.iterdir():
        if not entry.is_file():
            continue
        match = pattern.match(entry.name)
        if match:
            max_id = max(max_id, int(match.group(1)))
    return max_id + 1 if max_id > 0 else 1


def parse_run_id(value, output_dir, job_name):
    if value is None:
        return None
    if isinstance(value, int):
        if value <= 0:
            raise ValueError("run_id must be > 0")
        return value
    if isinstance(value, str):
        if value.lower() == "auto":
            return next_run_id(output_dir, job_name)
        if value.isdigit():
            return int(value)
    raise ValueError("run_id must be an integer, 'auto', or null")


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
output_dir = resolve_path(cfg.get("output_dir", "./output"))
os.makedirs(output_dir, exist_ok=True)
output_dir_path = Path(output_dir)
job_name = cfg.get("job_name", "uring")
run_id = parse_run_id(cfg.get("run_id"), output_dir_path, job_name)
name_prefix = f"{job_name}_run{run_id}" if run_id is not None else job_name
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
    "--progress_interval_ms", str(cfg.get("progress_interval_ms", 0)),
    "--pacing_mbps", str(cfg.get("pacing_mbps", 0)),
    "--delete_batch_size", str(cfg["delete_batch_size"]),
]

if cfg.get("wait_before", False):
    base_cmd.append("--wait_before")
if cfg.get("use_fsync", False):
    base_cmd.append("--use_fsync")
if cfg.get("direct", False):
    base_cmd.append("--direct")
if cfg.get("create_ssts", False):
    base_cmd.append("--create_ssts")
if cfg.get("delete_after_sync", True):
    base_cmd.append("--delete_after_sync")

extra_args = cfg.get("extra_args", [])
if extra_args:
    base_cmd.extend(extra_args)

for threads in thread_values:
    cmd = base_cmd + ["-t", str(threads)]
    output_path = output_dir_path / f"{name_prefix}_t{threads}.log"
    with output_path.open("w", encoding="utf-8") as log_file:
        subprocess.run(cmd, check=True, stdout=log_file)
