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


def add_optional_kv(lines, key, value):
    if value is None:
        return
    lines.append(f"{key}={value}")


def get_section(config, key):
    section = config.get(key, {})
    if section is None:
        return {}
    if not isinstance(section, dict):
        raise ValueError(f"{key} must be a dict")
    return section


def next_run_id(output_dir, job_name):
    if not output_dir.exists():
        return 1
    pattern = re.compile(rf"^{re.escape(job_name)}_run(\d+)_t\d+\.(?:log|fio)$")
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
fio_bin = expand_path(cfg.get("fio_bin", "fio"))
threads_list = get_thread_values(cfg)

file_cfg = get_section(cfg, "file")
device_cfg = get_section(cfg, "device")
uring_cfg = get_section(cfg, "uring")

mode = cfg.get("mode")
if mode not in ("file", "device"):
    raise ValueError("mode must be 'file' or 'device'")
use_device = mode == "device"

if use_device:
    filename = device_cfg.get("filename")
    if not filename:
        raise ValueError("device.filename is required when mode is 'device'")
    filename = resolve_path(filename)
    if not os.path.exists(filename):
        raise ValueError(f"filename does not exist: {filename}")
    size_override = device_cfg.get("size")
    if size_override is None:
        raise ValueError("device.size is required when mode is 'device'")
    size_per_job = int(size_override)
else:
    directory = file_cfg.get("directory")
    if not directory:
        raise ValueError("file.directory is required when mode is 'file'")
    directory = resolve_path(directory)
    if not os.path.isdir(directory):
        raise ValueError(f"directory does not exist: {directory}")

    files_per_thread = file_cfg.get("files_per_thread")
    file_size = file_cfg.get("file_size")
    if files_per_thread is None or file_size is None:
        raise ValueError("file.files_per_thread and file.file_size are required when mode is 'file'")
    files_per_thread = int(files_per_thread)
    file_size = int(file_size)
    size_per_job = files_per_thread * file_size

write_size = int(cfg["write_size"])
queue_depth = int(cfg["queue_depth"])

rw = cfg.get("rw", "write")
ioengine = cfg.get("ioengine", "io_uring")
use_uring = ioengine in ("io_uring", "io_uring_cmd") or ioengine.startswith("io_uring")
direct = bool(cfg.get("direct", True))
group_reporting = bool(cfg.get("group_reporting", True))
time_based = bool(cfg.get("time_based", False))
runtime_sec = cfg.get("runtime_sec")
sync_close = bool(file_cfg.get("sync_close", False)) if not use_device else False
fsync = bool(file_cfg.get("fsync", False)) if not use_device else False
fdatasync = bool(file_cfg.get("fdatasync", False)) if not use_device else False
sync_file_range = file_cfg.get("sync_file_range") if not use_device else None
fixedbufs = bool(uring_cfg.get("fixedbufs", False)) if use_uring else False
registerfiles = bool(uring_cfg.get("registerfiles", False)) if use_uring else False
sqthread_poll = bool(uring_cfg.get("sqthread_poll", False)) if use_uring else False
sqthread_poll_cpu = uring_cfg.get("sqthread_poll_cpu") if use_uring else None
hipri = bool(uring_cfg.get("hipri", False)) if use_uring else False

output_dir = cfg.get("output_dir")
if output_dir:
    output_dir = resolve_path(output_dir)
    os.makedirs(output_dir, exist_ok=True)
else:
    output_dir = str(Path(__file__).parent)
output_dir_path = Path(output_dir)

job_name = cfg.get("job_name", "fio_write")
extra_args = cfg.get("extra_args", [])
output_format = cfg.get("output_format")
run_id = parse_run_id(cfg.get("run_id"), output_dir_path, job_name)
name_prefix = f"{job_name}_run{run_id}" if run_id is not None else job_name

global_lines = [
    "[global]",
    f"ioengine={ioengine}",
    f"direct={1 if direct else 0}",
    f"rw={rw}",
    f"bs={write_size}",
    f"iodepth={queue_depth}",
    f"thread=1",
    f"group_reporting={1 if group_reporting else 0}",
    f"size={size_per_job}",
]

if use_device:
    global_lines.append(f"filename={filename}")
    offset_increment = device_cfg.get("offset_increment")
    if offset_increment is None:
        offset_increment = size_per_job
    if offset_increment:
        global_lines.append(f"offset_increment={int(offset_increment)}")
else:
    global_lines.extend([
        f"directory={directory}",
        f"nrfiles={files_per_thread}",
        f"filesize={file_size}",
    ])

if sync_close:
    global_lines.append("fsync_on_close=1")
if fsync:
    global_lines.append("fsync=1")
if fdatasync:
    global_lines.append("fdatasync=1")
if sync_file_range:
    global_lines.append(f"sync_file_range={sync_file_range}")
if fixedbufs:
    global_lines.append("fixedbufs=1")
if registerfiles:
    global_lines.append("registerfiles=1")
if sqthread_poll:
    global_lines.append("sqthread_poll=1")
if sqthread_poll_cpu is not None:
    global_lines.append(f"sqthread_poll_cpu={int(sqthread_poll_cpu)}")
if hipri:
    global_lines.append("hipri=1")
if time_based:
    global_lines.append("time_based=1")
    add_optional_kv(global_lines, "runtime", runtime_sec)


for threads in threads_list:
    job_lines = list(global_lines)
    job_lines.extend(["", f"[{job_name}_t{threads}]", f"numjobs={threads}"])
    job_path = output_dir_path / f"{name_prefix}_t{threads}.fio"
    job_path.write_text("\n".join(job_lines) + "\n", encoding="utf-8")

    cmd = [fio_bin, str(job_path)]
    if output_format:
        cmd.extend(["--output-format", output_format])
    if extra_args:
        cmd.extend(extra_args)

    output_path = output_dir_path / f"{name_prefix}_t{threads}.log"
    if output_format:
        cmd.extend(["--output", str(output_path)])
    subprocess.run(cmd, check=True)
