# Fio Bench Config

This directory contains the fio benchmark config for the current test setup.
The config file is JSON, which is translated into a fio job file at runtime.

## Top-level parameters

- `fio_bin`: Path to fio binary.
- `mode`: Selects `file` or `device` mode explicitly.
- `run_id`: Run identifier for output names (`auto` for next id).
  Output names become `{job_name}_run{run_id}_t{threads}.*`.
- `threads`: List of thread counts to test (`numjobs` in fio).
- `queue_depth`: io_uring queue depth (`iodepth`).
- `write_size`: I/O block size (`bs`).
- `rw`: I/O pattern (e.g. `write`, `read`, `randwrite`).
- `ioengine`: I/O engine (e.g. `io_uring`).
- `direct`: Enable direct I/O (`direct=1`).
- `group_reporting`: Enable fio group reporting.
- `time_based`: Run for a fixed time instead of fixed size.
- `runtime_sec`: Runtime in seconds when `time_based` is true.
- `output_dir`: Output directory for job files and logs.
- `output_format`: fio output format (e.g. `normal`, `json`).
- `job_name`: fio job name prefix.
- `extra_args`: Extra fio CLI arguments.

## File mode (`file`)

Used when `mode` is `file`.

- `directory`: Target directory for test files.
- `files_per_thread`: Number of files per thread (`nrfiles`).
- `file_size`: Size of each file (`filesize`).
- `sync_close`: Enable `fsync_on_close`.
- `fsync`: Enable `fsync` after every N writes (`fsync=1`).
- `fdatasync`: Enable `fdatasync` after every N writes (`fdatasync=1`).
- `sync_file_range`: String passed to fio `sync_file_range`.
  Example: `wait_before,write:4` (sync every 4 writes).

## Device mode (`device`)

Used when `mode` is `device`.

- `filename`: Target block device (e.g. `/dev/nvme0n1`).
- `size`: Per-job I/O size (`size`).
- `offset_increment`: Per-job offset increment for partitioning.

## io_uring options (`uring`)

Only applied when `ioengine` is `io_uring` or `io_uring_cmd`.

- `fixedbufs`: Pre-register buffers (`fixedbufs=1`).
- `registerfiles`: Pre-register files (`registerfiles=1`).
- `sqthread_poll`: Enable SQ polling thread (`sqthread_poll=1`).
- `sqthread_poll_cpu`: CPU core for SQ polling thread.
- `hipri`: Enable polled completions (`hipri=1`).
