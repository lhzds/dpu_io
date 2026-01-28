#include <argparse/argparse.hpp>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "bytes.h"
#include "comm/frontend.h"
#include "comm/frontend_qp.h"
#include "util.h"

using namespace pulsedb;

namespace {
constexpr size_t KiB = 1024;
constexpr size_t MiB = 1024 * KiB;

struct Options {
    std::string backend_name;
    std::string dev_pci;
    size_t qp_num{};
    size_t req_rb_cap{};
    size_t resp_rb_cap{};

    size_t threads{};
    size_t files_per_thread{};
    size_t wait_every_files{};
    size_t file_size{};
    size_t write_size{};
    size_t range_sync_size{};
    bool wait_before{};
    bool use_fsync{};
    size_t start_file_number{};

    size_t file_size_base{};
    int base_size_multiplier{};

    size_t stats_interval_ms{};
};

struct AtomicStats {
    std::atomic<uint64_t> write_bytes{};
    std::atomic<uint64_t> write_ops{};
    std::atomic<uint64_t> range_sync_ops{};
    std::atomic<uint64_t> sync_close_ops{};
    std::atomic<uint64_t> wait_all_ns_total{};
    std::atomic<uint64_t> wait_all_cnt{};
    std::atomic<uint64_t> wait_all_ns_max{};
};

Options read_args(int argc, char **argv) {
    argparse::ArgumentParser arg_parser("bench_compaction_host");

    arg_parser.add_argument("-n")
        .help("backend name")
        .required();
    arg_parser.add_argument("-r")
        .help("host dev pci")
        .required();
    arg_parser.add_argument("-q")
        .help("qp num (default = threads)")
        .scan<'d', size_t>()
        .default_value(0);
    arg_parser.add_argument("-x")
        .help("host req rb cap")
        .scan<'d', size_t>()
        .default_value(64 * MiB);
    arg_parser.add_argument("-y")
        .help("host resp rb cap")
        .scan<'d', size_t>()
        .default_value(1 * MiB);

    arg_parser.add_argument("-t")
        .help("threads")
        .scan<'d', size_t>()
        .default_value(1);
    arg_parser.add_argument("-f")
        .help("files per thread")
        .scan<'d', size_t>()
        .default_value(1);
    arg_parser.add_argument("--wait_every_files")
        .help("wait_all every N files per thread (0 means only at end)")
        .scan<'d', size_t>()
        .default_value(0);
    arg_parser.add_argument("-s")
        .help("file size (bytes)")
        .scan<'d', size_t>()
        .default_value(256 * MiB);
    arg_parser.add_argument("-w")
        .help("write size (bytes)")
        .scan<'d', size_t>()
        .default_value(2 * MiB);
    arg_parser.add_argument("-g")
        .help("range sync size (bytes, 0 disable)")
        .scan<'d', size_t>()
        .default_value(32 * MiB);
    arg_parser.add_argument("--wait_before")
        .help("sync_file_range wait_before")
        .default_value(false)
        .implicit_value(true);
    arg_parser.add_argument("--use_fsync")
        .help("fsync instead of fdatasync")
        .default_value(false)
        .implicit_value(true);
    arg_parser.add_argument("--start_file_number")
        .help("start file number")
        .scan<'d', size_t>()
        .default_value(1);

    arg_parser.add_argument("-b")
        .help("file size base (bytes)")
        .scan<'d', size_t>()
        .default_value(64 * MiB);
    arg_parser.add_argument("-m")
        .help("base size multiplier (0 = auto by file size base)")
        .scan<'d', int>()
        .default_value(0);

    arg_parser.add_argument("-i")
        .help("stats interval ms (0 disable)")
        .scan<'d', size_t>()
        .default_value(0);

    arg_parser.parse_args(argc, argv);

    Options opts;
    opts.backend_name = arg_parser.get<std::string>("-n");
    opts.dev_pci = arg_parser.get<std::string>("-r");
    opts.qp_num = arg_parser.get<size_t>("-q");
    opts.req_rb_cap = arg_parser.get<size_t>("-x");
    opts.resp_rb_cap = arg_parser.get<size_t>("-y");

    opts.threads = arg_parser.get<size_t>("-t");
    opts.files_per_thread = arg_parser.get<size_t>("-f");
    opts.wait_every_files = arg_parser.get<size_t>("--wait_every_files");
    opts.file_size = arg_parser.get<size_t>("-s");
    opts.write_size = arg_parser.get<size_t>("-w");
    opts.range_sync_size = arg_parser.get<size_t>("-g");
    opts.wait_before = arg_parser.get<bool>("--wait_before");
    opts.use_fsync = arg_parser.get<bool>("--use_fsync");
    opts.start_file_number = arg_parser.get<size_t>("--start_file_number");

    opts.file_size_base = arg_parser.get<size_t>("-b");
    opts.base_size_multiplier = arg_parser.get<int>("-m");

    opts.stats_interval_ms = arg_parser.get<size_t>("-i");

    if (opts.qp_num == 0) {
        opts.qp_num = opts.threads;
    }
    if (opts.write_size == 0 || opts.file_size == 0) {
        throw_with("write_size and file_size must be > 0");
    }
    if (opts.write_size > opts.req_rb_cap) {
        throw_with("write_size must be <= req_rb_cap");
    }
    if (opts.base_size_multiplier <= 0) {
        if (opts.file_size_base == 0) {
            throw_with("file_size_base must be > 0 for auto multiplier");
        }
        opts.base_size_multiplier = static_cast<int>(
            (opts.file_size + opts.file_size_base - 1) / opts.file_size_base
        );
        if (opts.base_size_multiplier <= 0) {
            opts.base_size_multiplier = 1;
        }
    }
    return opts;
}

void run_worker(size_t thread_id, Frontend &frontend, const Options &opts, AtomicStats &stats) {
    auto *qp = frontend.alloc_qp();

    std::string payload(opts.write_size, static_cast<char>('a' + (thread_id % 26)));
    size_t file_base = opts.start_file_number + thread_id * opts.files_per_thread;
    size_t inflight_files = 0;

    for (size_t f = 0; f < opts.files_per_thread; ++f) {
        size_t file_number = file_base + f;
        size_t offset = 0;
        size_t last_sync_offset = 0;
        while (offset < opts.file_size) {
            size_t cur_size = std::min(opts.write_size, opts.file_size - offset);
            qp->prep_write(file_number, offset, opts.base_size_multiplier);
            qp->append({payload.data(), cur_size});
            qp->post_write();
            stats.write_bytes.fetch_add(cur_size, std::memory_order_relaxed);
            stats.write_ops.fetch_add(1, std::memory_order_relaxed);

            offset += cur_size;
            if (opts.range_sync_size > 0 &&
                (offset - last_sync_offset) >= opts.range_sync_size) {
                size_t sync_size = offset - last_sync_offset;
                qp->post_range_sync(file_number, last_sync_offset, sync_size, opts.wait_before);
                stats.range_sync_ops.fetch_add(1, std::memory_order_relaxed);
                last_sync_offset = offset;
            }
        }
        qp->post_sync_close(file_number, opts.use_fsync);
        stats.sync_close_ops.fetch_add(1, std::memory_order_relaxed);

        ++inflight_files;
        if (opts.wait_every_files > 0 && inflight_files >= opts.wait_every_files) {
            auto start_ts = std::chrono::steady_clock::now();
            qp->wait_all();
            auto end_ts = std::chrono::steady_clock::now();
            auto wait_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_ts - start_ts).count();
            stats.wait_all_ns_total.fetch_add(static_cast<uint64_t>(wait_ns), std::memory_order_relaxed);
            stats.wait_all_cnt.fetch_add(1, std::memory_order_relaxed);
            auto prev_max = stats.wait_all_ns_max.load(std::memory_order_relaxed);
            while (wait_ns > static_cast<int64_t>(prev_max) &&
                   !stats.wait_all_ns_max.compare_exchange_weak(
                       prev_max, static_cast<uint64_t>(wait_ns), std::memory_order_relaxed)) {
            }
            inflight_files = 0;
        }
    }

    if (inflight_files > 0) {
        auto start_ts = std::chrono::steady_clock::now();
        qp->wait_all();
        auto end_ts = std::chrono::steady_clock::now();
        auto wait_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_ts - start_ts).count();
        stats.wait_all_ns_total.fetch_add(static_cast<uint64_t>(wait_ns), std::memory_order_relaxed);
        stats.wait_all_cnt.fetch_add(1, std::memory_order_relaxed);
        auto prev_max = stats.wait_all_ns_max.load(std::memory_order_relaxed);
        while (wait_ns > static_cast<int64_t>(prev_max) &&
               !stats.wait_all_ns_max.compare_exchange_weak(
                   prev_max, static_cast<uint64_t>(wait_ns), std::memory_order_relaxed)) {
        }
    }
    frontend.free_qp(qp);
}
} // namespace

int main(int argc, char **argv) {
    auto opts = read_args(argc, argv);
    std::cout << "backend: " << opts.backend_name << "\n"
              << "dev_pci: " << opts.dev_pci << "\n"
              << "threads: " << opts.threads << "\n"
              << "qp_num: " << opts.qp_num << "\n"
              << "files/thread: " << opts.files_per_thread << "\n"
              << "wait_every_files: " << opts.wait_every_files << "\n"
              << "file_size: " << opts.file_size << "\n"
              << "write_size: " << opts.write_size << "\n"
              << "range_sync_size: " << opts.range_sync_size << "\n"
              << "base_size_multiplier: " << opts.base_size_multiplier << "\n"
              << "wait_before: " << opts.wait_before << "\n"
              << "use_fsync: " << opts.use_fsync << std::endl;

    auto frontend = std::make_unique<Frontend>(
        opts.backend_name, opts.dev_pci, static_cast<int>(opts.qp_num),
        opts.req_rb_cap, opts.resp_rb_cap
    );

    AtomicStats stats;
    std::atomic<bool> stop_stats{};
    std::thread stats_thread;
    if (opts.stats_interval_ms > 0) {
        auto prev_ts = std::chrono::steady_clock::now();
        uint64_t prev_write_bytes = 0;
        uint64_t prev_write_ops = 0;
        uint64_t prev_range_sync_ops = 0;
        uint64_t prev_sync_close_ops = 0;
        uint64_t prev_wait_all_ns = 0;
        uint64_t prev_wait_all_cnt = 0;
        stats_thread = std::thread([&] {
            while (!stop_stats.load(std::memory_order_relaxed)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(opts.stats_interval_ms));
                auto now_ts = std::chrono::steady_clock::now();
                std::chrono::duration<double> secs = now_ts - prev_ts;
                if (secs.count() <= 0.0) {
                    prev_ts = now_ts;
                    continue;
                }
                uint64_t cur_write_bytes = stats.write_bytes.load(std::memory_order_relaxed);
                uint64_t cur_write_ops = stats.write_ops.load(std::memory_order_relaxed);
                uint64_t cur_range_sync_ops = stats.range_sync_ops.load(std::memory_order_relaxed);
                uint64_t cur_sync_close_ops = stats.sync_close_ops.load(std::memory_order_relaxed);
                uint64_t cur_wait_all_ns = stats.wait_all_ns_total.load(std::memory_order_relaxed);
                uint64_t cur_wait_all_cnt = stats.wait_all_cnt.load(std::memory_order_relaxed);
                uint64_t cur_wait_all_ns_max = stats.wait_all_ns_max.load(std::memory_order_relaxed);
                uint64_t delta_bytes = cur_write_bytes - prev_write_bytes;
                uint64_t delta_write_ops = cur_write_ops - prev_write_ops;
                uint64_t delta_range_sync_ops = cur_range_sync_ops - prev_range_sync_ops;
                uint64_t delta_sync_close_ops = cur_sync_close_ops - prev_sync_close_ops;
                uint64_t delta_wait_all_ns = cur_wait_all_ns - prev_wait_all_ns;
                uint64_t delta_wait_all_cnt = cur_wait_all_cnt - prev_wait_all_cnt;

                double mbps = delta_bytes / (1024.0 * 1024.0) / secs.count();
                double wait_all_avg_ms = 0.0;
                if (delta_wait_all_cnt > 0) {
                    wait_all_avg_ms = (delta_wait_all_ns / 1e6) / delta_wait_all_cnt;
                }
                std::cout << "[Host-Perf] " << mbps << " MiB/s, "
                          << (delta_write_ops / secs.count()) << " write_ops/s, "
                          << (delta_range_sync_ops / secs.count()) << " range_sync_ops/s, "
                          << (delta_sync_close_ops / secs.count()) << " sync_close_ops/s, "
                          << "wait_all_avg_ms " << wait_all_avg_ms << ", "
                          << "wait_all_max_ms " << (cur_wait_all_ns_max / 1e6)
                          << std::endl;

                prev_ts = now_ts;
                prev_write_bytes = cur_write_bytes;
                prev_write_ops = cur_write_ops;
                prev_range_sync_ops = cur_range_sync_ops;
                prev_sync_close_ops = cur_sync_close_ops;
                prev_wait_all_ns = cur_wait_all_ns;
                prev_wait_all_cnt = cur_wait_all_cnt;
            }
        });
    }

    auto start = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    workers.reserve(opts.threads);
    for (size_t i = 0; i < opts.threads; ++i) {
        workers.emplace_back([&, i] { run_worker(i, *frontend, opts, stats); });
    }
    for (auto &t : workers) {
        t.join();
    }
    auto end = std::chrono::steady_clock::now();
    stop_stats.store(true, std::memory_order_relaxed);
    if (stats_thread.joinable()) {
        stats_thread.join();
    }

    std::chrono::duration<double> secs = end - start;
    uint64_t total_bytes = stats.write_bytes.load(std::memory_order_relaxed);
    double mbps = total_bytes / (1024.0 * 1024.0) / secs.count();
    std::cout << "total_bytes: " << total_bytes << "\n"
              << "elapsed_s: " << secs.count() << "\n"
              << "throughput_mib_s: " << mbps << std::endl;
    return 0;
}
