#include "comm/backend.h"
#include "io/sst_pool.h"
#include "io/uring_engine.h"

#include <argparse/argparse.hpp>
#include <atomic>
#include <chrono>
#include <fmt/format.h>
#include <thread>

using namespace pulsedb;

std::string backend_name;
std::string dev_pci;
std::string dev_rep_pci;

size_t uring_queue_depth;
size_t dpu_dma_task_slot_num;
size_t dpu_req_rb_cap;
size_t dpu_resp_rb_cap;

std::string sst_pool_dir;
size_t base_pool_size;
size_t big_pool_cnt;
size_t big_pool_size;
size_t file_size_base;
size_t sst_pad_len;
size_t stats_interval_ms;

void read_args(int argc, char **argv) {
    argparse::ArgumentParser arg_parser;
    
    arg_parser.add_argument("-n")
        .help("backend name")
        .required();
    arg_parser.add_argument("-r")
        .help("dev pci")
        .required();
    arg_parser.add_argument("-R")
        .help("dev rep pci")
        .required();

    arg_parser.add_argument("-u")
        .help("uring queue depth")
        .required()
        .scan<'d', size_t>();
    arg_parser.add_argument("--dpu_dma_task_slot_num")
        .help("dpu dma task slot num")
        .scan<'d', size_t>()
        .default_value((size_t)256);
    arg_parser.add_argument("-x")
        .help("dpu req rb cap")
        .required()
        .scan<'d', size_t>();
    arg_parser.add_argument("-y")
        .help("dev resp rb cap")
        .required()
        .scan<'d', size_t>();   

    arg_parser.add_argument("-d")
        .help("sst pool dir")
        .required();
    arg_parser.add_argument("-s")
        .help("base pool size")
        .required()
        .scan<'d', size_t>();
    arg_parser.add_argument("-C")
        .help("big pool cnt")
        .scan<'d', size_t>()
        .default_value(0);
    arg_parser.add_argument("-S")
        .help("big pool size")
        .scan<'d', size_t>()
        .default_value(0);
    arg_parser.add_argument("-b")
        .help("file size base")
        .required()
        .scan<'d', size_t>();
    arg_parser.add_argument("-p")
        .help("sst padding len")
        .scan<'d', size_t>()
        .default_value(0);
    arg_parser.add_argument("-i")
        .help("stats interval ms (0 disable)")
        .scan<'d', size_t>()
        .default_value(0);

    arg_parser.parse_args(argc, argv);

    backend_name = arg_parser.get<std::string>("-n");
    dev_pci = arg_parser.get<std::string>("-r");
    dev_rep_pci = arg_parser.get<std::string>("-R");

    uring_queue_depth = arg_parser.get<size_t>("-u");
    dpu_dma_task_slot_num = arg_parser.get<size_t>("--dpu_dma_task_slot_num");
    dpu_req_rb_cap = arg_parser.get<size_t>("-x");
    dpu_resp_rb_cap = arg_parser.get<size_t>("-y");

    sst_pool_dir = arg_parser.get<std::string>("-d");
    base_pool_size = arg_parser.get<size_t>("-s");
    big_pool_cnt = arg_parser.get<size_t>("-C");
    big_pool_size = arg_parser.get<size_t>("-S");
    file_size_base = arg_parser.get<size_t>("-b");
    sst_pad_len = arg_parser.get<size_t>("-p");
    stats_interval_ms = arg_parser.get<size_t>("-i");

    std::cout << fmt::format(
        "backend name: {}\n"
        "dev pci: {}\n"
        "dev rep pci: {}\n"

        "uring queue depth: {}\n"
        "dpu dma task slot num: {}\n"
        "dpu req rb cap: {}\n"
        "dpu resp rb cap: {}\n"

        "sst pool dir: {}\n"
        "base pool size: {}\n"
        "big pool cnt: {}\n"
        "big pool size: {}\n"
        "file size base: {}\n"
        "sst padding len: {}\n"
        "stats interval ms: {}",
        backend_name, dev_pci, dev_rep_pci,
        uring_queue_depth, dpu_dma_task_slot_num, dpu_req_rb_cap, dpu_resp_rb_cap,
        sst_pool_dir, base_pool_size,
        big_pool_cnt, big_pool_size,
        file_size_base, sst_pad_len,
        stats_interval_ms
    ) << std::endl;
}

int main(int argc, char **argv) {
    read_args(argc, argv);

    auto sst_pool = std::make_unique<SstPool>(
        sst_pool_dir, base_pool_size,
        big_pool_cnt, big_pool_size,
        file_size_base, sst_pad_len
    );
    SPDLOG_INFO("[DPU] Dpu-io backend running...");
    auto uring_ctx = std::make_unique<UringEngine>(uring_queue_depth, std::move(sst_pool));
    auto *uring_ptr = uring_ctx.get();
    Backend backend(backend_name, dev_pci, dev_rep_pci,
        std::move(uring_ctx), dpu_dma_task_slot_num, dpu_req_rb_cap, dpu_resp_rb_cap);

    auto start_ts = std::chrono::steady_clock::now();
    auto start_dma = backend.dma()->stats_snapshot();
    auto start_uring = uring_ptr->stats_snapshot();

    std::atomic<bool> stop_stats{};
    std::thread stats_thread;
    if (stats_interval_ms > 0) {
        auto prev_dma = backend.dma()->stats_snapshot();
        auto prev_uring = uring_ptr->stats_snapshot();
        auto prev_ts = std::chrono::steady_clock::now();
        stats_thread = std::thread([&, prev_dma, prev_uring, prev_ts]() mutable {
            while (!stop_stats.load(std::memory_order_relaxed)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(stats_interval_ms));
                auto now_ts = std::chrono::steady_clock::now();
                auto cur_dma = backend.dma()->stats_snapshot();
                auto cur_uring = uring_ptr->stats_snapshot();
                std::chrono::duration<double> secs = now_ts - prev_ts;
                if (secs.count() <= 0.0) {
                    prev_ts = now_ts;
                    prev_dma = cur_dma;
                    prev_uring = cur_uring;
                    continue;
                }

                auto dma_bytes = cur_dma.total_bytes - prev_dma.total_bytes;
                auto dma_tasks = cur_dma.total_tasks - prev_dma.total_tasks;
                auto dma_latency_ns_total = cur_dma.memcpy_ns_total - prev_dma.memcpy_ns_total;
                auto dma_latency_cnt = cur_dma.memcpy_cnt - prev_dma.memcpy_cnt;
                double dma_mbps = dma_bytes / (1024.0 * 1024.0) / secs.count();
                double dma_ops = dma_tasks / secs.count();
                double dma_latency_avg_ms = 0.0;
                if (dma_latency_cnt > 0) {
                    dma_latency_avg_ms = (dma_latency_ns_total / 1e6) / dma_latency_cnt;
                }

                auto write_bytes = cur_uring.write_bytes - prev_uring.write_bytes;
                auto write_ops = cur_uring.write_ops - prev_uring.write_ops;
                auto range_sync_ops = cur_uring.range_sync_ops - prev_uring.range_sync_ops;
                auto sync_close_ops = cur_uring.sync_close_ops - prev_uring.sync_close_ops;
                auto write_latency_ns_total = cur_uring.write_ns_total - prev_uring.write_ns_total;
                auto write_latency_cnt = cur_uring.write_cnt - prev_uring.write_cnt;
                auto range_sync_latency_ns_total =
                    cur_uring.range_sync_ns_total - prev_uring.range_sync_ns_total;
                auto range_sync_latency_cnt =
                    cur_uring.range_sync_cnt - prev_uring.range_sync_cnt;
                auto sync_close_latency_ns_total =
                    cur_uring.sync_close_ns_total - prev_uring.sync_close_ns_total;
                auto sync_close_latency_cnt =
                    cur_uring.sync_close_cnt - prev_uring.sync_close_cnt;
                auto sync_close_wait_ns_total =
                    cur_uring.sync_close_wait_ns_total - prev_uring.sync_close_wait_ns_total;
                auto sync_close_wait_cnt =
                    cur_uring.sync_close_wait_cnt - prev_uring.sync_close_wait_cnt;
                double write_mbps = write_bytes / (1024.0 * 1024.0) / secs.count();
                double write_latency_avg_ms = 0.0;
                if (write_latency_cnt > 0) {
                    write_latency_avg_ms = (write_latency_ns_total / 1e6) / write_latency_cnt;
                }
                double range_sync_latency_avg_ms = 0.0;
                if (range_sync_latency_cnt > 0) {
                    range_sync_latency_avg_ms =
                        (range_sync_latency_ns_total / 1e6) / range_sync_latency_cnt;
                }
                double sync_close_latency_avg_ms = 0.0;
                if (sync_close_latency_cnt > 0) {
                    sync_close_latency_avg_ms =
                        (sync_close_latency_ns_total / 1e6) / sync_close_latency_cnt;
                }
                double sync_close_wait_avg_ms = 0.0;
                if (sync_close_wait_cnt > 0) {
                    sync_close_wait_avg_ms = (sync_close_wait_ns_total / 1e6) / sync_close_wait_cnt;
                }

                std::cout << fmt::format(
                    "[Perf] interval_s {:.2f}\n"
                    "  DMA   : {:.2f} MiB/s, {:.2f} ops/s, "
                    "lat_avg_ms {:.2f}, lat_max_ms {:.2f}, inflight={}, queued={}\n"
                    "  Uring : write {:.2f} MiB/s, write_ops {:.2f}/s, "
                    "write_lat_avg_ms {:.2f}, write_lat_max_ms {:.2f}\n"
                    "          range_sync_ops {:.2f}/s, range_sync_lat_avg_ms {:.2f}, "
                    "range_sync_lat_max_ms {:.2f}\n"
                    "          sync_close_ops {:.2f}/s, sync_close_lat_avg_ms {:.2f}, "
                    "sync_close_lat_max_ms {:.2f}\n"
                    "          sync_close_wait_avg_ms {:.2f}, sync_close_wait_max_ms {:.2f}, "
                    "inflight={}, cq_backlog={}",
                    secs.count(),
                    dma_mbps, dma_ops, dma_latency_avg_ms,
                    cur_dma.memcpy_ns_max / 1e6,
                    cur_dma.inflight_tasks, cur_dma.queued_tasks,
                    write_mbps,
                    write_ops / secs.count(),
                    write_latency_avg_ms,
                    cur_uring.write_ns_max / 1e6,
                    range_sync_ops / secs.count(),
                    range_sync_latency_avg_ms,
                    cur_uring.range_sync_ns_max / 1e6,
                    sync_close_ops / secs.count(),
                    sync_close_latency_avg_ms,
                    cur_uring.sync_close_ns_max / 1e6,
                    sync_close_wait_avg_ms,
                    cur_uring.sync_close_wait_ns_max / 1e6,
                    cur_uring.uring_cqe_backlog, cur_uring.cq_backlog
                ) << std::endl;

                prev_ts = now_ts;
                prev_dma = cur_dma;
                prev_uring = cur_uring;
            }
        });
    }

    backend.run();
    stop_stats.store(true, std::memory_order_relaxed);
    if (stats_thread.joinable()) {
        stats_thread.join();
    }
    auto end_ts = std::chrono::steady_clock::now();
    auto end_dma = backend.dma()->stats_snapshot();
    auto end_uring = uring_ptr->stats_snapshot();

    std::chrono::duration<double> secs = end_ts - start_ts;
    auto dma_bytes = end_dma.total_bytes - start_dma.total_bytes;
    auto dma_tasks = end_dma.total_tasks - start_dma.total_tasks;
    auto dma_latency_ns_total = end_dma.memcpy_ns_total - start_dma.memcpy_ns_total;
    auto dma_latency_cnt = end_dma.memcpy_cnt - start_dma.memcpy_cnt;
    double dma_mbps = dma_bytes / (1024.0 * 1024.0) / secs.count();
    double dma_ops = dma_tasks / secs.count();
    double dma_latency_avg_ms = 0.0;
    if (dma_latency_cnt > 0) {
        dma_latency_avg_ms = (dma_latency_ns_total / 1e6) / dma_latency_cnt;
    }

    auto write_bytes = end_uring.write_bytes - start_uring.write_bytes;
    auto write_ops = end_uring.write_ops - start_uring.write_ops;
    auto range_sync_ops = end_uring.range_sync_ops - start_uring.range_sync_ops;
    auto sync_close_ops = end_uring.sync_close_ops - start_uring.sync_close_ops;
    auto write_latency_ns_total = end_uring.write_ns_total - start_uring.write_ns_total;
    auto write_latency_cnt = end_uring.write_cnt - start_uring.write_cnt;
    auto range_sync_latency_ns_total =
        end_uring.range_sync_ns_total - start_uring.range_sync_ns_total;
    auto range_sync_latency_cnt =
        end_uring.range_sync_cnt - start_uring.range_sync_cnt;
    auto sync_close_latency_ns_total =
        end_uring.sync_close_ns_total - start_uring.sync_close_ns_total;
    auto sync_close_latency_cnt =
        end_uring.sync_close_cnt - start_uring.sync_close_cnt;
    auto sync_close_wait_ns_total =
        end_uring.sync_close_wait_ns_total - start_uring.sync_close_wait_ns_total;
    auto sync_close_wait_cnt =
        end_uring.sync_close_wait_cnt - start_uring.sync_close_wait_cnt;
    double write_mbps = write_bytes / (1024.0 * 1024.0) / secs.count();
    double write_latency_avg_ms = 0.0;
    if (write_latency_cnt > 0) {
        write_latency_avg_ms = (write_latency_ns_total / 1e6) / write_latency_cnt;
    }
    double range_sync_latency_avg_ms = 0.0;
    if (range_sync_latency_cnt > 0) {
        range_sync_latency_avg_ms =
            (range_sync_latency_ns_total / 1e6) / range_sync_latency_cnt;
    }
    double sync_close_latency_avg_ms = 0.0;
    if (sync_close_latency_cnt > 0) {
        sync_close_latency_avg_ms =
            (sync_close_latency_ns_total / 1e6) / sync_close_latency_cnt;
    }
    double sync_close_wait_avg_ms = 0.0;
    if (sync_close_wait_cnt > 0) {
        sync_close_wait_avg_ms = (sync_close_wait_ns_total / 1e6) / sync_close_wait_cnt;
    }

    std::cout << fmt::format(
        "[Perf-Summary] elapsed_s {:.2f}, dma_bytes {}\n"
        "  DMA   : {:.2f} MiB/s, {:.2f} ops/s, lat_avg_ms {:.2f}, lat_max_ms {:.2f}\n"
        "  Uring : write {:.2f} MiB/s, write_ops {:.2f}/s, "
        "write_lat_avg_ms {:.2f}, write_lat_max_ms {:.2f}\n"
        "          range_sync_ops {:.2f}/s, range_sync_lat_avg_ms {:.2f}, "
        "range_sync_lat_max_ms {:.2f}\n"
        "          sync_close_ops {:.2f}/s, sync_close_lat_avg_ms {:.2f}, "
        "sync_close_lat_max_ms {:.2f}\n"
        "          sync_close_wait_avg_ms {:.2f}, sync_close_wait_max_ms {:.2f}",
        secs.count(), dma_bytes,
        dma_mbps, dma_ops, dma_latency_avg_ms, end_dma.memcpy_ns_max / 1e6,
        write_mbps,
        write_ops / secs.count(),
        write_latency_avg_ms,
        end_uring.write_ns_max / 1e6,
        range_sync_ops / secs.count(),
        range_sync_latency_avg_ms,
        end_uring.range_sync_ns_max / 1e6,
        sync_close_ops / secs.count(),
        sync_close_latency_avg_ms,
        end_uring.sync_close_ns_max / 1e6,
        sync_close_wait_avg_ms,
        end_uring.sync_close_wait_ns_max / 1e6
    ) << std::endl;
    return 0;
}
