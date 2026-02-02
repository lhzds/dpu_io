#include <argparse/argparse.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <iostream>
#include <mutex>
#include <boost/lockfree/queue.hpp>
#include <string>
#include <thread>
#include <vector>

#include "bytes.h"
#include "io/sst_pool.h"
#include "io/uring_engine.h"
#include "util.h"

using namespace pulsedb;

namespace {
constexpr size_t KiB = 1024;
constexpr size_t MiB = 1024 * KiB;

struct Options {
    std::string sst_dir;
    size_t base_pool_size{};
    size_t big_pool_cnt{};
    size_t big_pool_size{};
    size_t file_size_base{};
    size_t sst_pad_len{};
    bool create_ssts{};

    size_t queue_depth{};
    size_t threads{};
    size_t files_per_thread{};
    size_t file_size{};
    size_t write_size{};
    size_t range_sync_size{};
    bool wait_before{};
    bool use_fsync{};
    size_t start_file_number{};
    size_t stats_interval_ms{};

    bool delete_after_sync{};
    size_t delete_batch_size{};

    size_t base_size_multiplier{};
};

enum class TaskType {
    kWrite,
    kRangeSync,
    kSyncClose,
    kDelete
};

struct Task {
    TaskType type{};
    size_t file_number{};
    size_t offset{};
    size_t size{};
    bool wait_before{};
    bool use_fsync{};
    BytesBuffer *payload{};
};

class TaskQueue {
public:
    void enqueue(Task task) {
        while (!m_queue.push(task)) {
            std::this_thread::yield();
        }
        m_size.fetch_add(1, std::memory_order_relaxed);
    }

    bool try_pop(Task &task) {
        if (!m_queue.pop(task)) {
            return false;
        }
        m_size.fetch_add(-1, std::memory_order_relaxed);
        return true;
    }

    bool has_pending() const {
        return m_size.load(std::memory_order_relaxed) > 0;
    }

private:
    static constexpr size_t kTaskQueueCapacity = 1 << 15;
    boost::lockfree::queue<Task, boost::lockfree::capacity<kTaskQueueCapacity>> m_queue;
    std::atomic<size_t> m_size{};
};

Options read_args(int argc, char **argv) {
    argparse::ArgumentParser arg_parser("bench_uring_host");

    arg_parser.add_argument("--sst_dir")
        .help("sst pool dir")
        .required();
    arg_parser.add_argument("--base_pool_size")
        .help("base pool size")
        .scan<'d', size_t>()
        .default_value(4000);
    arg_parser.add_argument("--big_pool_cnt")
        .help("big pool cnt")
        .scan<'d', size_t>()
        .default_value(0);
    arg_parser.add_argument("--big_pool_size")
        .help("big pool size")
        .scan<'d', size_t>()
        .default_value(0);
    arg_parser.add_argument("--file_size_base")
        .help("file size base (bytes)")
        .scan<'d', size_t>()
        .default_value(64 * MiB);
    arg_parser.add_argument("--sst_pad_len")
        .help("sst padding len (bytes)")
        .scan<'d', size_t>()
        .default_value(1 * MiB);
    arg_parser.add_argument("--create_ssts")
        .help("create sst files if missing")
        .default_value(false)
        .implicit_value(true);

    arg_parser.add_argument("--queue_depth")
        .help("uring queue depth")
        .scan<'d', size_t>()
        .default_value(1024);
    arg_parser.add_argument("-t")
        .help("threads")
        .scan<'d', size_t>()
        .default_value(1);
    arg_parser.add_argument("-f")
        .help("files per thread")
        .scan<'d', size_t>()
        .default_value(1);
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
    arg_parser.add_argument("--stats_interval_ms")
        .help("stats interval ms (0 disable)")
        .scan<'d', size_t>()
        .default_value(0);

    arg_parser.add_argument("--delete_after_sync")
        .help("delete file after sync_close")
        .default_value(false)
        .implicit_value(true);
    arg_parser.add_argument("--delete_batch_size")
        .help("delete batch size")
        .scan<'d', size_t>()
        .default_value(128);

    arg_parser.parse_args(argc, argv);

    Options opts;
    opts.sst_dir = arg_parser.get<std::string>("--sst_dir");
    opts.base_pool_size = arg_parser.get<size_t>("--base_pool_size");
    opts.big_pool_cnt = arg_parser.get<size_t>("--big_pool_cnt");
    opts.big_pool_size = arg_parser.get<size_t>("--big_pool_size");
    opts.file_size_base = arg_parser.get<size_t>("--file_size_base");
    opts.sst_pad_len = arg_parser.get<size_t>("--sst_pad_len");
    opts.create_ssts = arg_parser.get<bool>("--create_ssts");

    opts.queue_depth = arg_parser.get<size_t>("--queue_depth");
    opts.threads = arg_parser.get<size_t>("-t");
    opts.files_per_thread = arg_parser.get<size_t>("-f");
    opts.file_size = arg_parser.get<size_t>("-s");
    opts.write_size = arg_parser.get<size_t>("-w");
    opts.range_sync_size = arg_parser.get<size_t>("-g");
    opts.wait_before = arg_parser.get<bool>("--wait_before");
    opts.use_fsync = arg_parser.get<bool>("--use_fsync");
    opts.start_file_number = arg_parser.get<size_t>("--start_file_number");
    opts.stats_interval_ms = arg_parser.get<size_t>("--stats_interval_ms");

    opts.delete_after_sync = arg_parser.get<bool>("--delete_after_sync");
    opts.delete_batch_size = arg_parser.get<size_t>("--delete_batch_size");

    if (opts.write_size == 0 || opts.file_size == 0) {
        throw_with("write_size and file_size must be > 0");
    }
    if (opts.file_size_base == 0) {
        throw_with("file_size_base must be > 0");
    }
    if (opts.delete_batch_size == 0) {
        throw_with("delete_batch_size must be > 0");
    }
    if (opts.queue_depth == 0) {
        throw_with("queue_depth must be > 0");
    }
    opts.base_size_multiplier =
        (opts.file_size + opts.file_size_base - 1) / opts.file_size_base;
    if (opts.base_size_multiplier == 0) {
        opts.base_size_multiplier = 1;
    }
    size_t pool_id = opts.base_size_multiplier - 1;
    if (pool_id > opts.big_pool_cnt) {
        throw_with("file_size requires big_pool_cnt >= {}", pool_id);
    }
    if (!opts.delete_after_sync) {
        size_t total_files = opts.threads * opts.files_per_thread;
        if (pool_id == 0 && total_files > opts.base_pool_size) {
            throw_with("base_pool_size must be >= total files without delete");
        }
        if (pool_id > 0 && total_files > opts.big_pool_size) {
            throw_with("big_pool_size must be >= total files without delete");
        }
    }

    return opts;
}

struct StatsDelta {
    uint64_t write_bytes{};
    uint64_t write_ops{};
    uint64_t range_sync_ops{};
    uint64_t sync_close_ops{};
    uint64_t write_ns_total{};
    uint64_t write_cnt{};
    uint64_t range_sync_ns_total{};
    uint64_t range_sync_cnt{};
    uint64_t sync_close_ns_total{};
    uint64_t sync_close_cnt{};
    uint64_t sync_close_wait_ns_total{};
    uint64_t sync_close_wait_cnt{};
};

StatsDelta make_delta(const UringEngine::StatsSnapshot &cur,
                      const UringEngine::StatsSnapshot &prev) {
    StatsDelta delta;
    delta.write_bytes = cur.write_bytes - prev.write_bytes;
    delta.write_ops = cur.write_ops - prev.write_ops;
    delta.range_sync_ops = cur.range_sync_ops - prev.range_sync_ops;
    delta.sync_close_ops = cur.sync_close_ops - prev.sync_close_ops;
    delta.write_ns_total = cur.write_ns_total - prev.write_ns_total;
    delta.write_cnt = cur.write_cnt - prev.write_cnt;
    delta.range_sync_ns_total = cur.range_sync_ns_total - prev.range_sync_ns_total;
    delta.range_sync_cnt = cur.range_sync_cnt - prev.range_sync_cnt;
    delta.sync_close_ns_total = cur.sync_close_ns_total - prev.sync_close_ns_total;
    delta.sync_close_cnt = cur.sync_close_cnt - prev.sync_close_cnt;
    delta.sync_close_wait_ns_total =
        cur.sync_close_wait_ns_total - prev.sync_close_wait_ns_total;
    delta.sync_close_wait_cnt =
        cur.sync_close_wait_cnt - prev.sync_close_wait_cnt;
    return delta;
}

double avg_ms(uint64_t total_ns, uint64_t cnt) {
    if (cnt == 0) {
        return 0.0;
    }
    return (total_ns / 1e6) / cnt;
}

void run_worker(size_t thread_id, const Options &opts,
                BytesBuffer &payload,
                TaskQueue &task_queue,
                std::atomic<size_t> &next_file_number,
                std::atomic<size_t> &pending_files) {
    for (size_t f = 0; f < opts.files_per_thread; ++f) {
        size_t file_number = next_file_number.fetch_add(1, std::memory_order_relaxed);
        size_t offset = 0;
        size_t last_sync_offset = 0;
        while (offset < opts.file_size) {
            size_t cur_size = std::min(opts.write_size, opts.file_size - offset);
            task_queue.enqueue(Task{
                TaskType::kWrite,
                file_number,
                offset,
                cur_size,
                false,
                false,
                &payload
            });

            offset += cur_size;
            if (opts.range_sync_size > 0 &&
                (offset - last_sync_offset) >= opts.range_sync_size) {
                size_t sync_size = offset - last_sync_offset;
                task_queue.enqueue(Task{
                    TaskType::kRangeSync,
                    file_number,
                    last_sync_offset,
                    sync_size,
                    opts.wait_before,
                    false,
                    nullptr
                });
                last_sync_offset = offset;
            }
        }
        pending_files.fetch_add(1, std::memory_order_relaxed);
        task_queue.enqueue(Task{
            TaskType::kSyncClose,
            file_number,
            0,
            0,
            false,
            opts.use_fsync,
            nullptr
        });
    }
}
} // namespace

int main(int argc, char **argv) {
    auto opts = read_args(argc, argv);
    std::cout << "[Uring-Config]\n"
              << "  sst_dir           : " << opts.sst_dir << "\n"
              << "  base_pool_size    : " << opts.base_pool_size << "\n"
              << "  big_pool_cnt      : " << opts.big_pool_cnt << "\n"
              << "  big_pool_size     : " << opts.big_pool_size << "\n"
              << "  file_size_base    : " << opts.file_size_base << "\n"
              << "  sst_pad_len       : " << opts.sst_pad_len << "\n"
              << "  create_ssts       : " << opts.create_ssts << "\n"
              << "  queue_depth       : " << opts.queue_depth << "\n"
              << "  threads           : " << opts.threads << "\n"
              << "  files_per_thread  : " << opts.files_per_thread << "\n"
              << "  file_size         : " << opts.file_size << "\n"
              << "  write_size        : " << opts.write_size << "\n"
              << "  range_sync_size   : " << opts.range_sync_size << "\n"
              << "  wait_before       : " << opts.wait_before << "\n"
              << "  use_fsync         : " << opts.use_fsync << "\n"
              << "  start_file_number : " << opts.start_file_number << "\n"
              << "  stats_interval_ms : " << opts.stats_interval_ms << "\n"
              << "  delete_after_sync : " << opts.delete_after_sync << "\n"
              << "  delete_batch_size : " << opts.delete_batch_size << std::endl;

    auto sst_pool = std::make_unique<SstPool>(
        opts.sst_dir,
        static_cast<int>(opts.base_pool_size),
        static_cast<int>(opts.big_pool_cnt),
        static_cast<int>(opts.big_pool_size),
        opts.file_size_base,
        opts.sst_pad_len
    );
    if (opts.create_ssts) {
        sst_pool->create_ssts();
    }

    auto engine = std::make_unique<UringEngine>(
        static_cast<uint32_t>(opts.queue_depth), std::move(sst_pool));

    std::atomic<size_t> next_file_number{opts.start_file_number};
    std::atomic<size_t> pending_files{};
    TaskQueue task_queue;

    std::vector<BytesBuffer> payloads;
    payloads.reserve(opts.threads);
    for (size_t i = 0; i < opts.threads; ++i) {
        payloads.emplace_back(opts.write_size);
        std::memset(payloads.back().data(), static_cast<int>('a' + (i % 26)),
            payloads.back().size());
    }

    std::atomic<bool> stop_submit{};
    std::thread submit_thread([&] {
        Task task;
        while (!stop_submit.load(std::memory_order_relaxed) ||
               pending_files.load(std::memory_order_relaxed) > 0 ||
               task_queue.has_pending()) {
            bool progressed = false;
            for (int i = 0; i < 64; ++i) {
                if (!task_queue.try_pop(task)) {
                    break;
                }
                progressed = true;
                switch (task.type) {
                    case TaskType::kWrite: {
                        auto view = RingBuffer::View(
                            task.payload->subspan(0, task.size));
                        engine->write_async(task.file_number, task.offset,
                            static_cast<int>(opts.base_size_multiplier), view, nullptr);
                        break;
                    }
                    case TaskType::kRangeSync:
                        engine->range_sync_async(task.file_number, task.offset,
                            task.size, task.wait_before, nullptr);
                        break;
                    case TaskType::kSyncClose: {
                        auto *cb = IoEngine::CallbackFn::make(
                            [&](size_t done_file, int res) {
                                if (res < 0) {
                                    std::cerr << "sync_close failed on file "
                                              << done_file << ": " << strerror(-res)
                                              << std::endl;
                                }
                                if (opts.delete_after_sync) {
                                    task_queue.enqueue(Task{
                                        TaskType::kDelete,
                                        done_file,
                                        0,
                                        0,
                                        false,
                                        false,
                                        nullptr
                                    });
                                }
                                pending_files.fetch_add(-1, std::memory_order_relaxed);
                            });
                        engine->sync_close_async(task.file_number, task.use_fsync, cb);
                        break;
                    }
                    case TaskType::kDelete:
                        engine->delete_file(task.file_number);
                        break;
                }
            }
            while (engine->query()) {
                progressed = true;
            }
            if (!progressed) {
                std::this_thread::yield();
            }
        }
    });

    std::atomic<bool> stop_stats{};
    std::thread stats_thread;
    if (opts.stats_interval_ms > 0) {
        auto prev_ts = std::chrono::steady_clock::now();
        auto prev_stats = engine->stats_snapshot();
        stats_thread = std::thread([&] {
            while (!stop_stats.load(std::memory_order_relaxed)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(opts.stats_interval_ms));
                auto now_ts = std::chrono::steady_clock::now();
                std::chrono::duration<double> secs = now_ts - prev_ts;
                if (secs.count() <= 0.0) {
                    prev_ts = now_ts;
                    continue;
                }
                auto cur_stats = engine->stats_snapshot();
                auto delta = make_delta(cur_stats, prev_stats);

                double write_mbps = delta.write_bytes / (1024.0 * 1024.0) / secs.count();
                double write_ops = delta.write_ops / secs.count();
                double range_sync_ops = delta.range_sync_ops / secs.count();
                double sync_close_ops = delta.sync_close_ops / secs.count();

                std::cout << "[Uring-Perf] interval_s " << secs.count() << "\n"
                          << "  write       : " << write_mbps << " MiB/s, "
                          << write_ops << " ops/s, lat_avg_ms "
                          << avg_ms(delta.write_ns_total, delta.write_cnt)
                          << ", lat_max_ms " << (cur_stats.write_ns_max / 1e6) << "\n"
                          << "  range_sync  : " << range_sync_ops << " ops/s, lat_avg_ms "
                          << avg_ms(delta.range_sync_ns_total, delta.range_sync_cnt)
                          << ", lat_max_ms " << (cur_stats.range_sync_ns_max / 1e6) << "\n"
                          << "  sync_close  : " << sync_close_ops << " ops/s, lat_avg_ms "
                          << avg_ms(delta.sync_close_ns_total, delta.sync_close_cnt)
                          << ", lat_max_ms " << (cur_stats.sync_close_ns_max / 1e6) << "\n"
                          << "  close_wait  : avg_ms "
                          << avg_ms(delta.sync_close_wait_ns_total, delta.sync_close_wait_cnt)
                          << ", max_ms " << (cur_stats.sync_close_wait_ns_max / 1e6)
                          << ", inflight=" << cur_stats.uring_cqe_backlog
                          << ", cq_backlog=" << cur_stats.cq_backlog
                          << std::endl;

                prev_ts = now_ts;
                prev_stats = cur_stats;
            }
        });
    }

    auto start_ts = std::chrono::steady_clock::now();
    auto start_stats = engine->stats_snapshot();

    std::vector<std::thread> workers;
    workers.reserve(opts.threads);
    for (size_t i = 0; i < opts.threads; ++i) {
        workers.emplace_back([&, i] {
            run_worker(i, opts, payloads[i], task_queue, next_file_number, pending_files);
        });
    }
    for (auto &t : workers) {
        t.join();
    }

    stop_submit.store(true, std::memory_order_relaxed);
    if (submit_thread.joinable()) {
        submit_thread.join();
    }

    auto end_ts = std::chrono::steady_clock::now();
    stop_stats.store(true, std::memory_order_relaxed);
    if (stats_thread.joinable()) {
        stats_thread.join();
    }

    auto end_stats = engine->stats_snapshot();
    std::chrono::duration<double> secs = end_ts - start_ts;
    auto delta = make_delta(end_stats, start_stats);
    double write_mbps = delta.write_bytes / (1024.0 * 1024.0) / secs.count();
    double write_ops = delta.write_ops / secs.count();
    double range_sync_ops = delta.range_sync_ops / secs.count();
    double sync_close_ops = delta.sync_close_ops / secs.count();
    size_t total_files = opts.threads * opts.files_per_thread;
    uint64_t total_bytes = static_cast<uint64_t>(opts.file_size) *
        static_cast<uint64_t>(total_files);

    std::cout << "[Uring-Summary] elapsed_s " << secs.count()
              << ", total_bytes " << total_bytes << "\n"
              << "  write       : " << write_mbps << " MiB/s, "
              << write_ops << " ops/s, lat_avg_ms "
              << avg_ms(delta.write_ns_total, delta.write_cnt)
              << ", lat_max_ms " << (end_stats.write_ns_max / 1e6) << "\n"
              << "  range_sync  : " << range_sync_ops << " ops/s, lat_avg_ms "
              << avg_ms(delta.range_sync_ns_total, delta.range_sync_cnt)
              << ", lat_max_ms " << (end_stats.range_sync_ns_max / 1e6) << "\n"
              << "  sync_close  : " << sync_close_ops << " ops/s, lat_avg_ms "
              << avg_ms(delta.sync_close_ns_total, delta.sync_close_cnt)
              << ", lat_max_ms " << (end_stats.sync_close_ns_max / 1e6) << "\n"
              << "  close_wait  : avg_ms "
              << avg_ms(delta.sync_close_wait_ns_total, delta.sync_close_wait_cnt)
              << ", max_ms " << (end_stats.sync_close_wait_ns_max / 1e6)
              << std::endl;
    return 0;
}
