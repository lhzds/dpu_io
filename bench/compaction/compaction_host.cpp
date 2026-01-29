#include <argparse/argparse.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <iostream>
#include <mutex>
#include <queue>
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
    size_t slot_pool_size{};

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
    size_t base_size_multiplier{};

    size_t stats_interval_ms{};
    size_t delete_batch_size{};
};

struct AtomicStats {
    std::atomic<uint64_t> wait_all_ns_total{};
    std::atomic<uint64_t> wait_all_cnt{};
    std::atomic<uint64_t> wait_all_ns_max{};
    std::atomic<uint64_t> wait_slot_ns_total{};
    std::atomic<uint64_t> wait_slot_cnt{};
    std::atomic<uint64_t> wait_slot_ns_max{};
};

class SlotPool {
public:
    explicit SlotPool(size_t size): m_available(size) {}

    uint64_t acquire() {
        std::unique_lock<std::mutex> lock(m_mu);
        if (m_available > 0) {
            --m_available;
            return 0;
        }
        auto start = std::chrono::steady_clock::now();
        m_cv.wait(lock, [&] { return m_available > 0; });
        --m_available;
        auto end = std::chrono::steady_clock::now();
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
    }

    void release(size_t n) {
        {
            std::lock_guard<std::mutex> lock(m_mu);
            m_available += n;
        }
        m_cv.notify_all();
    }

private:
    std::mutex m_mu;
    std::condition_variable m_cv;
    size_t m_available{};
};

class DeleteQueue {
public:
    DeleteQueue(Frontend &frontend, SlotPool &slot_pool, size_t batch_size)
        : m_frontend(frontend), m_slot_pool(slot_pool), m_batch_size(batch_size) {}

    void enqueue(std::vector<size_t> file_nums) {
        if (file_nums.empty()) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(m_mu);
            m_queue.emplace(std::move(file_nums));
        }
        m_cv.notify_one();
    }

    void stop() {
        {
            std::lock_guard<std::mutex> lock(m_mu);
            m_stop = true;
        }
        m_cv.notify_all();
    }

    void run() {
        while (true) {
            std::vector<size_t> file_nums;
            {
                std::unique_lock<std::mutex> lock(m_mu);
                m_cv.wait(lock, [&] { return m_stop || !m_queue.empty(); });
                if (m_stop && m_queue.empty()) {
                    break;
                }
                file_nums = std::move(m_queue.front());
                m_queue.pop();
            }
            process_files(file_nums);
        }
    }

private:
    void process_files(const std::vector<size_t> &file_nums) {
        size_t idx = 0;
        while (idx < file_nums.size()) {
            size_t batch_size = std::min(m_batch_size, file_nums.size() - idx);
            std::vector<size_t> batch;
            batch.reserve(batch_size);
            for (size_t i = 0; i < batch_size; ++i) {
                batch.push_back(file_nums[idx++]);
            }
            m_frontend.delete_files(batch);
            m_slot_pool.release(batch.size());
        }
    }

    Frontend &m_frontend;
    SlotPool &m_slot_pool;
    size_t m_batch_size{};
    std::mutex m_mu;
    std::condition_variable m_cv;
    std::queue<std::vector<size_t>> m_queue;
    bool m_stop{};
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
    arg_parser.add_argument("--slot_pool_size")
        .help("max in-flight files limited by sst pool size")
        .scan<'d', size_t>()
        .required();

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
    arg_parser.add_argument("-i")
        .help("stats interval ms (0 disable)")
        .scan<'d', size_t>()
        .default_value(0);
    arg_parser.add_argument("--delete_batch_size")
        .help("delete_files batch size")
        .scan<'d', size_t>()
        .default_value(128);

    arg_parser.parse_args(argc, argv);

    Options opts;
    opts.backend_name = arg_parser.get<std::string>("-n");
    opts.dev_pci = arg_parser.get<std::string>("-r");
    opts.qp_num = arg_parser.get<size_t>("-q");
    opts.req_rb_cap = arg_parser.get<size_t>("-x");
    opts.resp_rb_cap = arg_parser.get<size_t>("-y");
    opts.slot_pool_size = arg_parser.get<size_t>("--slot_pool_size");

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

    opts.stats_interval_ms = arg_parser.get<size_t>("-i");
    opts.delete_batch_size = arg_parser.get<size_t>("--delete_batch_size");

    if (opts.qp_num == 0) {
        opts.qp_num = opts.threads;
    }
    if (opts.write_size == 0 || opts.file_size == 0) {
        throw_with("write_size and file_size must be > 0");
    }
    if (opts.write_size > opts.req_rb_cap) {
        throw_with("write_size must be <= req_rb_cap");
    }
    if (opts.slot_pool_size == 0) {
        throw_with("slot_pool_size must be > 0");
    }
    if (opts.delete_batch_size == 0) {
        throw_with("delete_batch_size must be > 0");
    }
    if (opts.file_size_base == 0) {
        throw_with("file_size_base must be > 0");
    }
    opts.base_size_multiplier =
        (opts.file_size + opts.file_size_base - 1) / opts.file_size_base;
    if (opts.base_size_multiplier == 0) {
        opts.base_size_multiplier = 1;
    }
    return opts;
}

void run_worker(size_t thread_id, Frontend &frontend, const Options &opts, AtomicStats &stats,
                SlotPool &slot_pool, DeleteQueue &deleter, std::atomic<size_t> &next_file_number) {
    auto *qp = frontend.alloc_qp();

    std::string payload(opts.write_size, static_cast<char>('a' + (thread_id % 26)));
    size_t inflight_files = 0;

    for (size_t f = 0; f < opts.files_per_thread; ++f) {
        auto wait_ns = slot_pool.acquire();
        if (wait_ns > 0) {
            stats.wait_slot_ns_total.fetch_add(wait_ns, std::memory_order_relaxed);
            stats.wait_slot_cnt.fetch_add(1, std::memory_order_relaxed);
            auto prev_max = stats.wait_slot_ns_max.load(std::memory_order_relaxed);
            while (wait_ns > static_cast<uint64_t>(prev_max) &&
                   !stats.wait_slot_ns_max.compare_exchange_weak(
                       prev_max, wait_ns, std::memory_order_relaxed)) {
            }
        }

        size_t file_number = next_file_number.fetch_add(1, std::memory_order_relaxed);
        size_t offset = 0;
        size_t last_sync_offset = 0;
        while (offset < opts.file_size) {
            size_t cur_size = std::min(opts.write_size, opts.file_size - offset);
            qp->prep_write(file_number, offset, opts.base_size_multiplier);
            qp->append({payload.data(), cur_size});
            qp->post_write();

            offset += cur_size;
            if (opts.range_sync_size > 0 &&
                (offset - last_sync_offset) >= opts.range_sync_size) {
                size_t sync_size = offset - last_sync_offset;
                qp->post_range_sync(file_number, last_sync_offset, sync_size, opts.wait_before);
                last_sync_offset = offset;
            }
        }
        qp->post_sync_close(file_number, opts.use_fsync);

        ++inflight_files;
        if (opts.wait_every_files > 0 && inflight_files >= opts.wait_every_files) {
            auto start_ts = std::chrono::steady_clock::now();
            auto file_map = qp->wait_all();
            auto end_ts = std::chrono::steady_clock::now();
            auto wait_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_ts - start_ts).count();
            stats.wait_all_ns_total.fetch_add(static_cast<uint64_t>(wait_ns), std::memory_order_relaxed);
            stats.wait_all_cnt.fetch_add(1, std::memory_order_relaxed);
            auto prev_max = stats.wait_all_ns_max.load(std::memory_order_relaxed);
            while (wait_ns > static_cast<int64_t>(prev_max) &&
                   !stats.wait_all_ns_max.compare_exchange_weak(
                       prev_max, static_cast<uint64_t>(wait_ns), std::memory_order_relaxed)) {
            }
            if (!file_map.empty()) {
                std::vector<size_t> file_nums;
                file_nums.reserve(file_map.size());
                for (const auto &item : file_map) {
                    file_nums.push_back(item.first);
                }
                deleter.enqueue(std::move(file_nums));
            }
            inflight_files = 0;
        }
    }

    if (inflight_files > 0) {
        auto start_ts = std::chrono::steady_clock::now();
        auto file_map = qp->wait_all();
        auto end_ts = std::chrono::steady_clock::now();
        auto wait_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_ts - start_ts).count();
        stats.wait_all_ns_total.fetch_add(static_cast<uint64_t>(wait_ns), std::memory_order_relaxed);
        stats.wait_all_cnt.fetch_add(1, std::memory_order_relaxed);
        auto prev_max = stats.wait_all_ns_max.load(std::memory_order_relaxed);
        while (wait_ns > static_cast<int64_t>(prev_max) &&
               !stats.wait_all_ns_max.compare_exchange_weak(
                   prev_max, static_cast<uint64_t>(wait_ns), std::memory_order_relaxed)) {
        }
        if (!file_map.empty()) {
            std::vector<size_t> file_nums;
            file_nums.reserve(file_map.size());
            for (const auto &item : file_map) {
                file_nums.push_back(item.first);
            }
            deleter.enqueue(std::move(file_nums));
        }
    }
    frontend.free_qp(qp);
}
} // namespace

int main(int argc, char **argv) {
    auto opts = read_args(argc, argv);
    std::cout << "[Host-Config]\n"
              << "  backend           : " << opts.backend_name << "\n"
              << "  dev_pci           : " << opts.dev_pci << "\n"
              << "  threads           : " << opts.threads << "\n"
              << "  qp_num            : " << opts.qp_num << "\n"
              << "  slot_pool_size    : " << opts.slot_pool_size << "\n"
              << "  files_per_thread  : " << opts.files_per_thread << "\n"
              << "  wait_every_files  : " << opts.wait_every_files << "\n"
              << "  file_size         : " << opts.file_size << "\n"
              << "  write_size        : " << opts.write_size << "\n"
              << "  range_sync_size   : " << opts.range_sync_size << "\n"
              << "  wait_before       : " << opts.wait_before << "\n"
              << "  use_fsync         : " << opts.use_fsync << "\n"
              << "  delete_batch_size : " << opts.delete_batch_size << std::endl;

    auto frontend = std::make_unique<Frontend>(
        opts.backend_name, opts.dev_pci, static_cast<int>(opts.qp_num),
        opts.req_rb_cap, opts.resp_rb_cap
    );

    AtomicStats stats;
    SlotPool slot_pool(opts.slot_pool_size);
    DeleteQueue deleter(*frontend, slot_pool, opts.delete_batch_size);
    std::atomic<size_t> next_file_number{opts.start_file_number};
    std::atomic<bool> stop_stats{};
    std::thread stats_thread;
    std::thread delete_thread([&] { deleter.run(); });
    if (opts.stats_interval_ms > 0) {
        auto prev_ts = std::chrono::steady_clock::now();
        uint64_t prev_wait_all_ns = 0;
        uint64_t prev_wait_all_cnt = 0;
        uint64_t prev_wait_slot_ns = 0;
        uint64_t prev_wait_slot_cnt = 0;
        stats_thread = std::thread([&] {
            while (!stop_stats.load(std::memory_order_relaxed)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(opts.stats_interval_ms));
                auto now_ts = std::chrono::steady_clock::now();
                std::chrono::duration<double> secs = now_ts - prev_ts;
                if (secs.count() <= 0.0) {
                    prev_ts = now_ts;
                    continue;
                }
                uint64_t cur_wait_all_ns = stats.wait_all_ns_total.load(std::memory_order_relaxed);
                uint64_t cur_wait_all_cnt = stats.wait_all_cnt.load(std::memory_order_relaxed);
                uint64_t cur_wait_all_ns_max = stats.wait_all_ns_max.load(std::memory_order_relaxed);
                uint64_t cur_wait_slot_ns = stats.wait_slot_ns_total.load(std::memory_order_relaxed);
                uint64_t cur_wait_slot_cnt = stats.wait_slot_cnt.load(std::memory_order_relaxed);
                uint64_t cur_wait_slot_ns_max = stats.wait_slot_ns_max.load(std::memory_order_relaxed);
                uint64_t delta_wait_all_ns = cur_wait_all_ns - prev_wait_all_ns;
                uint64_t delta_wait_all_cnt = cur_wait_all_cnt - prev_wait_all_cnt;
                uint64_t delta_wait_slot_ns = cur_wait_slot_ns - prev_wait_slot_ns;
                uint64_t delta_wait_slot_cnt = cur_wait_slot_cnt - prev_wait_slot_cnt;

                double wait_all_avg_ms = 0.0;
                if (delta_wait_all_cnt > 0) {
                    wait_all_avg_ms = (delta_wait_all_ns / 1e6) / delta_wait_all_cnt;
                }
                double wait_slot_avg_ms = 0.0;
                if (delta_wait_slot_cnt > 0) {
                    wait_slot_avg_ms = (delta_wait_slot_ns / 1e6) / delta_wait_slot_cnt;
                }
                std::cout << "[Host-Perf] interval_s " << secs.count() << "\n"
                          << "  wait_all : avg_ms " << wait_all_avg_ms
                          << ", max_ms " << (cur_wait_all_ns_max / 1e6) << "\n"
                          << "  wait_slot: avg_ms " << wait_slot_avg_ms
                          << ", max_ms " << (cur_wait_slot_ns_max / 1e6)
                          << std::endl;

                prev_ts = now_ts;
                prev_wait_all_ns = cur_wait_all_ns;
                prev_wait_all_cnt = cur_wait_all_cnt;
                prev_wait_slot_ns = cur_wait_slot_ns;
                prev_wait_slot_cnt = cur_wait_slot_cnt;
            }
        });
    }

    auto start = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    workers.reserve(opts.threads);
    for (size_t i = 0; i < opts.threads; ++i) {
        workers.emplace_back([&, i] {
            run_worker(i, *frontend, opts, stats, slot_pool, deleter, next_file_number);
        });
    }
    for (auto &t : workers) {
        t.join();
    }
    deleter.stop();
    if (delete_thread.joinable()) {
        delete_thread.join();
    }
    auto end = std::chrono::steady_clock::now();
    stop_stats.store(true, std::memory_order_relaxed);
    if (stats_thread.joinable()) {
        stats_thread.join();
    }

    std::chrono::duration<double> secs = end - start;
    uint64_t total_bytes = static_cast<uint64_t>(opts.file_size) *
        static_cast<uint64_t>(opts.files_per_thread) *
        static_cast<uint64_t>(opts.threads);
    double mbps = total_bytes / (1024.0 * 1024.0) / secs.count();
    uint64_t wait_all_ns_total = stats.wait_all_ns_total.load(std::memory_order_relaxed);
    uint64_t wait_all_cnt = stats.wait_all_cnt.load(std::memory_order_relaxed);
    uint64_t wait_all_ns_max = stats.wait_all_ns_max.load(std::memory_order_relaxed);
    uint64_t wait_slot_ns_total = stats.wait_slot_ns_total.load(std::memory_order_relaxed);
    uint64_t wait_slot_cnt = stats.wait_slot_cnt.load(std::memory_order_relaxed);
    uint64_t wait_slot_ns_max = stats.wait_slot_ns_max.load(std::memory_order_relaxed);
    double wait_all_avg_ms = 0.0;
    if (wait_all_cnt > 0) {
        wait_all_avg_ms = (wait_all_ns_total / 1e6) / wait_all_cnt;
    }
    double wait_slot_avg_ms = 0.0;
    if (wait_slot_cnt > 0) {
        wait_slot_avg_ms = (wait_slot_ns_total / 1e6) / wait_slot_cnt;
    }
    std::cout << "[Host-Summary]\n"
              << "  total_bytes       : " << total_bytes << "\n"
              << "  elapsed_s         : " << secs.count() << "\n"
              << "  throughput_mib_s  : " << mbps << "\n"
              << "  wait_all_avg_ms   : " << wait_all_avg_ms << "\n"
              << "  wait_all_max_ms   : " << (wait_all_ns_max / 1e6) << "\n"
              << "  wait_all_total_s  : " << (wait_all_ns_total / 1e9) << "\n"
              << "  wait_slot_avg_ms  : " << wait_slot_avg_ms << "\n"
              << "  wait_slot_max_ms  : " << (wait_slot_ns_max / 1e6) << "\n"
              << "  wait_slot_total_s : " << (wait_slot_ns_total / 1e9)
              << std::endl;
    return 0;
}
