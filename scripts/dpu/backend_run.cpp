#include "comm/backend.h"
#include "io/sst_pool.h"
#include "io/uring_engine.h"

#include <argparse/argparse.hpp>
#include <fmt/format.h>

using namespace pulsedb;

std::string backend_name;
std::string dev_pci;
std::string dev_rep_pci;

size_t uring_queue_depth;
size_t dpu_req_rb_cap;
size_t dpu_resp_rb_cap;

std::string sst_pool_dir;
size_t base_pool_size;
size_t big_pool_cnt;
size_t big_pool_size;
size_t file_size_base;
size_t sst_pad_len;

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

    arg_parser.parse_args(argc, argv);

    backend_name = arg_parser.get<std::string>("-n");
    dev_pci = arg_parser.get<std::string>("-r");
    dev_rep_pci = arg_parser.get<std::string>("-R");

    uring_queue_depth = arg_parser.get<size_t>("-u");
    dpu_req_rb_cap = arg_parser.get<size_t>("-x");
    dpu_resp_rb_cap = arg_parser.get<size_t>("-y");

    sst_pool_dir = arg_parser.get<std::string>("-d");
    base_pool_size = arg_parser.get<size_t>("-s");
    big_pool_cnt = arg_parser.get<size_t>("-C");
    big_pool_size = arg_parser.get<size_t>("-S");
    file_size_base = arg_parser.get<size_t>("-b");
    sst_pad_len = arg_parser.get<size_t>("-p");

    std::cout << fmt::format(
        "backend name: {}\n"
        "dev pci: {}\n"
        "dev rep pci: {}\n"

        "uring queue depth: {}\n"
        "dpu req rb cap: {}\n"
        "dpu resp rb cap: {}\n"

        "sst pool dir: {}\n"
        "base pool size: {}\n"
        "big pool cnt: {}\n"
        "big pool size: {}\n"
        "file size base: {}\n"
        "sst padding len: {}\n",
        backend_name, dev_pci, dev_rep_pci,
        uring_queue_depth, dpu_req_rb_cap, dpu_resp_rb_cap,
        sst_pool_dir, base_pool_size,
        big_pool_cnt, big_pool_size,
        file_size_base, sst_pad_len
    );
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
    Backend backend(backend_name, dev_pci, dev_rep_pci,
        std::move(uring_ctx), dpu_req_rb_cap, dpu_resp_rb_cap);
    backend.run();
    return 0;
}
