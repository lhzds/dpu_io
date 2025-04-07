#include "io/sst_pool.h"

#include <argparse/argparse.hpp>
#include <fmt/format.h>

std::string sst_pool_dir;
size_t base_pool_size;
size_t big_pool_cnt;
size_t big_pool_size;
size_t file_size_base;
size_t sst_pad_len;

void read_args(int argc, char **argv) {
    argparse::ArgumentParser arg_parser;
    
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

    sst_pool_dir = arg_parser.get<std::string>("-d");
    base_pool_size = arg_parser.get<size_t>("-s");
    big_pool_cnt = arg_parser.get<size_t>("-C");
    big_pool_size = arg_parser.get<size_t>("-S");
    file_size_base = arg_parser.get<size_t>("-b");
    sst_pad_len = arg_parser.get<size_t>("-p");

    std::cout << fmt::format(
        "sst pool dir: {}\n"
        "base pool size: {}\n"
        "big pool cnt: {}\n"
        "big pool size: {}\n"
        "file size base: {}\n"
        "sst padding len: {}\n",
        sst_pool_dir, base_pool_size,
        big_pool_cnt, big_pool_size,
        file_size_base, sst_pad_len
    );
}

int main(int argc, char *argv[]) {
    read_args(argc, argv);

    pulsedb::SstPool sst_pool(sst_pool_dir, base_pool_size,
        big_pool_cnt, big_pool_size, file_size_base, sst_pad_len);
    sst_pool.create_ssts();

    return 0;
}