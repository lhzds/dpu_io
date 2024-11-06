#include <argparse/argparse.hpp>
#include <chrono>
#include <cstdlib>
#include <fcntl.h>
#include <iostream>
#include <stdexcept>
#include <sys/ioctl.h>
#include <unistd.h>

#include "blobfs.hpp"
#include "bytes.h"
#include "clock.h"
#include "constants.h"
#include "print_util.h"
#include "spdk/blobfs.h"

std::string bdev_name;
std::string json_config_file;
std::string filename;

struct BlobFsSpdkCtx {
    static spdk_thread *app_thread;
    static spdk_filesystem *fs;
};
spdk_thread *BlobFsSpdkCtx::app_thread = nullptr;
spdk_filesystem *BlobFsSpdkCtx::fs = nullptr;
using BlobFs = BlobFsTemplate<BlobFsSpdkCtx>;

void read_args(int argc, char **argv) {
    argparse::ArgumentParser program("blobfs_async");
    
    program.add_argument("-b")
        .help("bdev name")
        .required();
    program.add_argument("-j")
        .help("json config file")
        .required();

    try {
        program.parse_args(argc, argv);
    }
    catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        std::cerr << program;
        std::exit(EXIT_FAILURE);
    }

    bdev_name = program.get<std::string>("-b");
    json_config_file = program.get<std::string>("-j");
    filename = TMP_FILE_NAME;
}

void write_bench(BlobFs &blobfs, const std::string_view &filename, size_t round) {
    auto f = blobfs.open(filename.data(), SPDK_BLOBFS_OPEN_CREATE);
    if (not f.has_value()) {
        throw std::runtime_error("failed to open " + std::string(filename));
    }

    BytesBuffer buf(BUF_SIZE);
    size_t write_offset{};

    for (size_t i = 1; i <= round; ++i) {
        while (write_offset < LIMIT_SIZE * i) {
            if (blobfs.write(*f, buf, write_offset) < 0) {
                throw std::runtime_error("write error");
            }
            write_offset += BUF_SIZE;
        }

        if (blobfs.fsync(*f) < 0) {
            throw std::runtime_error("fdatasync error");
        }
    }

    if (blobfs.close(*f) < 0) {
        throw std::runtime_error("close error");
    }
}

int main(int argc, char **argv) {
    read_args(argc, argv);

    BlobFs::set_cache_size(BLOBFS_CACHE_SIZE_MB);
    BlobFs blobfs(bdev_name, "blobfs", json_config_file);

    auto time = clock([&] {
        write_bench(blobfs, TMP_FILE_NAME, ROUND);
    });
    using namespace std::chrono;
    std::cout << "blobfs sync write bench: " 
        << duration_str(duration_cast<Duration>(time)) << std::endl;

    if (blobfs.remove(filename) < 0) {
        throw std::runtime_error("remove error");
    }
    return 0;
}
