#include <argparse/argparse.hpp>
#include <chrono>
#include <cstdlib>
#include <fcntl.h>
#include <iostream>
#include <queue>
#include <sys/ioctl.h>
#include <unistd.h>

#include "bytes.h"
#include "config.h"
#include "io/blobfs.hpp"
#include "test_util/clock.h"
#include "test_util/print_format.h"

struct BlobFsSpdkCtx {
    static spdk_thread *app_thread;
    static spdk_filesystem *fs;
};
spdk_thread *BlobFsSpdkCtx::app_thread = nullptr;
spdk_filesystem *BlobFsSpdkCtx::fs = nullptr;
using BlobFs = BlobFsTemplate<BlobFsSpdkCtx>;

std::string bdev_name;
std::string json_config_file;
std::vector<std::string> filenames;
std::vector<BlobFs::Handle> handles;

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
}

void write_bench(BlobFs &blobfs) {
    BytesBuffer buf(WRITE_BUF_SIZE);
    std::atomic<size_t> fsync_req_cnt{};

    for (size_t i = 0; i < FILE_NUM; ++i) {
        auto f = blobfs.open(filenames[i].data(), SPDK_BLOBFS_OPEN_CREATE);
        if (not f.has_value()) {
            throw_with("failed to open " + std::string(filenames[i]));
        }
        handles.emplace_back(*f);

        size_t offset{};
        for (size_t j = 0; j < ROUND; ++j) {
            std::atomic<size_t> write_req_cnt{};
            for (size_t k = 0; k < WRITE_BUF_NUM_PER_ROUND; ++k) {
                ++write_req_cnt;
                blobfs.write_async(*f, buf, offset, [&] (int fserrno) {
                    if (fserrno < 0) {
                        throw_with("write error: {}", fserrno);
                    }
                    --write_req_cnt;
                });
                offset += WRITE_BUF_SIZE;
            }
            while (write_req_cnt > 0);

            ++fsync_req_cnt;
            blobfs.fsync_async(*f, [&] (int fserrno) {
                if (fserrno < 0) {
                    throw_with("fsync error: {}", fserrno);
                }
                --fsync_req_cnt;
            });
        }
    }

    while (fsync_req_cnt > 0);
    for (auto &h : handles) {
        if (auto ret = blobfs.close(h); ret < 0) {
            throw_with("close error: {}", ret);
        }
    }
}

int main(int argc, char **argv) {
    read_args(argc, argv);
    handles.reserve(FILE_NUM);

    for (size_t i{}; i < ROUND; ++i) {
        filenames.emplace_back(TMP_FILE_PREFIX + std::to_string(i));
    }

    BlobFs::set_cache_size(BLOBFS_CACHE_SIZE_MB);
    BlobFs blobfs(bdev_name, "blobfs", json_config_file);
    
    auto time = clock([&] {
        write_bench(blobfs);
    });
    using namespace std::chrono;
    std::cout << "blobfs async write bench: " 
        << duration_str(duration_cast<Duration>(time)) << std::endl;

    return 0;
}
