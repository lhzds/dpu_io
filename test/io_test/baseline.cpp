#include <argparse/argparse.hpp>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fmt/format.h>
#include <iostream>
#include <string>
#include <sys/ioctl.h>
#include <unistd.h>

#include "bytes.h"
#include "clock.h"
#include "constants.h"
#include "print_util.h"

std::string test_dir;
std::vector<std::string> filenames;

void read_args(int argc, char **argv) {
    argparse::ArgumentParser program("baseline");
    
    program.add_argument("-d")
        .help("test dir")
        .required();

    try {
        program.parse_args(argc, argv);
    }
    catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        std::cerr << program;
        std::exit(EXIT_FAILURE);
    }

    test_dir = program.get<std::string>("-d");
    if (not test_dir.ends_with('/')) {
        test_dir.push_back('/');
    }
}

void write_bench() {
    BytesBuffer buf(WRITE_BUF_SIZE);

    for (size_t i = 0; i < FILE_NUM; ++i) {
        int fd = open(filenames[i].data(), O_WRONLY|O_CREAT|O_TRUNC, 0644);
        if (fd < 0) {
            throw_with("failed to open {}: {}", filenames[i], strerror(errno));
        }

        for (size_t j = 0; j < ROUND; ++j) {
            for (size_t k = 0; k < WRITE_BUF_NUM_PER_ROUND; ++k) {
                if (write(fd, buf.data(), WRITE_BUF_SIZE) < 0) {
                    throw_with("write error: {}", strerror(errno));
                }
            }

            if (fdatasync(fd) < 0) {
                throw_with("fdatasync error: {}", strerror(errno));
            }   
        }
    
        if (close(fd) < 0) {
            throw_with("close error: {}", strerror(errno));
        }
    }
}

int main(int argc, char **argv) {
    read_args(argc, argv);

    for (size_t i{}; i < ROUND; ++i) {
        filenames.emplace_back(test_dir + TMP_FILE_PREFIX + std::to_string(i));
    }

    auto time = clock([&] {
        write_bench();
    });
    using namespace std::chrono;
    std::cout << "baseline write bench: " 
        << duration_str(duration_cast<Duration>(time)) << std::endl;
    
    for (const auto &f : filenames) {
        if (remove(f.data()) < 0) {
            throw_with("unlink error: {}", strerror(errno));
        }
    }
    return 0;
}
