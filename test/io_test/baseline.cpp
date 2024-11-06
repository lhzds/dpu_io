#include <argparse/argparse.hpp>
#include <chrono>
#include <cstdlib>
#include <fcntl.h>
#include <fmt/format.h>
#include <iostream>
#include <sys/ioctl.h>
#include <unistd.h>

#include "bytes.h"
#include "clock.h"
#include "constants.h"
#include "print_util.h"

std::string test_dir;
std::string filename;

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
    filename = test_dir + TMP_FILE_NAME;
}

void write_bench(size_t round) {
    int fd = open(filename.data(), O_WRONLY|O_CREAT|O_TRUNC, 0644);
    if (fd < 0) {
        throw std::runtime_error(fmt::format("failed to open {}: {}", filename, strerror(errno)));
    }

    BytesBuffer buf(BUF_SIZE);
    size_t write_offset{};

    for (size_t i = 1; i <= round; ++i) {
        while (write_offset < LIMIT_SIZE * i) {
            if (write(fd, buf.data(), BUF_SIZE) < 0) {
                throw std::runtime_error("write error");
            }
            write_offset += BUF_SIZE;
        }

        if (fdatasync(fd) < 0) {
            throw std::runtime_error("fdatasync error");
        }
    }

    if (close(fd) < 0) {
        throw std::runtime_error("close error");
    }
}

int main(int argc, char **argv) {
    read_args(argc, argv);

    auto time = clock([&] {
        write_bench(ROUND);
    });
    using namespace std::chrono;
    std::cout << "baseline write bench: " 
        << duration_str(duration_cast<Duration>(time)) << std::endl;
    
    if (remove(filename.data()) < 0) {
        throw std::runtime_error("unlink error");
    }
    return 0;
}
