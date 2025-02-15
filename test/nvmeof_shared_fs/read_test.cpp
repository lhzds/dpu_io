#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

#include "common.h"

int main() {
    std::string filename = kFileBaseName + std::to_string(kTestFileId);

    int fd = open(filename.c_str(), O_RDWR | O_DIRECT);
    if (fd < 0) {
        std::cerr << "failed to open " << filename << ": " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }


    void *buffer;
    if (posix_memalign(&buffer, 4096, kFileSize) != 0) {
        std::cerr << "Failed to allocate aligned memory" << std::endl;
        exit(EXIT_FAILURE);
    }
    if (int ret = read(fd, buffer, kFileSize); ret < 0) {
        std::cerr << filename << " read error" << ": " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    std::string_view read_content((char *)buffer, kFileSize);
    std::cout << "content: " << read_content.substr(0, 10) << std::endl;
    auto test_content = make_content(kTestFileId);
    if (test_content != read_content) {
        std::cerr << "content is not expected" << std::endl;
        exit(EXIT_FAILURE);
    }

    close(fd);
    return 0;
}
