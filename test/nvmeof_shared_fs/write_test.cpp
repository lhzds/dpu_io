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

    int fd = open(filename.c_str(), O_RDWR);
    if (fd < 0) {
        std::cerr << "failed to open " << filename << ": " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    std::string write_content = make_content(kTestFileId);
    if (int ret = write(fd, write_content.c_str(), write_content.size()); ret < 0) {
        std::cerr << filename << " write error" << ": " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    if (int ret = fsync(fd); ret < 0) {
        std::cerr << filename << " fsync error" << ": " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    close(fd);
    return 0;
}
