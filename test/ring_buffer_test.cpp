#include <cstdlib>
#include <iostream>
#include <string>

#include "bytes.h"
#include "ring_buffer.h"

int main(int argc, char** argv) {
    RingBuffer ring_buf(1 << 7);

    for (size_t i = 0; i < 1024; ++i) {
        std::string str1 = std::to_string(i);
        for (size_t j = 0; j < 2; ++j) {
            ring_buf.write(str1);
        }
        
        std::string str2;
        for (size_t j = 0; j < 2; ++j) {
            str2.resize(str1.size());
            ring_buf.read(str2);

            if (str1 != str2) {
                std::cerr << "Test failed." << std::endl;
                exit(EXIT_FAILURE);
            }
        }
    }

    std::cout << "Test passes." << std::endl;
    return 0;
}
