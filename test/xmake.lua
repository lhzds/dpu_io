add_requires("gtest")

target("ring_buffer_test")
    set_kind("binary")
    add_files("ring_buffer_test.cpp")
    add_packages("gtest")

target("queue_dma_test")
    set_kind("binary")
    add_files("queue_dma_test.cpp")
    add_deps("docacxx")
    add_packages("gtest")

target("format_test")
    set_kind("binary")
    add_files("format_test.cpp")
    add_packages("gtest")
