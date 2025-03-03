add_requires("gtest")

target("test_ring_buffer")
    set_kind("binary")
    add_files("test_ring_buffer.cpp")
    add_packages("gtest")

target("test_queue_dma")
    set_kind("binary")
    add_files("test_queue_dma.cpp")
    add_deps("docacxx")
    add_packages("gtest")

includes("comm/xmake.lua")
includes("doca_comch/xmake.lua")
