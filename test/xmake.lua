add_includedirs("util")

target("ring_buffer_test")
    set_kind("binary")
    add_files("ring_buffer_test.cpp")

includes("io_test/xmake.lua")