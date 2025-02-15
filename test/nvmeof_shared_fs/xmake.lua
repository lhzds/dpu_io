target("create_pool")
    set_kind("binary")
    add_files("test/shared_pool/create_pool.cpp")

target("write_test")
    set_kind("binary")
    add_files("test/shared_pool/write_test.cpp")

target("read_test")
    set_kind("binary")
    add_files("test/shared_pool/read_test.cpp")
