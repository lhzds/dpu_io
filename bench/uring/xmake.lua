target("bench_uring")
    set_kind("binary")
    add_files("uring_bench.cpp", "../../src/dpu/sst_pool.cpp")
