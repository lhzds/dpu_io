target("bench_uring_host")
    set_kind("binary")
    add_files("uring_bench.cpp", "../../src/dpu/sst_pool.cpp")
