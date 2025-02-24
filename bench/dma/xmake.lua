target("bench_dma_host")
    set_kind("binary")
    add_files("bench_host.cpp")
    add_deps("docacxx")

target("bench_dma_dpu")
    set_kind("binary")
    add_files("bench_dpu.cpp")
    add_deps("docacxx")
