target("bench_dma_host")
    set_kind("binary")
    add_files("host.cpp")
    add_deps("docacxx")

target("bench_dma_dpu")
    set_kind("binary")
    add_files("dpu.cpp")
    add_deps("docacxx")
