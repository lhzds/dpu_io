target("dma_host")
    set_kind("binary")
    add_files("dma_host.cpp")
    add_deps("docacxx")

target("dma_dpu")
    set_kind("binary")
    add_files("dma_dpu.cpp")
    add_deps("docacxx")
