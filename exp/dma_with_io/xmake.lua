target("dma_with_io_host")
    set_kind("binary")
    add_files("dma_with_io_host.cpp")
    add_deps("docacxx")

target("dma_with_io_dpu")
    set_kind("binary")
    add_files("dma_with_io_dpu.cpp")
    add_deps("docacxx")
