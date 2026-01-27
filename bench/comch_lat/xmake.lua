target("bench_comch_lat_host")
    set_kind("binary")
    add_files("comch_lat_host.cpp")
    add_deps("docacxx")

target("bench_comch_lat_dpu")
    set_kind("binary")
    add_files("comch_lat_dpu.cpp")
    add_deps("docacxx")
