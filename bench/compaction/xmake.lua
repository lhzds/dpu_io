target("bench_compaction_host")
    set_kind("binary")
    add_files("host.cpp")
    add_deps("docacxx")
