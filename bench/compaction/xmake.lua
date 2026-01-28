target("bench_compaction_host")
    set_kind("binary")
    add_files("compaction_host.cpp")
    add_deps("docacxx", "frontend")
