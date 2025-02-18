target("backend")
    set_kind("static")
    add_files("*.cpp", "../sst_pool.cpp")
    add_deps("docacxx")
    