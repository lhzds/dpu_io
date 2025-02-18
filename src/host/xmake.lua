target("frontend")
    set_kind("static")
    add_files("frontend_qp.cpp", "frontend.cpp")
    add_deps("docacxx")
    
target("create_sst_pool")
    set_kind("binary")
    add_files("create_sst_pool.cpp", "../sst_pool.cpp")
