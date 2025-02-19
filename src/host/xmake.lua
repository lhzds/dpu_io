target("frontend")
    set_kind("static")
    add_files("frontend_qp.cpp", "frontend.cpp")
    add_deps("docacxx")
