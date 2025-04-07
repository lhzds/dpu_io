target("backend_run")
    set_kind("binary")
    add_files("backend_run.cpp")
    add_deps("docacxx", "backend")
