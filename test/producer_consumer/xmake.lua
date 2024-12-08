target("dpu_server")
    set_kind("binary")
    add_files("dpu_server.cpp")
    add_deps("docacxx")

target("host_client_test")
    set_kind("binary")
    add_files("host_client_test.cpp")
    add_deps("docacxx")
