target("test_dpu_server")
    set_kind("binary")
    add_files("test_dpu_server.cpp")
    add_deps("docacxx")

target("test_host_client")
    set_kind("binary")
    add_files("test_host_client.cpp")
    add_deps("docacxx")
