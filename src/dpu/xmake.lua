target("backend")
    set_kind("static")
    add_files("*.cpp", "../sst_pool/sst_pool.cpp")
    add_deps("docacxx")
    add_rules("utils.install.cmake_importfiles")
    add_rules("utils.install.pkgconfig_importfiles")
    