add_includedirs("include")

target("backend")
    set_kind("static")
    add_files("*.cpp")
    add_deps("docacxx")
    