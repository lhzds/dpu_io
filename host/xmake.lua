add_includedirs("include")

target("frontend")
    set_kind("static")
    add_files("*.cpp")
    add_deps("docacxx")
    