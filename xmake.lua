set_version("1.0.0")
add_rules("mode.debug", "mode.release")
set_languages("c++20")

if is_mode("releasedbg") then
    set_symbols("debug")
    set_optimize("faster")
    add_defines("NDEBUG")
    add_cxxflags("-fno-omit-frame-pointer")
end

if not is_mode("debug") then
    add_defines("SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_OFF")
end
add_defines("SPDLOG_FMT_EXTERNAL")

add_requires("fmt 9.1.0", "spdlog 1.12.0", "boost", "argparse")
add_packages("spdlog", "fmt", "argparse")
add_packages("boost", {links = {}})

add_ldflags("-luring")

add_includedirs("include")

includes("deps/docacxx/xmake.lua")
includes("src/host/xmake.lua")
includes("src/dpu/xmake.lua")
includes("scripts/host/xmake.lua")
includes("scripts/dpu/xmake.lua")

includes("bench/xmake.lua")
includes("test/xmake.lua")
