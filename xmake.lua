add_rules("mode.debug", "mode.release")
set_languages("c++20")

if is_mode("debug") then
    add_defines("SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG")
end
add_defines("SPDLOG_FMT_EXTERNAL")

add_requires("fmt 9.1.0", "spdlog", "boost", "argparse")
add_packages("fmt", "spdlog", "boost", "argparse")

add_ldflags("-luring")

add_includedirs("include")

includes("deps/docacxx/xmake.lua")
includes("src/host/xmake.lua")
includes("src/dpu/xmake.lua")
includes("src/sst_pool/xmake.lua")

includes("exp/xmake.lua")
includes("test/xmake.lua")
