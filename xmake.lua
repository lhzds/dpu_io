add_rules("mode.debug", "mode.release")
set_languages("c++20")

if is_mode("debug") then
    add_defines("SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG")
end
add_defines("SPDLOG_FMT_EXTERNAL")

add_requires("fmt 9.1.0", "spdlog", "boost")
add_packages("fmt", "spdlog", "boost")

add_ldflags("-luring")

add_includedirs("include")

includes("deps/docacxx/xmake.lua")
includes("host/xmake.lua")
includes("dpu/xmake.lua")

includes("exp/xmake.lua")
includes("test/xmake.lua")
