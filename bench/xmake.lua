add_requires("argparse")
add_requires("nlohmann_json", {alias = "json"})
add_packages("argparse", "json")

-- includes("io/xmake.lua")
includes("comch_lat/xmake.lua")
includes("compaction/xmake.lua")
includes("uring/xmake.lua")
