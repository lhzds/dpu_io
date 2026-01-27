add_requires("argparse")
add_requires("nlohmann_json", {alias = "json"})
add_packages("argparse", "json")

-- includes("io/xmake.lua")
includes("dma/xmake.lua")
includes("compaction/xmake.lua")
