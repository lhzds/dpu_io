add_rules("mode.debug", "mode.release")
set_languages("c++20")

if is_mode("debug") then
    add_defines("SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG")
end

add_requires("fmt 9.1.0", "spdlog", "boost")
add_packages("fmt", "spdlog", "boost")

-- function link_spdk(target)
--     local spdk_root_dir = path.join(os.projectdir(), "deps/spdk/build")
--     local pkgconfig_path = path.join(spdk_root_dir, "lib/pkgconfig")

--     -- 添加静态库链接
--     local spdk_libs = {}
--     for _, file in ipairs(os.files(path.join(pkgconfig_path, "*.pc"))) do
--         local lib_name = path.filename(file):gsub("%.pc$", "")
--         if lib_name ~= "spdk_syslibs" and lib_name ~= "spdk_ut" and lib_name ~= "spdk_ut_mock" then
--             table.insert(spdk_libs, lib_name)
--         end
--     end
   
--     -- local spdk_libs = {"spdk_blobfs", "spdk_event", "spdk_blobfs_bdev", "spdk_env_dpdk"}
--     local spdk_lib_ldflags, _ = os.iorunv("pkg-config", {"--libs", table.unpack(spdk_libs)}, {envs={PKG_CONFIG_PATH=pkgconfig_path}})
    
--     target:add("ldflags", "-Wl,--whole-archive -Wl,-Bstatic", {force = true})
--     target:add("ldflags", string.split(spdk_lib_ldflags, " "), {force = true})
--     target:add("ldflags", "-Wl,-Bdynamic -Wl,--no-whole-archive", {force = true})

--     local sys_lib_ldflags, _ = os.iorunv("pkg-config", {"--libs", "--static", "spdk_syslibs"}, {envs={PKG_CONFIG_PATH=pkgconfig_path}})
--     target:add("ldflags", sys_lib_ldflags, {force = true})
--     target:add("ldflags", "-lnuma -lbsd", {force = true})
-- end

add_includedirs("include")
-- add_includedirs("deps/spdk/build/include")
-- add_includedirs("deps/spdk/dpdk/build/include")

includes("deps/docacxx/xmake.lua")
includes("host/xmake.lua")
includes("dpu/xmake.lua")

-- includes("exp/xmake.lua")
includes("test/xmake.lua")
