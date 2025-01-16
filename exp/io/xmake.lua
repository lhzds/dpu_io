target("baseline")
    set_kind("binary")
    add_files("baseline.cpp")
    
target("blobfs_async")
    set_kind("binary")
    add_files("blobfs_async.cpp")
    before_build(link_spdk)

target("blobfs_sync")
    set_kind("binary")
    add_files("blobfs_sync.cpp")
    before_build(link_spdk)
