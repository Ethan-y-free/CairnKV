add_library(cairnkv_storage STATIC
    state_machine.cc)

target_include_directories(cairnkv_storage PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src)

target_link_libraries(cairnkv_storage PUBLIC
    cairnkv_raft
    rocksdb::rocksdb)
