add_library(cairnkv_service STATIC
    kv_service.cc)

target_include_directories(cairnkv_service PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src)

target_link_libraries(cairnkv_service PUBLIC
    cairnkv_raft
    cairnkv_storage
    cairnkv_common)
