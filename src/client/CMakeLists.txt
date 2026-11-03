add_library(cairnkv_client STATIC
    client.cc)

target_include_directories(cairnkv_client PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src)

target_link_libraries(cairnkv_client PUBLIC
    cairnkv_protocol
    cairnkv_common
    Threads::Threads)
