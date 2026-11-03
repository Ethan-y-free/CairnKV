add_library(cairnkv_common STATIC
    status.cc
    encoding.cc
    logging.cc)

target_include_directories(cairnkv_common PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src)

target_link_libraries(cairnkv_common PUBLIC Threads::Threads)
