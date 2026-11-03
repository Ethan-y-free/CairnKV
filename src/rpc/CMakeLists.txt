add_library(cairnkv_protocol STATIC
    frame.cc
    message.cc)

target_include_directories(cairnkv_protocol PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src)

target_link_libraries(cairnkv_protocol PUBLIC cairnkv_common)
