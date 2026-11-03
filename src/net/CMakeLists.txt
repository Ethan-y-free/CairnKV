add_library(cairnkv_net STATIC
    buffer.cc
    socket_utils.cc
    channel.cc
    event_loop.cc
    event_loop_thread_pool.cc
    acceptor.cc
    connection.cc
    tcp_server.cc)

target_include_directories(cairnkv_net PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src)

target_link_libraries(cairnkv_net PUBLIC
    cairnkv_common
    Threads::Threads)
