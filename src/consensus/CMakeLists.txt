add_library(cairnkv_consensus STATIC
    ${CMAKE_SOURCE_DIR}/src/raft/raft_node.cc
    ${CMAKE_SOURCE_DIR}/src/raft/peer_client.cc)

target_include_directories(cairnkv_consensus PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src)

target_link_libraries(cairnkv_consensus PUBLIC
    cairnkv_raft
    cairnkv_storage
    cairnkv_protocol
    cairnkv_net
    cairnkv_common
    Threads::Threads)

add_library(cairnkv_rpc_server STATIC
    rpc_server.cc
    request_pool.cc)

target_include_directories(cairnkv_rpc_server PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src)

target_link_libraries(cairnkv_rpc_server PUBLIC
    cairnkv_consensus
    cairnkv_net
    cairnkv_protocol)
