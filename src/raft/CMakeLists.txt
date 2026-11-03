add_library(cairnkv_raft STATIC
    log_entry.cc
    log_store.cc
    hard_state.cc
    raft_message.cc)

target_include_directories(cairnkv_raft PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src)

target_link_libraries(cairnkv_raft PUBLIC cairnkv_common)
