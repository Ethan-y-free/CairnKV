add_executable(encoding_test encoding_test.cc)
target_link_libraries(encoding_test PRIVATE
    cairnkv_protocol
    cairnkv_common
    GTest::gtest
    GTest::gtest_main)
add_test(NAME encoding_test COMMAND encoding_test)

add_executable(wal_test wal_test.cc)
target_link_libraries(wal_test PRIVATE
    cairnkv_raft
    GTest::gtest
    GTest::gtest_main)
add_test(NAME wal_test COMMAND wal_test)

add_executable(hard_state_test hard_state_test.cc)
target_link_libraries(hard_state_test PRIVATE
    cairnkv_raft
    GTest::gtest
    GTest::gtest_main)
add_test(NAME hard_state_test COMMAND hard_state_test)

add_executable(state_machine_test state_machine_test.cc)
target_link_libraries(state_machine_test PRIVATE
    cairnkv_storage
    GTest::gtest
    GTest::gtest_main)
add_test(NAME state_machine_test COMMAND state_machine_test)

add_executable(kv_service_test kv_service_test.cc)
target_link_libraries(kv_service_test PRIVATE
    cairnkv_service
    GTest::gtest
    GTest::gtest_main)
add_test(NAME kv_service_test COMMAND kv_service_test)

add_executable(raft_message_test raft_message_test.cc)
target_link_libraries(raft_message_test PRIVATE
    cairnkv_raft
    GTest::gtest
    GTest::gtest_main)
add_test(NAME raft_message_test COMMAND raft_message_test)

add_executable(raft_node_test raft_node_test.cc)
target_link_libraries(raft_node_test PRIVATE
    cairnkv_consensus
    GTest::gtest
    GTest::gtest_main)
add_test(NAME raft_node_test COMMAND raft_node_test)
