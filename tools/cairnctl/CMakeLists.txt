add_executable(cairnctl main.cc)
target_link_libraries(cairnctl PRIVATE cairnkv_client)
