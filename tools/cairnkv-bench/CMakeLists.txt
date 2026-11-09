add_executable(cairnkv-bench
    main.cc
    bench.cc)

target_link_libraries(cairnkv-bench PRIVATE cairnkv_client)
