cmake_minimum_required(VERSION 3.20)

project(CairnKV VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(CAIRNKV_BUILD_SERVER "Build cairnkd" ON)
option(CAIRNKV_BUILD_CLIENT "Build libcairnkv" ON)
option(CAIRNKV_BUILD_TOOLS "Build cairnctl" ON)
option(CAIRNKV_BUILD_BENCH "Build cairnkv-bench" ON)
option(CAIRNKV_BUILD_TESTS "Build tests" ON)

find_package(Threads REQUIRED)

find_path(CAIRNKV_ROCKSDB_INCLUDE_DIR rocksdb/db.h)
find_library(CAIRNKV_ROCKSDB_LIBRARY NAMES rocksdb)
if (NOT CAIRNKV_ROCKSDB_INCLUDE_DIR OR NOT CAIRNKV_ROCKSDB_LIBRARY)
    message(FATAL_ERROR "RocksDB not found. Install librocksdb-dev or set the paths.")
endif()

add_library(rocksdb::rocksdb UNKNOWN IMPORTED)
set_target_properties(rocksdb::rocksdb PROPERTIES
    IMPORTED_LOCATION "${CAIRNKV_ROCKSDB_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${CAIRNKV_ROCKSDB_INCLUDE_DIR}")

add_compile_definitions(_GNU_SOURCE)
add_compile_options(-Wall -Wextra -Wpedantic)

add_subdirectory(src/common)
add_subdirectory(src/raft)
add_subdirectory(src/storage)
add_subdirectory(src/server)
add_subdirectory(src/net)
add_subdirectory(src/rpc)
add_subdirectory(src/consensus)
add_subdirectory(src/client)
add_subdirectory(tools/cairnctl)

if (CAIRNKV_BUILD_BENCH)
    add_subdirectory(tools/cairnkv-bench)
endif()

if (CAIRNKV_BUILD_SERVER)
    add_executable(cairnkd src/server/node_main.cc)
    target_link_libraries(cairnkd PRIVATE cairnkv_rpc_server)
endif()

if (CAIRNKV_BUILD_TESTS)
    enable_testing()
    find_package(GTest REQUIRED)
    add_subdirectory(tests)
endif()
