cmake_minimum_required(VERSION 3.15)
project(rdma_test)

set(CMAKE_CXX_STANDARD 20)
add_executable(client client.cpp)
add_executable(server server.cpp)
target_link_libraries(client pthread rdmacm ibverbs)
target_link_libraries(server pthread rdmacm ibverbs)
