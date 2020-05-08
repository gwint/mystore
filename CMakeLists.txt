cmake_minimum_required(VERSION 3.10)

project(MyStore VERSION 1.0)
set(CMAKE_CXX_FLAGS "-Wall -pedantic -g")

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(ThriftGeneratedFiles
    /home/gregory/distributed-key-value-store/gen-cpp/ReplicaService.cpp
    /home/gregory/distributed-key-value-store/gen-cpp/replicaservice_types.cpp
    /home/gregory/distributed-key-value-store/gen-cpp/replicaservice_constants.cpp
)

set(ServerFiles replica.cpp lockhandler.cpp)

add_executable(MyStore ${ThriftGeneratedFiles} ${ServerFiles})

target_include_directories(MyStore PRIVATE spdlog/include/)
target_include_directories(MyStore PRIVATE cpp-dotenv/)

target_link_libraries(MyStore thrift)
target_link_libraries(MyStore pthread)
