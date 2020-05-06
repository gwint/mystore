cmake_minimum_required(VERSION 3.10)

project(MyStore VERSION 1.0)
set(CMAKE_CXX_FLAGS "-Wall -pedantic")

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(ThriftGeneratedFiles
    /home/gregory/distributed-key-value-store/gen-cpp/ReplicaService.cpp
    /home/gregory/distributed-key-value-store/gen-cpp/replicaservice_types.cpp
    /home/gregory/distributed-key-value-store/gen-cpp/replicaservice_constants.cpp
)

set(ServerFiles replica.cpp lockhandler.cpp)

add_executable(MyStore ${ThriftGeneratedFiles} ${ServerFiles})

target_link_libraries(MyStore thrift)

