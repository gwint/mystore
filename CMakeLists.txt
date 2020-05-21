include(ExternalProject)

cmake_minimum_required(VERSION 3.10)

project(MyStore VERSION 1.0)
set(CMAKE_CXX_FLAGS "-Wall -pedantic -g")

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

ExternalProject_Add(cpp-dotenv
    PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/cpp-dotenv
    GIT_REPOSITORY https://github.com/adeharo9/cpp-dotenv.git
    INSTALL_COMMAND ""
)

ExternalProject_Add(spdlog
    PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG v1.6.0
    INSTALL_COMMAND ""
)

execute_process(
    COMMAND thrift -r --gen cpp replicaservice.thrift
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/
)

set(ThriftGeneratedFiles
    /home/gregory/distributed-key-value-store/gen-cpp/ReplicaService.cpp
    /home/gregory/distributed-key-value-store/gen-cpp/replicaservice_types.cpp
    /home/gregory/distributed-key-value-store/gen-cpp/replicaservice_constants.cpp
)

set(ServerFiles replica.cpp lockhandler.cpp replica.hpp lockhandler.hpp)

add_executable(MyStore ${ThriftGeneratedFiles} ${ServerFiles})

add_dependencies(MyStore spdlog)
add_dependencies(MyStore cpp-dotenv)

target_include_directories(MyStore PRIVATE spdlog/src/spdlog/include/)
target_include_directories(MyStore PRIVATE cpp-dotenv/src/cpp-dotenv/)

target_link_libraries(MyStore thrift)
target_link_libraries(MyStore pthread)
