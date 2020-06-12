include(ExternalProject)

cmake_minimum_required(VERSION 3.10)

project(MyStore VERSION 1.0)
set(CMAKE_CXX_FLAGS "-Wall -pedantic -g -O3")

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

set(ServerFiles replica.cpp lockhandler.cpp replica.hpp lockhandler.hpp)

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp
           ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/ReplicaService.cpp
           ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/replicaservice_types.cpp
           ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/replicaservice_constants.cpp
    COMMAND thrift -r --gen cpp ${CMAKE_CURRENT_SOURCE_DIR}/replicaservice.thrift
    COMMAND thrift -r --gen py ${CMAKE_CURRENT_SOURCE_DIR}/replicaservice.thrift
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/replicaservice.thrift
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_executable(MyStore
    ${ServerFiles}
)

add_library(ThriftFileCompilation
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/ReplicaService.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/replicaservice_types.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/replicaservice_constants.cpp
)

add_dependencies(MyStore ThriftFileCompilation)
add_dependencies(MyStore spdlog)
add_dependencies(MyStore cpp-dotenv)

target_include_directories(MyStore PRIVATE gen-cpp/)
target_include_directories(MyStore PRIVATE spdlog/src/spdlog/include/)
target_include_directories(MyStore PRIVATE cpp-dotenv/src/cpp-dotenv/)

target_link_libraries(MyStore thrift)
target_link_libraries(MyStore pthread)
target_link_libraries(MyStore ThriftFileCompilation)
