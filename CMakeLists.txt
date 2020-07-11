include(ExternalProject)

cmake_minimum_required(VERSION 3.10)

project(MyStore VERSION 1.0)
set(CMAKE_CXX_FLAGS "-Wall -pedantic -g -O3")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(ServerFiles replica.cpp lockhandler.cpp)
set(TestFiles tests/test_replica.cpp)
set(HelperFiles replica.cpp lockhandler.cpp)

configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)

execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download)
if(result)
  message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
  message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/googletest-src
                 ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
                 EXCLUDE_FROM_ALL)

# The gtest/gtest_main targets carry header search path
# dependencies automatically when using CMake 2.8.11 or
# later. Otherwise we have to add them here ourselves.
if (CMAKE_VERSION VERSION_LESS 2.8.11)
  include_directories("${gtest_SOURCE_DIR}/include")
endif()

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

ExternalProject_Add(thrift-file
    PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/thriftfile
    GIT_REPOSITORY https://github.com/gwint/mystore-thrift-file.git
    INSTALL_COMMAND ""
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
)

ExternalProject_Add(argparse
    PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/argparse
    GIT_REPOSITORY https://github.com/hbristow/argparse.git
    INSTALL_COMMAND ""
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
)

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp
           ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/ReplicaService.cpp
           ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/replicaservice_types.cpp
    COMMAND thrift -r --gen cpp ${CMAKE_CURRENT_SOURCE_DIR}/thriftfile/src/thrift-file/replicaservice.thrift
    COMMAND thrift -r --gen py ${CMAKE_CURRENT_SOURCE_DIR}/thriftfile/src/thrift-file/replicaservice.thrift
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/thriftfile/src/thrift-file/replicaservice.thrift
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_executable(MyStore
    ${ServerFiles}
)

add_executable(MyStoreTests
    ${TestFiles}
)

add_library(ThriftFileCompilation
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/ReplicaService.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/replicaservice_types.cpp
)

add_library(HelperFiles
    ${HelperFiles}
)

add_dependencies(ThriftFileCompilation thrift-file)
add_dependencies(MyStore ThriftFileCompilation)
add_dependencies(MyStore spdlog)
add_dependencies(MyStore cpp-dotenv)
add_dependencies(MyStore argparse)

add_dependencies(MyStoreTests ThriftFileCompilation)
add_dependencies(MyStoreTests spdlog)
add_dependencies(MyStoreTests cpp-dotenv)
add_dependencies(MyStoreTests argparse)

add_dependencies(HelperFiles spdlog)
add_dependencies(HelperFiles cpp-dotenv)
add_dependencies(HelperFiles argparse)

target_include_directories(MyStore PRIVATE gen-cpp/)
target_include_directories(MyStore PRIVATE spdlog/src/spdlog/include/)
target_include_directories(MyStore PRIVATE cpp-dotenv/src/cpp-dotenv/)
target_include_directories(MyStore PRIVATE argparse/src/argparse/)

target_include_directories(HelperFiles PRIVATE spdlog/src/spdlog/include/)
target_include_directories(HelperFiles PRIVATE cpp-dotenv/src/cpp-dotenv/)
target_include_directories(HelperFiles PRIVATE argparse/src/argparse/)

target_include_directories(MyStoreTests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/)

target_link_libraries(MyStore thrift)
target_link_libraries(MyStore pthread)
target_link_libraries(MyStore ThriftFileCompilation)

target_link_libraries(MyStoreTests gtest_main)
target_link_libraries(MyStoreTests thrift)
target_link_libraries(MyStoreTests pthread)
target_link_libraries(MyStoreTests ThriftFileCompilation)

target_link_libraries(MyStoreTests HelperFiles)
