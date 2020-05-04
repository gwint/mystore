cmake_minimum_required(VERSION 3.10)

project(MyStore VERSION 1.0)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_executable(MyStore replica.cpp lockhandler.cpp)
