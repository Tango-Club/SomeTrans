cmake_minimum_required(VERSION 3.17)
project(SomeTrans)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CAMKE_CXX_FLAGS} -O2 -pthread -fcoroutines")
add_executable(SomeTrans main.cpp SchemaReader.hpp)

