cmake_minimum_required(VERSION 2.6)
project(mastermind)

add_definitions("-std=c++0x -Wall -O2 -g")

find_package(Boost REQUIRED system program_options)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake")

find_package(elliptics REQUIRED)
find_package(CocaineNative REQUIRED)
find_package(Cocaine REQUIRED)
find_package(RapidJSON REQUIRED)
find_package(CURL REQUIRED)

include(cmake/locate_library.cmake)
locate_library(MSGPACK "msgpack.hpp" "msgpack")

include_directories(${elliptics_INCLUDE_DIRS} ${CocaineNative_INCLUDE_DIRS}
    ${RAPIDJSON_INCLUDE_DIRS} ${CURL_INCLUDE_DIRS})

add_subdirectory(src/collector)

# enable_testing()
# add_subdirectory(tests/collector)

