find_package(Threads REQUIRED)
include_directories(${Threads_INCLUDE_DIR})

# GTest
find_package(GTest)

if (NOT GTEST_FOUND)
	find_path(GTEST_SRC_DIR src/gtest.cc HINTS /usr/src/gtest)
	find_path(GTEST_INCLUDE_DIRS gtest.h HINTS /usr/include/gtest)
	if (GTEST_SRC_DIR AND GTEST_INCLUDE_DIRS)
		message(STATUS "GTest source package found. Compiling...")
		add_subdirectory(${GTEST_SRC_DIR} ${CMAKE_BINARY_DIR}/gtest)

		set(GTEST_FOUND 1)
		set(GTEST_MAIN_LIBRARIES ${CMAKE_BINARY_DIR}/gtest/libgtest_main.a)
		set(GTEST_LIBRARIES ${CMAKE_BINARY_DIR}/gtest/libgtest.a)
		set(GTEST_BOTH_LIBRARIES ${GTEST_LIBRARIES} ${GTEST_MAIN_LIBRARIES})
	else (GTEST_SRC_DIR AND GTEST_INCLUDE_DIRS)
		message(SEND_ERROR "Cannot find required package: GTest")
	endif (GTEST_SRC_DIR AND GTEST_INCLUDE_DIRS)
else (NOT GTEST_FOUND)
endif (NOT GTEST_FOUND)

include_directories(${GTEST_INCLUDE_DIRS})
set(GTEST_TARGETS gtest gtest_main)

# Touch test CMakeLists to update tests sources
add_custom_target(check_tests ALL touch ${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt)

# Show test printouts
set(CMAKE_CTEST_COMMAND ctest --output-on-failure)
add_custom_target(run_tests ${CMAKE_CTEST_COMMAND})

#set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "bin/")

include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR} ../../src/collector)

set(COLLECTOR_LIBS
        ${elliptics_cpp_LIBRARY} ${elliptics_client_LIBRARY}
        ${CocaineNative_LIBRARIES} ${Cocaine_LIBRARY}
        ${CURL_LIBRARIES} boost_system boost_thread msgpack rt)

# Link collector sources into archive
add_library(collector STATIC
    ../../src/collector/Backend.cpp
    ../../src/collector/BackendParser.cpp
    ../../src/collector/CocaineHandlers.cpp
    ../../src/collector/ConfigParser.cpp
    ../../src/collector/Couple.cpp
    ../../src/collector/Discovery.cpp
    ../../src/collector/DiscoveryTimer.cpp
    ../../src/collector/Filter.cpp
    ../../src/collector/FilterParser.cpp
    ../../src/collector/FS.cpp
    ../../src/collector/Group.cpp
    ../../src/collector/Metrics.cpp
    ../../src/collector/Namespace.cpp
    ../../src/collector/Node.cpp
    ../../src/collector/Parser.cpp
    ../../src/collector/ProcfsParser.cpp
    ../../src/collector/Storage.cpp
    ../../src/collector/ThreadPool.cpp
    ../../src/collector/WorkerApplication.cpp)
target_link_libraries(collector ${COLLECTOR_LIBS})

# Get all test source files
file(GLOB_RECURSE test_src_files RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.test.cpp")

foreach(test_src_file IN LISTS test_src_files)
    string(REGEX REPLACE "[.]test[.]cpp$" "_test" test_exec_file ${test_src_file})

    add_executable(${test_exec_file} ${test_src_file})

    target_link_libraries(${test_exec_file} ${CMAKE_THREAD_LIBS_INIT}
        ${GTEST_BOTH_LIBRARIES} ${COLLECTOR_LIBS} collector)
    add_dependencies(${test_exec_file} ${GTEST_TARGETS})

    add_test(${test_exec_file} ${test_exec_file})
    list(APPEND test_list ${test_exec_file})
endforeach(test_src_file)

add_dependencies(run_tests ${test_list})

