cmake_minimum_required(VERSION 2.8)
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

file(GLOB UTIL_TEST_SOURCES *.c*)
add_executable(utils-tests ${UTIL_TEST_SOURCES})
set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${LEVELDB_LIBRARY_DIRS})
target_link_libraries(utils-tests hxhim.a gtest gtest_main ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m)
