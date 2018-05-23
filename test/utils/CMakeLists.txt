cmake_minimum_required(VERSION 2.8)
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

file(GLOB MDHIM_TEST_SOURCES *.c*)
add_executable(mdhim-utils-tests ${MDHIM_TEST_SOURCES})
set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${LEVELDB_LIBRARY_DIRS})
target_link_libraries(mdhim-utils-tests utils.a gtest gtest_main)
