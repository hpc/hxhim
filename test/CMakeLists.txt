#
# CMakeLists file for HXHIM tests
#

cmake_minimum_required(VERSION 3.6.3)

# GoogleTest
configure_file(${CMAKE_SOURCE_DIR}/contrib/CMakeLists.txt.in ../googletest-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/googletest-download )
if(result)
  message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/googletest-download )
if(result)
    message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_BINARY_DIR}/googletest-src
                 ${CMAKE_BINARY_DIR}/googletest-build)

message( STATUS "GTest source dir: " ${gtest_SOURCE_DIR})

include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

file(GLOB_RECURSE HXHIM_TESTS hxhim/*.c*)
file(GLOB_RECURSE TRANSPORT_TESTS transport/*.c*)
file(GLOB_RECURSE UTIL_TESTS utils/*.c*)

add_executable(hxhim-tests main.cpp ${HXHIM_TESTS} ${TRANSPORT_TESTS} ${UTIL_TESTS})
set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${LEVELDB_LIBRARY_DIRS})
target_link_libraries(hxhim-tests hxhim.a gtest gtest_main ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m)
