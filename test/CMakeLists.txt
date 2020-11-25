#
# CMakeLists file for HXHIM tests
#

cmake_minimum_required(VERSION 3.6.3)

# GoogleTest
configure_file(${CMAKE_SOURCE_DIR}/contrib/googletest.txt.in ../googletest-download/CMakeLists.txt)
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

set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
set(BUILD_GMOCK   OFF CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_BINARY_DIR}/googletest-src
                 ${CMAKE_BINARY_DIR}/googletest-build)

message( STATUS "GTest source dir: " ${gtest_SOURCE_DIR})

include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

add_executable(googletest main.cpp)
target_include_directories(googletest SYSTEM BEFORE PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR})

set(TEST_COMMON
  TestDatastore.hpp
)

foreach(FILE ${TEST_COMMON})
  target_sources(googletest PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${FILE})
endforeach()

add_subdirectory(hxhim)
add_subdirectory(datastore)
add_subdirectory(transport)
add_subdirectory(utils)
target_link_libraries(googletest hxhim.a gtest ${EXEC_LDFLAGS})
add_test(googletest ${MPIEXEC} -np 1 googletest)
