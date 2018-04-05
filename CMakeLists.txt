# CMakeLists files in this project can
# refer to the root source directory of the project as ${HXHIM_SOURCE_DIR} and
# to the root binary directory of the project as ${HXHIM_BINARY_DIR}.
cmake_minimum_required (VERSION 3.6.3)
project (HXHIM)

#
# Enable C++14
#
set (CMAKE_CXX_STANDARD 14)

#
# Enable extra compiler flags
#
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -g3")
#set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wall")

#
# Add additional cmake files
#
set(HXHIM_CMAKE_MODULE_DIR ./CMake)
set(CMAKE_MODULE_PATH $PCMAKE_MODULE_PATH} ${HXHIM_CMAKE_MODULE_DIR})

#
# Project wide include stuff
#
include_directories(BEFORE include)
set(HEADER_FILES include/hxhim.h)
include_directories(src/transport)
include_directories(src/mdhim)
include_directories(src/utils)

# Include CMake modules
include(FindPkgConfig REQUIRED)

#
# External projects
#
find_package(MPI REQUIRED)
if (MPI_FOUND)
    message(STATUS "MPI_C_LIBRARIES=" ${MPI_C_LIBRARIES})
    message(STATUS "MPI_C_LINK_FLAGS=" ${MPI_C_LINK_FLAGS})
    message(STATUS "MPI_C_COMPILE_FLAGS=" ${MPI_C_COMPILE_FLAGS})
    message(STATUS "MPI_CXX_LINK_FLAGS=" ${MPI_CXX_LINK_FLAGS})
    message(STATUS "MPI_CXX_COMPILE_FLAGS=" ${MPI_CXX_COMPILE_FLAGS})
    include_directories(SYSTEM ${MPI_INCLUDE_PATH})
    add_definitions(-DHXHIM_USE_MPI)
endif()

# GoogleTest
configure_file(contrib/CMakeLists.txt.in googletest-download/CMakeLists.txt)
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

# The gtest/gtest_main targets carry header search path
# dependencies automatically when using CMake 2.8.11 or
# later. Otherwise we have to add them here ourselves.
if (CMAKE_VERSION VERSION_LESS 2.8.11)
    include_directories("${gtest_SOURCE_DIR}/include")
endif()

message( STATUS "GTest source dir: " ${gtest_SOURCE_DIR})

#
# Add subdirectories
#
add_subdirectory (src/base)
add_subdirectory (src/demo)
add_subdirectory (src/mdhim)
add_subdirectory (src/transport)
add_subdirectory (src/utils)

#
# Add unit test subdirectories
#
add_subdirectory(test/mdhim)
add_subdirectory(test/transport)
add_subdirectory(test/utils)

#
# Add examples subdirectory
#
add_subdirectory(examples)
