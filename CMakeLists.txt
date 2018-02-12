# CMakeLists files in this project can
# refer to the root source directory of the project as ${HXHIM_SOURCE_DIR} and
# to the root binary directory of the project as ${HXHIM_BINARY_DIR}.
cmake_minimum_required (VERSION 3.6.3)
project (HXHIM)

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
include_directories(src/comm)
include_directories(src/mdhim)
include_directories(src/utils)


#
# External projects
#
find_package(MPI)
if (MPI_FOUND)
    include_directories(${MPI_INCLUDE_PATH})
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

#
# Add subdirectories
#
add_subdirectory (src/base)
add_subdirectory (src/comm)
add_subdirectory (src/mdhim)
add_subdirectory (src/demo)
add_subdirectory (src/utils)

#
# Add unit test subdirectories
#
add_subdirectory(test/comm)
add_subdirectory(test/mdhim)
