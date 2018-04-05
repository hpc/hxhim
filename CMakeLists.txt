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

# do not search for thallium with find_package
pkg_search_module(THALLIUM REQUIRED thallium)
if (THALLIUM_FOUND)
    message(STATUS "THALLIUM_INCLUDEDIR=" ${THALLIUM_INCLUDEDIR})
    message(STATUS "THALLIUM_LIBDIR=" ${THALLIUM_LIBDIR})
    include_directories(AFTER SYSTEM ${THALLIUM_INCLUDEDIR})
    string (REPLACE ";" " " THALLIUM_LDFLAGS  "${THALLIUM_LDFLAGS}")
    SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath -Wl,${THALLIUM_LIBDIR}")
    add_definitions(-DHXHIM_USE_THALLIUM)

    pkg_search_module(MERCURY REQUIRED mercury)
    if (MERCURY_FOUND)
      message(STATUS "MERCURY_INCLUDEDIR=" ${MERCURY_INCLUDEDIR})
      message(STATUS "MERCURY_LIBDIR=" ${MERCURY_LIBDIR})
      include_directories(AFTER SYSTEM ${MERCURY_INCLUDEDIR})
      SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath -Wl,${MERCURY_LIBDIR}")
    endif()

    pkg_search_module(ARGOBOTS REQUIRED argobots)
    if (ARGOBOTS_FOUND)
      message(STATUS "ARGOBOTS_INCLUDEDIR=" ${ARGOBOTS_INCLUDEDIR})
      message(STATUS "ARGOBOTS_LIBDIR=" ${ARGOBOTS_LIBDIR})
      include_directories(AFTER SYSTEM ${ARGOBOTS_INCLUDEDIR})
      SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath -Wl,${ARGOBOTS_LIBDIR}")
    endif()

    pkg_search_module(ABT_SNOOZER REQUIRED abt-snoozer)
    if (ABT_SNOOZER_FOUND)
      message(STATUS "ABT_SNOOZER_INCLUDEDIR=" ${ABT_SNOOZER_INCLUDEDIR})
      message(STATUS "ABT_SNOOZER_LIBDIR=" ${ABT_SNOOZER_LIBDIR})
      include_directories(AFTER SYSTEM ${ABT_SNOOZER_INCLUDEDIR})
      SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath -Wl,${ABT_SNOOZER_LIBDIR}")
    endif()

    pkg_search_module(MARGO REQUIRED margo)
    if (MARGO_FOUND)
      message(STATUS "MARGO_INCLUDEDIR=" ${MARGO_INCLUDEDIR})
      message(STATUS "MARGO_LIBDIR=" ${MARGO_LIBDIR})
      include_directories(AFTER SYSTEM ${MARGO_INCLUDEDIR})
      SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath -Wl,${MARGO_LIBDIR}")
    endif()
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
