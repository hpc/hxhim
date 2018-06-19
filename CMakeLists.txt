# CMakeLists files in this project can
# refer to the root source directory of the project as ${HXHIM_SOURCE_DIR} and
# to the root binary directory of the project as ${HXHIM_BINARY_DIR}.
cmake_minimum_required (VERSION 3.6.3)
project (HXHIM)

#
# Enable C++14
#
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

#
# Enable extra compiler flags
#
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -O0 -g3")

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
find_package(PkgConfig REQUIRED)

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

    pkg_search_module(MARGO REQUIRED margo)
    if (MARGO_FOUND)
      message(STATUS "MARGO_INCLUDEDIR=" ${MARGO_INCLUDEDIR})
      message(STATUS "MARGO_LIBDIR=" ${MARGO_LIBDIR})
      include_directories(AFTER SYSTEM ${MARGO_INCLUDEDIR})
      SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath -Wl,${MARGO_LIBDIR}")
    endif()
endif()

#
# LevelDB Support
#
pkg_search_module(LEVELDB REQUIRED leveldb)
if(${LEVELDB_FOUND})
    message(STATUS "LevelDB Data Storage Module Enabled")
    message(STATUS "LEVELDB_LIBRARY_DIRS=" ${LEVELDB_LIBRARY_DIRS})
    message(STATUS "LEVELDB_LIBRARIES=" ${LEVELDB_LIBRARIES})
    link_directories(${LEVELDB_LIBRARY_DIRS})
    set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${LEVELDB_LIBRARY_DIRS})
    #link_libraries(${LEVELDB_LIBRARIES})
    include_directories(AFTER SYSTEM ${LEVELDB_INCLUDE_DIRS})
    find_library(LEVELDB_LIBRARY NAMES leveldb HINTS ${LEVELDB_LIBRARY_DIRS})
    add_definitions(-DHXHIM_USE_LEVELDB)
endif()

#
# RocksDB Support
#
pkg_search_module(ROCKSDB rocksdb)
if(${ROCKSDB_FOUND})
  message(STATUS "RocksDB Data Storage Module Enabled")
  include_directories(AFTER SYSTEM ${ROCKSDB_INCLUDE_DIRS})
  add_definitions(-DHXHIM_USE_ROCKSDB)
endif()

#
# MySQL Support
#
find_package(MYSQL QUIET)
if(${MYSQL_FOUND})
  message(STATUS "MySQL Data Storage Module Enabled")
  add_definitions(-DHXHIM_USE_MYSQL)
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
# Create libraries
#
file(GLOB INCLUDES include/*.c*)
file(GLOB BASE src/base/*.c*)
file(GLOB MDHIM src/mdhim/*.c*)
file(GLOB TRANSPORT src/transport/*.c*)
file(GLOB UTILS src/utils/*.c*)
list(FILTER UTILS EXCLUDE REGEX ".*/elen.tpp")

set(LIBMDHIM_SOURCES ${MDHIM} ${TRANSPORT} ${UTILS})
message(STATUS "LIBMDHIM_SOURCES: " ${LIBMDHIM_SOURCES})
add_library(mdhim OBJECT ${LIBMDHIM_SOURCES})
set_target_properties(mdhim PROPERTIES POSITION_INDEPENDENT_CODE ON)
add_library(mdhim.a  STATIC $<TARGET_OBJECTS:mdhim>)
set_target_properties(mdhim.a  PROPERTIES OUTPUT_NAME mdhim)
add_library(mdhim.so SHARED $<TARGET_OBJECTS:mdhim>)
set_target_properties(mdhim.so PROPERTIES OUTPUT_NAME mdhim)

set(LIBHXHIM_SOURCES ${INCLUDES} ${BASE})
message(STATUS "LIBHXHIM_SOURCES: " ${LIBHXHIM_SOURCES})
add_library(hxhim OBJECT ${LIBHXHIM_SOURCES})
set_target_properties(hxhim PROPERTIES POSITION_INDEPENDENT_CODE ON)
add_library(hxhim.a  STATIC $<TARGET_OBJECTS:hxhim>)
set_target_properties(hxhim.a  PROPERTIES OUTPUT_NAME hxhim)
add_library(hxhim.so SHARED $<TARGET_OBJECTS:hxhim>)
set_target_properties(hxhim.so PROPERTIES OUTPUT_NAME hxhim)

#
# Install headers and libraries
#
INSTALL(FILES
  src/mdhim/mdhim.h
  src/mdhim/index_struct.h
  src/mdhim/mdhim_config.h
  src/mdhim/mdhim_constants.h
  src/mdhim/mdhim_options.h
  src/mdhim/mdhim_options_struct.h
  src/mdhim/mdhim_struct.h
  src/mdhim/range_server_info_struct.h
  src/transport/transport.h
  src/transport/transport_constants.h
  src/utils/mlog2.h
  src/utils/mlogfacs2.h
  src/utils/uthash.h
  DESTINATION include/mdhim)

install(FILES
  include/hxhim-types.h
  include/hxhim.h
  include/hxhim.hpp
  include/return.h
  include/return.hpp
  DESTINATION include/hxhim)

INSTALL(TARGETS
  mdhim.a
  mdhim.so
  hxhim.a
  hxhim.so
  DESTINATION lib)

#
# Add configuration files
#
add_subdirectory(config)

#
# Add demo executable
#
add_subdirectory(src/demo)

#
# Add unit test executables
#
add_subdirectory(test/hxhim)
add_subdirectory(test/mdhim)
add_subdirectory(test/transport)
add_subdirectory(test/utils)

#
# Add example executables
#
add_subdirectory(examples)
