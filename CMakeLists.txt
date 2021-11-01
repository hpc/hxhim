# CMakeLists files in this project can
# refer to the root source directory of the project as ${HXHIM_SOURCE_DIR} and
# to the root binary directory of the project as ${HXHIM_BINARY_DIR}.
cmake_minimum_required (VERSION 3.6.3)
project (HXHIM LANGUAGES C CXX)

#
# Enable C++11
#
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

#
# Enable extra compiler flags
#
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -Wextra -fmax-errors=1 -O0 -g3 -DDEBUG") # -D_GLIBCXX_DEBUG

#
# linker flags used internally
#
set(EXEC_LDFLAGS)

#
# pkg-config flags
#
set(PC_CFLAGS)
set(PC_LIBS)        # flags required no matter which hxhim library is being linked against
set(PC_STATIC_LIBS) # normally linked to by shared library, so only used when linking with libhxhim.a

#
# Include CMake modules
#
find_package(PkgConfig REQUIRED)

#
# Need pthread
#
find_package(Threads REQUIRED)

#
# List of common linker flags
#
list(APPEND EXEC_LDFLAGS Threads::Threads m)
list(APPEND PC_LIBS -lstdc++)
list(APPEND PC_STATIC_LIBS ${CMAKE_THREAD_LIBS_INIT} -lm)

#
# External projects
#
if(POLICY CMP0074)
  cmake_policy(SET CMP0074 OLD)
endif()

find_package(MPI REQUIRED)
if (CMAKE_SYSTEM_NAME STREQUAL "CrayLinuxEnvironment")
    set(MPI_C_COMPILE_FLAGS "${MPI_C_COMPILE_FLAGS} -dynamic -craympich-mt")
    set(MPI_CXX_COMPILE_FLAGS "${MPI_CXX_COMPILE_FLAGS} -dynamic -craympich-mt")
endif()

message(STATUS "MPI_CXX_LIBRARIES=" ${MPI_CXX_LIBRARIES})
message(STATUS "MPI_CXX_LINK_FLAGS=" ${MPI_CXX_LINK_FLAGS})
message(STATUS "MPI_CXX_INCLUDE_DIRS=" ${MPI_CXX_INCLUDE_DIRS})
include_directories(SYSTEM ${MPI_CXX_INCLUDE_DIRS})

list(APPEND EXEC_LDFLAGS ${MPI_CXX_LIBRARIES})
foreach(INCLUDE_DIR ${MPI_CXX_INCLUDE_DIRS})
  list(APPEND PC_CFLAGS "-I${INCLUDE_DIR}")
endforeach()
list(APPEND PC_LIBS -lmpi)
list(APPEND PC_STATIC_LIBS ${MPI_CXX_LIBRARIES})

# jemalloc is nice to have but not necessary
pkg_search_module(JEMALLOC jemalloc)
if (JEMALLOC_FOUND)
  option(ENABLE_JEMALLOC "Use jemalloc if found" On)
  if (ENABLE_JEMALLOC)
    message(STATUS "JEMALLOC_INCLUDEDIR=" ${JEMALLOC_INCLUDEDIR})
    message(STATUS "JEMALLOC_LIBDIR=" ${JEMALLOC_LIBDIR})
    list(APPEND EXEC_LDFLAGS ${JEMALLOC_LDFLAGS})
    list(APPEND PC_STATIC_LIBS ${JEMALLOC_LDFLAGS})
  else()
    message(STATUS "jemalloc found but not used")
  endif()
else()
  message(STATUS "jemalloc not found")
endif()

# do not search for thallium with find_package
pkg_search_module(THALLIUM thallium)
if (THALLIUM_FOUND)
  option(ENABLE_THALLIUM "Use Thallium if found" On)

  if (ENABLE_THALLIUM)
    # thallium needs C++14
    set(CMAKE_CXX_STANDARD 14)

    pkg_search_module(MERCURY REQUIRED mercury)
    message(STATUS "MERCURY_INCLUDEDIR=" ${MERCURY_INCLUDEDIR})
    message(STATUS "MERCURY_LIBDIR=" ${MERCURY_LIBDIR})

    pkg_search_module(ARGOBOTS REQUIRED argobots)
    message(STATUS "ARGOBOTS_INCLUDEDIR=" ${ARGOBOTS_INCLUDEDIR})
    message(STATUS "ARGOBOTS_LIBDIR=" ${ARGOBOTS_LIBDIR})

    pkg_search_module(MARGO REQUIRED margo)
    message(STATUS "MARGO_INCLUDEDIR=" ${MARGO_INCLUDEDIR})
    message(STATUS "MARGO_LIBDIR=" ${MARGO_LIBDIR})

    message(STATUS "THALLIUM_INCLUDEDIR=" ${THALLIUM_INCLUDEDIR})
    message(STATUS "THALLIUM_LIBDIR=" ${THALLIUM_LIBDIR})
    include_directories(AFTER SYSTEM ${THALLIUM_INCLUDE_DIRS})
    add_definitions(-DHXHIM_HAVE_THALLIUM=1)

    list(APPEND EXEC_LDFLAGS ${THALLIUM_STATIC_LDFLAGS})

    list(APPEND PC_CFLAGS -DHXHIM_HAVE_THALLIUM=1)
    list(APPEND PC_LIBS ${THALLIUM_STATIC_LDFLAGS})
  else ()
    message(STATUS "Thallium found but not used")
  endif()
else()
  message(STATUS "Thallium not found")
endif()

#
# LevelDB Support
#
pkg_search_module(LEVELDB leveldb)
if(LEVELDB_FOUND)
  option(ENABLE_LEVELDB "Use LevelDB if found" On)

  if (ENABLE_LEVELDB)
    message(STATUS "LevelDB Data Storage Module Enabled")
    message(STATUS "LEVELDB_INCLUDEDIR=" ${LEVELDB_INCLUDEDIR})
    message(STATUS "LEVELDB_LIBDIR=" ${LEVELDB_LIBDIR})
    include_directories(AFTER SYSTEM ${LEVELDB_INCLUDE_DIRS})
    add_definitions(-DHXHIM_HAVE_LEVELDB=1)

    list(APPEND EXEC_LDFLAGS ${LEVELDB_LDFLAGS})

    list(APPEND PC_CFLAGS -DHXHIM_HAVE_LEVELDB=1)
    list(APPEND PC_LIBS ${LEVELDB_LDFLAGS})

    option(DBBENCH "Build dbbench for hxhim" On)
  else()
    message(STATUS "LevelDB found but not used")
  endif()
else()
  message(STATUS "LevelDB not found")
endif()

#
# RocksDB Support
#
pkg_search_module(ROCKSDB rocksdb)
if(ROCKSDB_FOUND)
  option(ENABLE_ROCKSDB "Use RocksDB if found" On)

  if (ENABLE_ROCKSDB)
    message(STATUS "RocksDB Data Storage Module Enabled")
    message(STATUS "ROCKSDB_INCLUDEDIR=" ${ROCKSDB_INCLUDEDIR})
    message(STATUS "ROCKSDB_INCLUDE_DIRS=" ${ROCKSDB_INCLUDE_DIRS})
    message(STATUS "ROCKSDB_LIBDIR=" ${ROCKSDB_LIBDIR})
    include_directories(AFTER SYSTEM ${ROCKSDB_INCLUDE_DIRS})
    add_definitions(-DHXHIM_HAVE_ROCKSDB=1)

    list(APPEND EXEC_LDFLAGS ${ROCKSDB_LDFLAGS})

    list(APPEND PC_CFLAGS -DHXHIM_HAVE_ROCKSDB=1)
    list(APPEND PC_LIBS ${ROCKSDB_LDFLAGS})
  else()
    message(STATUS "RocksDB found but not used")
  endif()
else()
  message(STATUS "RocksDB not found")
endif()

# always look for HDF5
find_package(HDF5)
if (HDF5_FOUND)
  option(ENABLE_HDF5 "Use HDF5 if found" On)

  if (ENABLE_HDF5)
    message(STATUS "HDF5_INCLUDE_DIRS=" ${HDF5_INCLUDE_DIRS})
    message(STATUS "HDF5_LIBRARIES=" ${HDF5_LIBRARIES})
  else()
    message(STATUS "HDF5 found but not used")
  endif()
else()
  message(STATUS "HDF5 not found")
endif()

#
# build library
#
add_subdirectory(src)

#
# Install headers and libraries
#
add_subdirectory(include)

#
# Add configuration files
#
add_subdirectory(config)

#
# Add extra files
#
add_subdirectory(contrib)

#
# Add demo executable
#
add_subdirectory(demo)

#
# Add unit test executable
#
enable_testing()
add_subdirectory(test)

#
# Add example executables
#
add_subdirectory(examples)
