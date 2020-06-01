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
set(COMMON_LDFLAGS Threads::Threads m)

#
# External projects
#
find_package(MPI REQUIRED)

if (CMAKE_SYSTEM_NAME STREQUAL "CrayLinuxEnvironment")
    set(MPI_C_COMPILE_FLAGS "${MPI_C_COMPILE_FLAGS} -dynamic -craympich-mt")
    set(MPI_CXX_COMPILE_FLAGS "${MPI_CXX_COMPILE_FLAGS} -dynamic -craympich-mt")
endif()

message(STATUS "MPI_C_LIBRARIES=" ${MPI_C_LIBRARIES})
message(STATUS "MPI_C_LINK_FLAGS=" ${MPI_C_LINK_FLAGS})
message(STATUS "MPI_C_COMPILE_FLAGS=" ${MPI_C_COMPILE_FLAGS})
message(STATUS "MPI_CXX_LINK_FLAGS=" ${MPI_CXX_LINK_FLAGS})
message(STATUS "MPI_CXX_COMPILE_FLAGS=" ${MPI_CXX_COMPILE_FLAGS})
set(PC_LIBS "${PC_LIBS} ${MPI_C_LINK_FLAGS} ${MPI_CXX_LINK_FLAGS}")
include_directories(SYSTEM ${MPI_INCLUDE_PATH})

set(COMMON_LDFLAGS ${COMMON_LDFLAGS} ${MPI_C_LIBRARIES})

if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
  pkg_search_module(JEMALLOC REQUIRED jemalloc)
  set(COMMON_LDFLAGS ${COMMON_LDFLAGS} ${JEMALLOC_LDFLAGS})
endif()

# do not search for thallium with find_package
pkg_search_module(THALLIUM thallium)
if (THALLIUM_FOUND)
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

  set(COMMON_LDFLAGS ${COMMON_LDFLAGS} ${THALLIUM_LDFLAGS})

  set(PC_CFLAGS "${PC_CFLAGS} -DHXHIM_HAVE_THALLIUM=1")
  set(PC_LIBS   "${PC_LIBS} ${THALLIUM_LDFLAGS}")
endif()

#
# LevelDB Support
#
pkg_search_module(LEVELDB leveldb)
if(LEVELDB_FOUND)
  message(STATUS "LevelDB Data Storage Module Enabled")
  message(STATUS "LEVELDB_INCLUDEDIR=" ${LEVELDB_INCLUDEDIR})
  message(STATUS "LEVELDB_LIBDIR=" ${LEVELDB_LIBDIR})
  include_directories(AFTER SYSTEM ${LEVELDB_INCLUDE_DIRS})
  add_definitions(-DHXHIM_HAVE_LEVELDB=1)

  set(COMMON_LDFLAGS ${COMMON_LDFLAGS} ${LEVELDB_LDFLAGS})

  set(PC_CFLAGS "${PC_CFLAGS} -DHXHIM_HAVE_LEVELDB=1")
  set(PC_LIBS   "${PC_LIBS} ${LEVELDB_LDFLAGS}")
endif()

option(ENABLE_HDF5 "Use HDF5 if found" On)

# always look for HDF5
find_package(HDF5)
if (HDF5_FOUND)
  message(STATUS "HDF5 found")
  message(STATUS "HDF5_INCLUDE_DIRS=" ${HDF5_INCLUDE_DIRS})
  message(STATUS "HDF5_LIBRARIES=" ${HDF5_LIBRARIES})
endif()

#
# build library
#
include_directories(include)
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
include_directories(demo)
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
