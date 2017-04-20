#
# Use PkgConfig to find external libraries
#
find_package(PkgConfig REQUIRED)

#
# MPI Support
#
if(APPLE)
  pkg_search_module(MPICH REQUIRED mpich)
endif()
find_package(MPI REQUIRED)

if(${MPI_FOUND})
  message(STATUS "MPI Communications Module Enabled")
  add_definitions(-DHXHIM_USE_MPI=1)
  set(CMAKE_CXX_COMPILER mpicxx)
  set(CMAKE_C_COMPILER mpicc)
  set(CMAKE_CXX_COMPILE_FLAGS ${CMAKE_CXX_COMPILE_FLAGS} ${MPI_COMPILE_FLAGS})
  set(CMAKE_CXX_LINK_FLAGS ${CMAKE_CXX_LINK_FLAGS} ${MPI_LINK_FLAGS})
  include_directories(AFTER ${MPI_C_INCLUDE_PATH})
endif()

#
# LevelDB Support
#
pkg_search_module(LEVELDB REQUIRED leveldb)
include_directories(AFTER ${LEVELDB_INCLUDE_DIRS})
if(${LEVELDB_FOUND})
  message(STATUS "LevelDB Data Storage Module Enabled")
  add_definitions(-DHXHIM_USE_LEVELDB=1)
endif()

#
# RocksDB Support
#
pkg_search_module(ROCKSDB rocksdb)
include_directories(AFTER ${ROCKSDB_INCLUDE_DIRS})
if(${ROCKSDB_FOUND})
  message(STATUS "RocksDB Data Storage Module Enabled")
  add_definitions(-DHXHIM_USE_ROCKSDB=1)
endif()

#
# MySQL Support
#
find_package(MYSQL QUIET)
if(${MYSQL_FOUND})
  message(STATUS "MySQL Data Storage Module Enabled")
  add_definitions(-DHXHIM_USE_MYSQL)
endif()

#
# Include utils files for MDHIM
#
include_directories(AFTER ../utils)

#
# Create the mdhim library
#
file(GLOB LIBMDHIM_SOURCES *.c)
add_library(mdhim STATIC ${LIBMDHIM_SOURCES})
#add_library(mdhim DYNAMIC ${LIBMDHIM_SOURCES})
