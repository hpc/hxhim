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

MESSAGE( STATUS "MPI Found: " ${MPI_FOUND})
if (MPI_FOUND)
    MESSAGE( STATUS "MPI_C_LIBRARIES=" ${MPI_C_LIBRARIES})
    MESSAGE( STATUS "MPI_C_LINK_FLAGS=" ${MPI_C_LINK_FLAGS})
    MESSAGE( STATUS "MPI_C_COMPILE_FLAGS=" ${MPI_C_COMPILE_FLAGS})
    MESSAGE( STATUS "MPI_CXX_LINK_FLAGS=" ${MPI_CXX_LINK_FLAGS})
    MESSAGE( STATUS "MPI_CXX_COMPILE_FLAGS=" ${MPI_CXX_COMPILE_FLAGS})
    message(STATUS "MPI Transportunications Module Enabled")
    add_definitions(-DHXHIM_USE_MPI=1)
    #include_directories(AFTER ${MPI_C_INCLUDE_PATH})
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
    include_directories(AFTER ${LEVELDB_INCLUDE_DIRS})
    find_library(LEVELDB_LIBRARY NAMES leveldb HINTS ${LEVELDB_LIBRARY_DIRS})
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
# Create the mdhim library
#
file(GLOB LIBMDHIM_SOURCES *.c*)
MESSAGE( STATUS "LIBMDHIM_SOURCES: " ${LIBMDHIM_SOURCES})
MESSAGE( STATUS "INCLUDE_DIRECTORIES: " ${INCLUDE_DIRECTORIES})
add_library(mdhim STATIC ${LIBMDHIM_SOURCES})
#add_library(mdhim.dylib DYNAMIC ${LIBMDHIM_SOURCES})
