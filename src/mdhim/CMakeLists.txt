#
# Use PkgConfig to find external libraries
#
find_package(PkgConfig REQUIRED)

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
    add_definitions(-DHXHIM_USE_LEVELDB)
endif()

#
# RocksDB Support
#
pkg_search_module(ROCKSDB rocksdb)
include_directories(AFTER ${ROCKSDB_INCLUDE_DIRS})
if(${ROCKSDB_FOUND})
  message(STATUS "RocksDB Data Storage Module Enabled")
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

#
# Copy the default configuration file to the build directory
#
configure_file(mdhim.conf ${CMAKE_BINARY_DIR} COPYONLY)

#
# Create the mdhim library
#
file(GLOB LIBMDHIM_SOURCES *.c*)
MESSAGE( STATUS "LIBMDHIM_SOURCES: " ${LIBMDHIM_SOURCES})
MESSAGE( STATUS "INCLUDE_DIRECTORIES: " ${INCLUDE_DIRECTORIES})
add_library(mdhim STATIC ${LIBMDHIM_SOURCES})
#add_library(mdhim.dylib DYNAMIC ${LIBMDHIM_SOURCES})
