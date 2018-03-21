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

find_package(THALLIUM REQUIRED thallium)
if(${THALLIUM_FOUND})
    message(STATUS "Thallium Data Storage Module Enabled")
    message(STATUS "THALLIUM_INCLUDE_PATH=" ${THALLIUM_INCLUDE_PATH})
    message(STATUS "THALLIUM_LIBRARY_DIRS=" ${THALLIUM_LIBRARY_DIRS})
    link_directories(${THALLIUM_LIBRARY_DIRS})
    set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${THALLIUM_LIBRARY_DIRS})
    #link_libraries(${THALLIUM_LIBRARIES})
    include_directories(AFTER ${THALLIUM_INCLUDE_DIRS})
    find_library(THALLIUM_LIBRARY NAMES thallium HINTS ${THALLIUM_LIBRARY_DIRS})

    message(STATUS "Mercury Data Storage Module Enabled")
    message(STATUS "MERCURY_INCLUDEDIR=" ${MERCURY_INCLUDEDIR})
    message(STATUS "MERCURY_LIBRARY_DIRS=" ${MERCURY_LIBRARY_DIRS})
    link_directories(${MERCURY_LIBRARY_DIRS})
    set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${MERCURY_LIBRARY_DIRS})
    #link_libraries(${MERCURY_LIBRARIES})
    include_directories(AFTER ${MERCURY_INCLUDEDIR})
    find_library(MERCURY_LIBRARY NAMES mercury HINTS ${MERCURY_LIBRARY_DIRS})

    message(STATUS "Argobots Data Storage Module Enabled")
    message(STATUS "ARGOBOTS_INCLUDEDIR=" ${ARGOBOTS_INCLUDEDIR})
    message(STATUS "ARGOBOTS_LIBRARY_DIRS=" ${ARGOBOTS_LIBRARY_DIRS})
    link_directories(${ARGOBOTS_LIBRARY_DIRS})
    set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${ARGOBOTS_LIBRARY_DIRS})
    #link_libraries(${ARGOBOTS_LIBRARIES})
    include_directories(AFTER ${ARGOBOTS_INCLUDEDIR})
    find_library(ARGOBOTS_LIBRARY NAMES argobots HINTS ${ARGOBOTS_LIBRARY_DIRS})

    message(STATUS "Abt-Snoozer Data Storage Module Enabled")
    message(STATUS "ABT_SNOOZER_INCLUDEDIR=" ${ABT_SNOOZER_INCLUDEDIR})
    message(STATUS "ABT_SNOOZER_LIBRARY_DIRS=" ${ABT_SNOOZER_LIBRARY_DIRS})
    link_directories(${ABT_SNOOZER_LIBRARY_DIRS})
    set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${ABT_SNOOZER_LIBRARY_DIRS})
    #link_libraries(${ABT_SNOOZER_LIBRARIES})
    include_directories(AFTER ${ABT_SNOOZER_INCLUDEDIR})
    find_library(ABT_SNOOZER_LIBRARY NAMES abt-snoozer HINTS ${ABT_SNOOZER_LIBRARY_DIRS})

    message(STATUS "Margo Data Storage Module Enabled")
    message(STATUS "MARGO_INCLUDEDIR=" ${MARGO_INCLUDEDIR})
    message(STATUS "MARGO_LIBRARY_DIRS=" ${MARGO_LIBRARY_DIRS})
    link_directories(${MARGO_LIBRARY_DIRS})
    set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${MARGO_LIBRARY_DIRS})
    #link_libraries(${MARGO_LIBRARIES})
    include_directories(AFTER ${MARGO_INCLUDEDIR})
    find_library(MARGO_LIBRARY NAMES margo HINTS ${MARGO_LIBRARY_DIRS})

    add_definitions(-DHXHIM_USE_THALLIUM=1)
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
