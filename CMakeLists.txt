# CMakeLists files in this project can
# refer to the root source directory of the project as ${HXHIM_SOURCE_DIR} and
# to the root binary directory of the project as ${HXHIM_BINARY_DIR}.
cmake_minimum_required (VERSION 3.6.3)
project (HXHIM LANGUAGES C CXX)

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
# Create library
#
file(GLOB_RECURSE BASE src/base/*.c*)
file(GLOB_RECURSE DATASTORE src/datastore/*.c*)
file(GLOB_RECURSE HXHIM src/hxhim/*.c*)
file(GLOB_RECURSE TRANSPORT src/transport/*.c*)
file(GLOB_RECURSE UTILS src/utils/*.c*)

set(LIBHXHIM_SOURCES ${BASE} ${DATASTORE} ${HXHIM} ${TRANSPORT} ${UTILS})
list(REMOVE_ITEM LIBHXHIM_SOURCES **.tpp)
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
add_subdirectory(include)

INSTALL(TARGETS
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
# Add unit test executable
#
add_subdirectory(test)

#
# Add example executables
#
add_subdirectory(examples)
