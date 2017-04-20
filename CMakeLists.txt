# CMakeLists files in this project can
# refer to the root source directory of the project as ${HXHIM_SOURCE_DIR} and
# to the root binary directory of the project as ${HXHIM_BINARY_DIR}.
cmake_minimum_required (VERSION 2.8.11)
project (HXHIM)

#
# Add additional cmake files
#
set(HXHIM_CMAKE_MODULE_DIR ./CMake)
set(CMAKE_MODULE_PATH $PCMAKE_MODULE_PATH} ${HXHIM_CMAKE_MODULE_DIR})


#
# Add source locations
#
add_subdirectory (src/base)
add_subdirectory (src/mdhim)
add_subdirectory (src/demo)
add_subdirectory (src/utils)

#
# Project wide include stuff
#
include_directories(BEFORE include)
set(HEADER_FILES include/hxhim.h)
