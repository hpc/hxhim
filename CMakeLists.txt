# CMakeLists files in this project can
# refer to the root source directory of the project as ${HELLO_SOURCE_DIR} and
# to the root binary directory of the project as ${HELLO_BINARY_DIR}.
cmake_minimum_required (VERSION 2.8.11)
project (HXHIM)

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
