cmake_minimum_required(VERSION 3.6.3)

include_directories(${CMAKE_BINARY_DIR}/include ${CMAKE_SOURCE_DIR}/include)

add_library(util.o OBJECT util.cpp)

add_subdirectory(hxhim)

if (HDF5_FOUND)
  add_subdirectory(vol)
endif()
