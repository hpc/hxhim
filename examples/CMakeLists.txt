cmake_minimum_required(VERSION 3.6.3)

include_directories(${CMAKE_BINARY_DIR}/include ${CMAKE_SOURCE_DIR}/include)

add_subdirectory(hxhim)

if (HDF5_FOUND AND ENABLE_HDF5)
  add_subdirectory(vol)
endif()
