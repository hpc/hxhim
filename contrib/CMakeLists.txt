cmake_minimum_required (VERSION 3.6.3)

if (HDF5_FOUND AND ENABLE_HDF5)
  add_subdirectory(vol)
endif()
