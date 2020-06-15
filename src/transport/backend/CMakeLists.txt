cmake_minimum_required (VERSION 3.6.3)

add_subdirectory(local)
add_subdirectory(MPI)

if (THALLIUM_FOUND AND ENABLE_THALLIUM)
  add_subdirectory(Thallium)
endif()
