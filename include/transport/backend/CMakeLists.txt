cmake_minimum_required(VERSION 3.6.3)

target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/backends.hpp)

add_subdirectory(local)
add_subdirectory(MPI)

if (THALLIUM_FOUND AND ENABLE_THALLIUM)
  add_subdirectory(Thallium)
endif()
