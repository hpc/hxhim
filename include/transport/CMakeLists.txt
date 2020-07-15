cmake_minimum_required(VERSION 3.6.3)

target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/constants.hpp)

install(FILES
  constants.hpp
  DESTINATION include/transport)

add_subdirectory(Messages)
add_subdirectory(backend)
