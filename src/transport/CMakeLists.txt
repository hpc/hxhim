cmake_minimum_required (VERSION 3.6.3)

add_subdirectory(backend)

target_sources(hxhim PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/transport.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/transports.cpp
)
