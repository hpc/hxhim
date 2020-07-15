cmake_minimum_required (VERSION 3.6.3)

target_sources(hxhim PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/Stats.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/accessors.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/cache.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/hxhim.cpp
)
