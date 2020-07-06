cmake_minimum_required (VERSION 3.6.3)

target_sources(hxhim PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/Results.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/Stats.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/accessors.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/cache.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/config.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/double.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/float.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/hash.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/hxhim.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/options.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/private.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/single_type.cpp
)
