cmake_minimum_required(VERSION 3.6.3)

target_sources(googletest PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/Blob.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/Configuration.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/Histogram.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/Stats.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/elen.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/is_range_server.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/memory.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/triplestore.cpp
)
