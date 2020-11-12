cmake_minimum_required(VERSION 3.6.3)

set(UTILS_TEST_FILES
  Configuration.cpp
  Histogram.cpp
  Stats.cpp
  elen.cpp
  little_endian.cpp
  memory.cpp
)

foreach(FILE ${UTILS_TEST_FILES})
  target_sources(googletest PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/${FILE}")
endforeach()
