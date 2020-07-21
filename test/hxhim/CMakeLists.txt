cmake_minimum_required(VERSION 3.6.3)

set(HXHIM_TEST_FILES
  BadGet.cpp
  GetStats.cpp
  Histogram.cpp
  OpenClose.cpp
  PutGet.cpp
  Results.cpp
  accessors.cpp
  cache.cpp
  enqueue.cpp
  generic_options.cpp
  generic_options.hpp
  shuffle.cpp
  transport.cpp
)

foreach(FILE ${HXHIM_TEST_FILES})
  target_sources(googletest PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/${FILE}")
endforeach()
