cmake_minimum_required(VERSION 3.6.3)

set(HXHIM_TEST_FILES
  BadGet.cpp
  Blob.cpp
  GetStats.cpp
  Histogram.cpp
  OpenClose.cpp
  PutGet.cpp
  PutGetOp.cpp
  RangeServer.cpp
  Results.cpp
  accessors.cpp
  background_put.cpp
  enqueue.cpp
  generic_options.cpp
  generic_options.hpp
  transport.cpp
  triplestore.cpp
)

foreach(FILE ${HXHIM_TEST_FILES})
  target_sources(googletest PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/${FILE}")
endforeach()

if (ASYNC_PUTS)
  target_compile_definitions(googletest PRIVATE ASYNC_PUTS=1)
endif()
