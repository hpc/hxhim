cmake_minimum_required(VERSION 3.6.3)

set(HXHIM_TEST_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/BadGet.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/check_memory.hpp
  ${CMAKE_CURRENT_SOURCE_DIR}/generic_options.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/generic_options.hpp
  ${CMAKE_CURRENT_SOURCE_DIR}/Histogram.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/PutGet.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/Results.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/transport.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/triplestore.cpp

  PARENT_SCOPE)
