cmake_minimum_required(VERSION 3.6.3)

set(TRANSPORT_TESTS_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/mpi.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/thallium.cpp

  PARENT_SCOPE)
