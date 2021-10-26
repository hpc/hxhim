cmake_minimum_required(VERSION 3.6.3)

target_sources(googletest PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/messages.cpp
)
