cmake_minimum_required(VERSION 3.6.3)

install(FILES
  constants.hpp
  DESTINATION include/transport)

add_subdirectory(Messages)
