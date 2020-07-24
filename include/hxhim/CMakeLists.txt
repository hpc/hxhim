#
# Install headers and libraries
#

cmake_minimum_required (VERSION 3.6.3)

set(HXHIM_HEADERS
  Results.h
  Results.hpp
  Stats.hpp
  accessors.h
  accessors.hpp
  config.h
  config.hpp
  constants.h
  double.h
  double.hpp
  float.h
  float.hpp
  hash.h
  hxhim.h
  hxhim.hpp
  options.h
  options.hpp
  single_type.h
  single_type.hpp
  struct.h
)

foreach(HEADER ${HXHIM_HEADERS})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${HEADER})
  install(FILES ${HEADER} DESTINATION include/hxhim)
endforeach()

add_subdirectory(private)
