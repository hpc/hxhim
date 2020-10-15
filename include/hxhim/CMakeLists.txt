#
# Install headers and libraries
#

cmake_minimum_required (VERSION 3.6.3)

set(HXHIM_HEADERS
  RangeServer.hpp
  Results.h
  Results.hpp
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

# option(ASYNC_PUTS "Flush PUTs asynchronously in addtion to explicit FlushPuts" On)
# if (ASYNC_PUTS)
#   target_compile_definitions(hxhim PRIVATE ASYNC_PUTS=1)
# endif()

foreach(HEADER ${HXHIM_HEADERS})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${HEADER})
  install(FILES ${HEADER} DESTINATION include/hxhim)
endforeach()

add_subdirectory(private)
