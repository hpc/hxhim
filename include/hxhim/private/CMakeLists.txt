cmake_minimum_required (VERSION 3.6.3)

set(PRIVATE_HEADERS
  Results.hpp
  Stats.hpp
  accessors.hpp
  hxhim.hpp
  process.hpp
)

foreach(HEADER ${PRIVATE_HEADERS})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${HEADER})
endforeach()
