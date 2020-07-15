cmake_minimum_required (VERSION 3.6.3)

set(LOCAL_TRANSPORT_HEADERS
  RangeServer.hpp
  local.hpp
)

foreach(HEADER ${LOCAL_TRANSPORT_HEADERS})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${HEADER})
endforeach()
