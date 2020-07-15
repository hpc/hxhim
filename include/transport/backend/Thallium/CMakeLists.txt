cmake_minimum_required (VERSION 3.6.3)

set(THALLIUM_TRANSPORT_HEADERS
  EndpointGroup.hpp
  Init.hpp
  Options.hpp
  RangeServer.hpp
  Thallium.hpp
  Utilities.hpp
)

foreach(HEADER ${THALLIUM_TRANSPORT_HEADERS})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${HEADER})
endforeach()
