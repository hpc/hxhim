cmake_minimum_required (VERSION 3.6.3)

set(THALLIUM_SRC
  EndpointGroup.cpp
  Init.cpp
  RangeServer.cpp
  Utilities.cpp
)

foreach(FILE ${THALLIUM_SRC})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${FILE})
endforeach()
