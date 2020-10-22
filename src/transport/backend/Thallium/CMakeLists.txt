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

option(ASYNC_THALLIUM "Send Thallium RPCs asynchronously" Off)
if (ASYNC_THALLIUM)
  target_compile_definitions(hxhim PRIVATE ASYNC_THALLIUM=1)
endif()
