cmake_minimum_required (VERSION 3.6.3)

target_sources(hxhim PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/EndpointBase.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/EndpointGroup.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/Init.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/Instance.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/RangeServer.cpp
)
