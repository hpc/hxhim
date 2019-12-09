cmake_minimum_required (VERSION 3.6.3)

set(THALLIUM_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/Endpoint.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/EndpointGroup.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/Init.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/RangeServer.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/Utilities.cpp

  PARENT_SCOPE)
