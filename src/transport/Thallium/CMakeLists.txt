cmake_minimum_required (VERSION 3.6.3)

set(THALLIUM_SRC
  EndpointGroup.cpp
  Init.cpp
  RangeServer.cpp
  Utilities.cpp
)

add_library(TRANSPORT_THALLIUM OBJECT ${THALLIUM_SRC})
set(TRANSPORTS ${TRANSPORTS} $<TARGET_OBJECTS:TRANSPORT_THALLIUM> PARENT_SCOPE)
