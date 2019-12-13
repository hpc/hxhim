cmake_minimum_required (VERSION 3.6.3)

set(MPI_SRC
  EndpointBase.cpp
  EndpointGroup.cpp
  Init.cpp
  Instance.cpp
  RangeServer.cpp
)

add_library(TRANSPORT_MPI OBJECT ${MPI_SRC})
set(TRANSPORTS ${TRANSPORTS} $<TARGET_OBJECTS:TRANSPORT_MPI> PARENT_SCOPE)
