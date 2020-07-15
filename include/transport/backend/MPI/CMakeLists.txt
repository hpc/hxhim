cmake_minimum_required (VERSION 3.6.3)

set(MPI_TRANSPORT_HEADERS
  constants.h
  EndpointBase.hpp
  EndpointGroup.hpp
  EndpointGroup.tpp
  Init.hpp
  Instance.hpp
  MPI.hpp
  Options.hpp
  RangeServer.hpp
)

foreach(HEADER ${MPI_TRANSPORT_HEADERS})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${HEADER})
endforeach()
