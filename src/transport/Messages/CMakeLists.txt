cmake_minimum_required (VERSION 3.6.3)

set(MESSAGE_SOURCES
  Message.cpp
  Messages.cpp

  Request.cpp
  Response.cpp
  SubjectPredicate.cpp

  BPut.cpp
  BGet.cpp
  BGetOp.cpp
  BDelete.cpp
  BHistogram.cpp

  Packer.cpp
  Unpacker.cpp
)

foreach(MESSAGE ${MESSAGE_SOURCES})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${MESSAGE})
endforeach()
