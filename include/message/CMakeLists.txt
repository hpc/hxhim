cmake_minimum_required(VERSION 3.6.3)

set(MESSAGE_HEADERS
  constants.hpp

  Message.hpp
  Messages.hpp

  Request.hpp
  Response.hpp
  SubjectPredicate.hpp

  BPut.hpp
  BGet.hpp
  BGetOp.hpp
  BDelete.hpp
  BHistogram.hpp

  Packer.hpp
  Unpacker.hpp
)

foreach(MESSAGE ${MESSAGE_HEADERS})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${MESSAGE})
endforeach()
