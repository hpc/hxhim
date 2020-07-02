cmake_minimum_required(VERSION 3.6.3)

set(MESSAGES
  BDelete.hpp
  BGet.hpp
  BGetOp.hpp
  BHistogram.hpp
  BPut.hpp
  Message.hpp
  Messages.hpp
  Packer.hpp
  Request.hpp
  Response.hpp
  Unpacker.hpp
)


foreach(MESSAGE ${MESSAGES})
install(FILES
  ${MESSAGE}
  DESTINATION include/transport/Messages)
endforeach()
