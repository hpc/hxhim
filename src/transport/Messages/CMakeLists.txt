cmake_minimum_required (VERSION 3.6.3)

set(MESSAGES_SRC
  Message.cpp
  Send.cpp
  Recv.cpp
  Bulk.cpp

  Request.cpp
  Response.cpp

  SendBPut.cpp
  SendBGet.cpp
  SendBGet2.cpp
  SendBGetOp.cpp
  SendBDelete.cpp
  SendBHistogram.cpp

  RecvBPut.cpp
  RecvBGet.cpp
  RecvBGet2.cpp
  RecvBGetOp.cpp
  RecvBDelete.cpp
  RecvBHistogram.cpp

  Packer.cpp
  Unpacker.cpp
)

add_library(MESSAGES OBJECT ${MESSAGES_SRC})
set(MESSAGESS ${MESSAGESS} $<TARGET_OBJECTS:MESSAGES> PARENT_SCOPE)
