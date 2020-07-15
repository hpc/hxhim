cmake_minimum_required (VERSION 3.6.3)

target_sources(hxhim PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/Message.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/Messages.cpp

  ${CMAKE_CURRENT_SOURCE_DIR}/Request.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/Response.cpp

  ${CMAKE_CURRENT_SOURCE_DIR}/BPut.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/BGet.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/BGetOp.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/BDelete.cpp

  ${CMAKE_CURRENT_SOURCE_DIR}/Packer.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/Unpacker.cpp
)
