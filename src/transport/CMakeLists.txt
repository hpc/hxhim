cmake_minimum_required (VERSION 3.6.3)

add_subdirectory(messages)
add_subdirectory(backend)

set(TRANSPORT_SRC
  ${MESSAGES_SRC}
  ${BACKEND_SRC}
  ${CMAKE_CURRENT_SOURCE_DIR}/transport.cpp

  PARENT_SCOPE)
