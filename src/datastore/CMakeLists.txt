cmake_minimum_required (VERSION 3.6.3)

set(DATASTORE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/datastore.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/InMemory.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/leveldb.cpp

  PARENT_SCOPE)
