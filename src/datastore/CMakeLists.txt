cmake_minimum_required (VERSION 3.6.3)

target_sources(hxhim PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/datastore.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/InMemory.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/leveldb.cpp
)
