cmake_minimum_required(VERSION 3.6.3)

if (SOP)
  target_compile_definitions(googletest PRIVATE SOP=1)
endif()

if (PSO)
  target_compile_definitions(googletest PRIVATE PSO=1)
endif()

if (POS)
  target_compile_definitions(googletest PRIVATE POS=1)
endif()

if (OSP)
  target_compile_definitions(googletest PRIVATE OSP=1)
endif()

if (OPS)
  target_compile_definitions(googletest PRIVATE OPS=1)
endif()

target_sources(googletest PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/GetStats.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/InMemory.cpp
)

if (LEVELDB_FOUND AND ENABLE_LEVELDB)
target_sources(googletest PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/leveldb.cpp
)
endif()
