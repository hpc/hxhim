cmake_minimum_required (VERSION 3.6.3)

set(HXHIM_SRC
  RangeServer.cpp
  Results.cpp
  Stats.cpp
  accessors.cpp
  config.cpp
  destroy.cpp
  hash.cpp
  hxhim.cpp
  init.cpp
  options.cpp
  private.cpp
  triplestore.cpp
)

foreach(FILE ${HXHIM_SRC})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${FILE})
endforeach()

option(PRINT_TIMESTAMPS "Whether or not to print timestamps" Off)
if (PRINT_TIMESTAMPS)
  target_compile_definitions(hxhim PRIVATE PRINT_TIMESTAMPS)
endif()

add_subdirectory(ops)
