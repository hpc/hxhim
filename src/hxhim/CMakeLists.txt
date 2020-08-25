cmake_minimum_required (VERSION 3.6.3)

set(HXHIM_SRC
  Results.cpp
  accessors.cpp
  config.cpp
  double.cpp
  float.cpp
  hash.cpp
  hxhim.cpp
  options.cpp
  single_type.cpp
)

foreach(FILE ${HXHIM_SRC})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${FILE})
endforeach()

option(PRINT_TIMESTAMPS "Whether or not to print timestamps" Off)
if (PRINT_TIMESTAMPS)
  target_compile_definitions(hxhim PRIVATE PRINT_TIMESTAMPS)
  option(PRINT_FIND_DST "Whether or not to print FindDst timestamps" Off)
  if (PRINT_FIND_DST)
    target_compile_definitions(hxhim PRIVATE PRINT_FIND_DST)
  endif()
endif()

add_subdirectory(private)
