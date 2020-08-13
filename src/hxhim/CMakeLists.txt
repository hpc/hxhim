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

option(PRINT_RESULT_TIMESTAMPS "Whether or not to print result timestamps" Off)
if (PRINT_RESULT_TIMESTAMPS)
  target_compile_definitions(hxhim PRIVATE PRINT_RESULT_TIMESTAMPS)
endif()

add_subdirectory(private)
