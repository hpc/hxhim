cmake_minimum_required (VERSION 3.6.3)

set(HXHIM_SRC
  Histograms.cpp
  Results.cpp
  Stats.cpp
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

add_subdirectory(private)
