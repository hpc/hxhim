cmake_minimum_required (VERSION 3.6.3)

set(UTILS_SRC
  Blob.cpp
  Configuration.cpp
  Histogram.cpp
  Stats.cpp
  elen.cpp
  memory.cpp
  mkdir_p.cpp
  mlog2.c
)

foreach(FILE ${UTILS_SRC})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${FILE})
endforeach()
