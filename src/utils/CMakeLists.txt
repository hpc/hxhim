cmake_minimum_required (VERSION 3.6.3)

include(TestBigEndian)
test_big_endian(SYSTEM_BIG_ENDIAN)

if (SYSTEM_BIG_ENDIAN)
  message(STATUS "This machine is big endian")
else()
  message(STATUS "This machine is little endian")
endif()

target_compile_definitions(hxhim PRIVATE SYSTEM_BIG_ENDIAN=${SYSTEM_BIG_ENDIAN})

set(UTILS_SRC
  Blob.cpp
  Configuration.cpp
  Histogram.cpp
  Stats.cpp
  elen.cpp
  is_range_server.c
  memory.cpp
  mkdir_p.cpp
  mlog2.c
  triplestore.cpp
)

foreach(FILE ${UTILS_SRC})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${FILE})
endforeach()
