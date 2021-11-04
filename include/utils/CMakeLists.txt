cmake_minimum_required (VERSION 3.6.3)

include(TestBigEndian)
test_big_endian(SYSTEM_BIG_ENDIAN)
target_compile_definitions(hxhim PRIVATE SYSTEM_BIG_ENDIAN=${SYSTEM_BIG_ENDIAN})

set(INSTALLED_HEADERS
  Configuration.hpp
  Histogram.h
  Histogram.hpp
  Stats.hpp
  elen.h
  elen.hpp
  elen.tpp
  memory.hpp
  mlog2.h
  mlogfacs2.h
  type_traits.hpp
  uthash.h
)

set(NOT_INSTALLED_HEADERS
  Blob.hpp
  little_endian.hpp
  macros.hpp
  mkdir_p.hpp
)

foreach(HEADER ${INSTALLED_HEADERS} ${NOT_INSTALLED_HEADERS})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${HEADER})
endforeach()

foreach(HEADER ${INSTALLED_HEADERS})
  install(FILES ${HEADER} DESTINATION include/utils)
endforeach()
