cmake_minimum_required (VERSION 3.6.3)

set(INSTALLED_HEADERS
  Blob.hpp
  Histogram.h
  Stats.h
  memory.hpp
)

set(NOT_INSTALLED_HEADERS
  Configuration.hpp
  Histogram.hpp
  big_endian.hpp
  elen.h
  elen.hpp
  elen.tpp
  mlog2.h
  mlogfacs2.h
  type_traits.hpp
)

foreach(HEADER ${INSTALLED_HEADERS} ${NOT_INSTALLED_HEADERS})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${HEADER})
endforeach()

foreach(HEADER ${INSTALLED_HEADERS})
  install(FILES ${HEADER} DESTINATION include/utils)
endforeach()
