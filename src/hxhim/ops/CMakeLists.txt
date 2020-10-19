cmake_minimum_required (VERSION 3.6.3)

set(HXHIM_SRC
  PUT.cpp
  GET.cpp
  GETOP.cpp
  DELETE.cpp
  HISTOGRAM.cpp

  BPUT.cpp
  BGET.cpp
  BGETOP.cpp
  BDELETE.cpp
  BHISTOGRAM.cpp

  double.cpp
  float.cpp
  single_type.cpp

  FLUSH.cpp
  SYNC.cpp

  impl.cpp
)

foreach(FILE ${HXHIM_SRC})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${FILE})
endforeach()
