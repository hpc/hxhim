cmake_minimum_required (VERSION 3.6.3)

#
# Install headers
#
install(FILES
  Configuration.hpp
  Histogram.h
  Histogram.hpp
  elen.h
  elen.hpp
  elen.tpp
  memory.hpp
  mlog2.h
  mlogfacs2.h
  type_traits.hpp
  DESTINATION include/utils)
