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
  enable_if_t.hpp
  memory.hpp
  mlog2.h
  mlogfacs2.h
  DESTINATION include/utils)
