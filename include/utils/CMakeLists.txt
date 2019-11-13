#
# Install headers
#
set(HEADERS
  Configuration.hpp
  FixedBufferPoolImpl.tpp
  Histogram.h
  Histogram.hpp
  elen.h
  elen.hpp
  elen.tpp
  enable_if_t.hpp
  mlog2.h
  mlogfacs2.h)

# DebugBufferPoolImpl is only installed in Debug mode
# FixedBufferPool.hpp has to be modified to either include or not include the header
set(DEBUGBUFFERPOOLIMPL "")
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(DEBUGBUFFERPOOLIMPL "#include \"utils/DebugBufferPoolImpl.tpp\"")
  set(HEADERS ${HEADERS} DebugBufferPoolImpl.tpp)
endif()
configure_file(FixedBufferPool.hpp ${CMAKE_CURRENT_BINARY_DIR}/FixedBufferPool.hpp @ONLY)
set(HEADERS ${HEADERS} ${CMAKE_CURRENT_BINARY_DIR}/FixedBufferPool.hpp)

install(FILES
  ${HEADERS}
  DESTINATION include/utils)
