cmake_minimum_required (VERSION 3.6.3)

set(VOL_SOURCES
  dataset.c
  dataset.h
  double3.c
  double3.h
  file.c
  file.h
  float3.c
  float3.h
  group.c
  group.h
  hxhim_vol.c
  hxhim_vol.h
  info.c
  info.h
  init_term.c
  init_term.h
  structs.h
)

INSTALL(FILES hxhim_vol.h DESTINATION include/vol)

# create the static library
add_library(hxhim_vol STATIC ${VOL_SOURCES})
target_include_directories(hxhim_vol PUBLIC ${CMAKE_SOURCE_DIR}/include ${HDF5_INCLUDE_DIRS} ${MPI_C_COMPILE_FLAGS} ${MPI_CXX_COMPILE_FLAGS})
add_dependencies(hxhim_vol hxhim.a)
INSTALL(TARGETS hxhim_vol DESTINATION lib)
