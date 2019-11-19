cmake_minimum_required (VERSION 3.6.3)

find_package(HDF5 REQUIRED)
message(STATUS "HDF5_INCLUDE_DIRS=" ${HDF5_INCLUDE_DIRS})
message(STATUS "HDF5_LIBRARIES=" ${HDF5_LIBRARIES})

set(VOL_SOURCES
  dataset.c
  dataset.h
  double3.c
  double3.h
  example.c
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

# create the example executable
add_executable(hxhim_vol_example example.c)
add_dependencies(hxhim_vol_example hxhim.a hxhim_vol)
target_include_directories(hxhim_vol_example PUBLIC ${CMAKE_SOURCE_DIR}/include ${HDF5_INCLUDE_DIRS} ${MPI_C_COMPILE_FLAGS})
target_link_libraries(hxhim_vol_example hxhim_vol hxhim.a ${HDF5_LIBRARIES} ${LEVELDB_LIBRARY} ${MPI_C_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m stdc++)
