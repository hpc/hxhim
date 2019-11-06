cmake_minimum_required (VERSION 3.6.3)

find_package(HDF5 REQUIRED)
message(STATUS "HDF5_INCLUDE_DIRS=" ${HDF5_INCLUDE_DIRS})
message(STATUS "HDF5_LIBRARIES=" ${HDF5_LIBRARIES})

set(VOL_SOURCES
  dataset.c
  double3.c
  file.c
  float3.c
  hxhim_vol.c
  info.c
  initialize.c
  terminate.c
)

INSTALL(FILES hxhim_vol.h DESTINATION include/vol)

# create the static library
add_library(hxhim_vol_lib STATIC ${VOL_SOURCES})
target_include_directories(hxhim_vol_lib PUBLIC ${CMAKE_SOURCE_DIR}/include ${HDF5_INCLUDE_DIRS} ${MPI_C_COMPILE_FLAGS} ${MPI_CXX_COMPILE_FLAGS})
add_dependencies(hxhim_vol_lib hxhim.a)
INSTALL(TARGETS hxhim_vol_lib DESTINATION lib)

# create the example executable
add_executable(hxhim_vol_example example.c)
add_dependencies(hxhim_vol_example hxhim.a hxhim_vol_lib)
target_include_directories(hxhim_vol_example PUBLIC ${CMAKE_SOURCE_DIR}/include ${HDF5_INCLUDE_DIRS} ${MPI_C_COMPILE_FLAGS} ${MPI_CXX_COMPILE_FLAGS})
target_link_libraries(hxhim_vol_example hxhim_vol_lib hxhim.a ${HDF5_LIBRARIES} ${LEVELDB_LIBRARY} ${MPI_C_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m stdc++)
