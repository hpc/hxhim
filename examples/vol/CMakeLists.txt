cmake_minimum_required (VERSION 3.6.3)

# create the example hdf5 executable
add_executable(hxhim_vol_example example.c)
add_dependencies(hxhim_vol_example hxhim_vol)
target_include_directories(hxhim_vol_example PUBLIC ${CMAKE_SOURCE_DIR}/contrib/vol ${MPI_C_COMPILE_FLAGS})
target_link_libraries(hxhim_vol_example hxhim_vol hxhim.a ${HDF5_LIBRARIES} ${COMMON_LDFLAGS} stdc++)
