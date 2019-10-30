cmake_minimum_required (VERSION 3.6.3)

#
# Create the demo executable
#
# MPI Tests need to run differently than regular tests
add_executable(hxhim-demo hxhim-demo.cxx mesh.cpp meshgen.cc)
target_include_directories(hxhim-demo BEFORE PRIVATE ${CMAKE_BINARY_DIR}/include ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(hxhim-demo ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES})
set_target_properties(hxhim-demo PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
set_target_properties(hxhim-demo PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
