#
# Create the demo executable
#
# MPI Tests need to run differently than regular tests
add_executable(hxhim-demo hxhim-demo.cxx mesh.cpp meshgen.cc)
if (MPI_FOUND)
    target_link_libraries(hxhim-demo mdhim mdhimutils ${LEVELDB_LIBRARY} gtest gtest_main ${MPI_CXX_LIBRARIES})
    set_target_properties(hxhim-demo PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
    set_target_properties(hxhim-demo PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
endif()
