#
# Create the demo executable
#
# MPI Tests need to run differently than regular tests
add_executable(hxhim-demo hxhim-demo.cxx)
message( STATUS "MPI_FOUND=" ${MPI_FOUND})
if (MPI_FOUND)
    message( STATUS "MPI_CXX_LIBRARIES=" ${MPI_CXX_LIBRARIES})
    message( STATUS "MPI_CXX_COMPILE_FLAGS=" ${MPI_CXX_COMPILE_FLAGS})
    message( STATUS "MPI_CXX_LINK_FLAGS=" ${MPI_CXX_LINK_FLAGS})
    set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${LEVELDB_LIBRARY_DIRS})
    target_link_libraries(hxhim-demo mdhim mdhimutils ${LEVELDB_LIBRARY} gtest gtest_main ${MPI_CXX_LIBRARIES})
    set_target_properties(hxhim-demo PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
    set_target_properties(hxhim-demo PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
endif()

#include_directories(BEFORE ../../include)
#add_executable(hxhim-demo hxhim-demo.cxx ../comm/comm_mpi.cpp)
#target_link_libraries(hxhim-demo)
