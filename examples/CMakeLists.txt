#
# Create the demo executable
#
cmake_minimum_required(VERSION 2.8)

# MPI Tests need to run differently than regular tests
message( STATUS "MPI_FOUND=" ${MPI_FOUND})
if (MPI_FOUND)
    message( STATUS "MPI_CXX_LIBRARIES=" ${MPI_CXX_LIBRARIES})
    message( STATUS "MPI_CXX_COMPILE_FLAGS=" ${MPI_CXX_COMPILE_FLAGS})
    message( STATUS "MPI_CXX_LINK_FLAGS=" ${MPI_CXX_LINK_FLAGS})
    add_executable(putget putget.c)
    set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${LEVELDB_LIBRARY_DIRS})
    target_link_libraries(putget mdhim mdhimutils transport ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} pthread m thallium)
    set_target_properties(putget PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
    set_target_properties(putget PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
endif()
