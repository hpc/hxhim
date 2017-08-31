#
# Create the demo executable
#
cmake_minimum_required(VERSION 2.8)
MESSAGE( STATUS "GTest source dir: " ${gtest_SOURCE_DIR})
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

# MPI Tests need to run differently than regular tests
MESSAGE( STATUS "MPI Found: " ${MPI_FOUND})
if (MPI_FOUND)
    MESSAGE( STATUS "MPI CXX Link Flags: " ${MPI_CXX_LINK_FLAGS})
    MESSAGE( STATUS "MPI CXX Compile flags: " ${MPI_CXX_COMPILE_FLAGS})
    MESSAGE( STATUS "MPI CXX Libraries: " ${MPI_CXX_LIBRARIES})
    add_executable(hxhim-comm-mpi-tests comm_mpi_test.cpp ../../src/comm/comm_mpi.cpp)
    target_link_libraries(hxhim-comm-mpi-tests gtest gtest_main ${MPI_CXX_LIBRARIES})
    set_target_properties(hxhim-comm-mpi-tests PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
    set_target_properties(hxhim-comm-mpi-tests PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
    #set(UNIT_TEST hxhim-comm-tests)
    #set(MPI_COMMAND mpirun -np 2 ${UNIT_TEST})
    #add_custom_command(TARGET ${UNIT_TEST} POST_BUILD COMMAND ${MPI_COMMAND})
endif()

