#
# Create the demo executable
#
cmake_minimum_required(VERSION 2.8)
message( STATUS "GTest source dir: " ${gtest_SOURCE_DIR})
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

# MPI Tests need to run differently than regular tests
message( STATUS "MPI_FOUND=" ${MPI_FOUND})
if (MPI_FOUND)
    message( STATUS "MPI_CXX_LIBRARIES=" ${MPI_CXX_LIBRARIES})
    message( STATUS "MPI_CXX_COMPILE_FLAGS=" ${MPI_CXX_COMPILE_FLAGS})
    message( STATUS "MPI_CXX_LINK_FLAGS=" ${MPI_CXX_LINK_FLAGS})
    file(GLOB MDHIM_TEST_SOURCES *.c*)
    add_executable(mdhim-tests ${MDHIM_TEST_SOURCES})
    set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${LEVELDB_LIBRARY_DIRS})
    target_link_libraries(mdhim-tests mdhim mdhimutils transport ${LEVELDB_LIBRARY} gtest gtest_main ${MPI_CXX_LIBRARIES} thallium)
    set_target_properties(mdhim-tests PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
    set_target_properties(mdhim-tests PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
    #set(UNIT_TEST hxhim-transport-tests)
    #set(MPI_TRANSPORTAND mpirun -np 2 ${UNIT_TEST})
    #add_custom_transportand(TARGET ${UNIT_TEST} POST_BUILD TRANSPORTAND ${MPI_TRANSPORTAND})
endif()
