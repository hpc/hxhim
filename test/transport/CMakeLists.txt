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
    file(GLOB TRANSPORT_TEST_SOURCES *.c*)
    add_executable(transport-tests ${TRANSPORT_TEST_SOURCES})
    set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${LEVELDB_LIBRARY_DIRS})
    target_link_libraries(transport-tests mdhim transport mdhimutils ${LEVELDB_LIBRARY} gtest gtest_main ${MPI_CXX_LIBRARIES})
    set_target_properties(transport-tests PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
    set_target_properties(transport-tests PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
    #set(UNIT_TEST hxhim-transport-tests)
    #set(MPI_TRANSPORTAND mpirun -np 2 ${UNIT_TEST})
    #add_custom_transportand(TARGET ${UNIT_TEST} POST_BUILD TRANSPORTAND ${MPI_TRANSPORTAND})
endif()
