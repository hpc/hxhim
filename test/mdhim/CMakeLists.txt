#
# Create the MDHIM tests
#
cmake_minimum_required(VERSION 2.8)
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

file(GLOB MDHIM_TEST_SOURCES *.c* ../main.cpp)
add_executable(mdhim-tests ${MDHIM_TEST_SOURCES})
set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${LEVELDB_LIBRARY_DIRS})
target_link_libraries(mdhim-tests mdhim.so ${LEVELDB_LIBRARY} gtest gtest_main ${MPI_CXX_LIBRARIES} ${THALLIUM_LIBRARIES})
set_target_properties(mdhim-tests PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
set_target_properties(mdhim-tests PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS} ${THALLIUM_LDFLAGS}")
#set(UNIT_TEST hxhim-transport-tests)
#set(MPI_TRANSPORTAND mpirun -np 2 ${UNIT_TEST})
#add_custom_transportand(TARGET ${UNIT_TEST} POST_BUILD TRANSPORTAND ${MPI_TRANSPORTAND})
