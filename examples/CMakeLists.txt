#
# Create the demo executable
#
cmake_minimum_required(VERSION 2.8)

set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${LEVELDB_LIBRARY_DIRS})

add_executable(putget putget.c)
target_link_libraries(putget mdhim mdhimutils transport ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m)
set_target_properties(putget PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
set_target_properties(putget PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")

add_executable(cli cli.cpp input.cpp put.cpp get.cpp bput.cpp  bget.cpp)
target_link_libraries(cli mdhim mdhimutils transport ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m)
set_target_properties(cli PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
set_target_properties(cli PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
