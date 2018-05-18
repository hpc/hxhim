#
# Create the demo executable
#
cmake_minimum_required(VERSION 2.8)

set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${LEVELDB_LIBRARY_DIRS})

add_executable(hx-c hx.c)
target_link_libraries(hx-c hxhim mdhim transport utils ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m)
set_target_properties(hx-c PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
set_target_properties(hx-c PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
INSTALL(TARGETS hx-c DESTINATION bin)

add_executable(hx-c++ hx.cpp)
target_link_libraries(hx-c++ hxhim mdhim transport utils ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m)
set_target_properties(hx-c++ PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
set_target_properties(hx-c++ PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
INSTALL(TARGETS hx-c++ DESTINATION bin)