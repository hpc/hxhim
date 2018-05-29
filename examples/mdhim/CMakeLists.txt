#
# Create the MDHIM examples
#
cmake_minimum_required(VERSION 2.8)

set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${LEVELDB_LIBRARY_DIRS})

add_executable(putget putget.c)
target_link_libraries(putget mdhim.so ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m stdc++)
set_target_properties(putget PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
set_target_properties(putget PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")

add_executable(cli cli.cpp util.cpp put.cpp get.cpp bput.cpp bget.cpp del.cpp bdel.cpp)
target_link_libraries(cli mdhim.so ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m)
set_target_properties(cli PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
set_target_properties(cli PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
INSTALL(TARGETS cli DESTINATION bin/mdhim)

# Scripts
# different_num_processes.sh requires ../generate_kv_pairs.sh
configure_file(different_num_processes.sh ${CMAKE_CURRENT_BINARY_DIR}/different_num_processes.sh COPYONLY)
INSTALL(PROGRAMS different_num_processes.sh DESTINATION bin/mdhim)
