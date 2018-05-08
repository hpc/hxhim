#
# Create the demo executable
#
cmake_minimum_required(VERSION 2.8)

set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${LEVELDB_LIBRARY_DIRS})

add_executable(putget putget.c)
target_link_libraries(putget mdhim transport utils ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m)
set_target_properties(putget PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
set_target_properties(putget PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")

add_executable(cli cli.cpp util.cpp put.cpp get.cpp bput.cpp bget.cpp del.cpp bdel.cpp)
target_link_libraries(cli mdhim transport utils ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m)
set_target_properties(cli PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
set_target_properties(cli PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
configure_file(random.sh ${CMAKE_CURRENT_BINARY_DIR}/random.sh COPYONLY)
configure_file(different_num_processes.sh ${CMAKE_CURRENT_BINARY_DIR}/different_num_processes.sh COPYONLY)
INSTALL(TARGETS cli DESTINATION bin)
INSTALL(FILES random.sh different_num_processes.sh DESTINATION bin PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ)
