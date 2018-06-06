#
# Create the MDHIM examples
#
cmake_minimum_required(VERSION 2.8)

set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${LEVELDB_LIBRARY_DIRS})

add_executable(putget putget.c)
target_link_libraries(putget mdhim.a ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m stdc++)
set_target_properties(putget PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
set_target_properties(putget PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")

add_executable(mdhim-cli cli.cpp $<TARGET_OBJECTS:util.o> put.cpp get.cpp bput.cpp bget.cpp del.cpp bdel.cpp)
target_link_libraries(mdhim-cli mdhim.a ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m)
set_target_properties(mdhim-cli PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
set_target_properties(mdhim-cli PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
set_target_properties(mdhim-cli PROPERTIES OUTPUT_NAME cli)
INSTALL(TARGETS mdhim-cli DESTINATION bin/mdhim)

# Scripts
# generate_commands.sh and different_num_processes.sh require generate_kv_pairs.sh
configure_file(generate_kv_pairs.sh ${CMAKE_CURRENT_BINARY_DIR}/generate_kv_pairs.sh COPYONLY)
INSTALL(PROGRAMS generate_kv_pairs.sh DESTINATION bin/mdhim)

configure_file(generate_commands.sh ${CMAKE_CURRENT_BINARY_DIR}/random.sh COPYONLY)
INSTALL(PROGRAMS random.sh DESTINATION bin/mdhim)

configure_file(different_num_processes.sh ${CMAKE_CURRENT_BINARY_DIR}/different_num_processes.sh COPYONLY)
INSTALL(PROGRAMS different_num_processes.sh DESTINATION bin/mdhim)
