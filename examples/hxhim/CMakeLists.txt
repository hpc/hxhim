#
# Create the MDHIM examples
#
cmake_minimum_required(VERSION 2.8)

set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${LEVELDB_LIBRARY_DIRS})

add_executable(hx-c hx.c print_results.c)
target_link_libraries(hx-c hxhim.a mdhim.a ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m stdc++)
set_target_properties(hx-c PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
set_target_properties(hx-c PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
INSTALL(TARGETS hx-c DESTINATION bin/hxhim)

add_executable(hx-c++ hx.cpp)
target_link_libraries(hx-c++ hxhim.a mdhim.a ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m)
set_target_properties(hx-c++ PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
set_target_properties(hx-c++ PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
INSTALL(TARGETS hx-c++ DESTINATION bin/hxhim)

add_executable(puts puts.c)
target_link_libraries(puts hxhim.a mdhim.a ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m stdc++)
set_target_properties(puts PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
set_target_properties(puts PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
INSTALL(TARGETS puts DESTINATION bin/hxhim)

add_executable(hxhim-cli cli.cpp print_results.c $<TARGET_OBJECTS:util.o>)
target_link_libraries(hxhim-cli hxhim.a mdhim.a ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m)
set_target_properties(hxhim-cli PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
set_target_properties(hxhim-cli PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
set_target_properties(hxhim-cli PROPERTIES OUTPUT_NAME cli)
INSTALL(TARGETS hxhim-cli DESTINATION bin/hxhim)

add_executable(lowest lowest.c)
target_link_libraries(lowest hxhim.a mdhim.a ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m stdc++)
set_target_properties(lowest PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
set_target_properties(lowest PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
INSTALL(TARGETS lowest DESTINATION bin/hxhim)

include(TestBigEndian)
test_big_endian(LEVELDB_IS_BIG_ENDIAN)
set(LEVELDB_ROOT $ENV{HOME}/leveldb)
add_executable(db_bench db_bench.cc ${LEVELDB_ROOT}/util/histogram.cc ${LEVELDB_ROOT}/util/testutil.cc)
target_link_libraries(db_bench hxhim.a mdhim.a ${LEVELDB_LIBRARY} ${MPI_CXX_LIBRARIES} ${THALLIUM_LDFLAGS} pthread m stdc++)
set_target_properties(db_bench PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS} -I${LEVELDB_ROOT} -DLEVELDB_PLATFORM_POSIX -DLEVELDB_IS_BIG_ENDIAN=${LEVELDB_IS_BIG_ENDIAN}")
set_target_properties(db_bench PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
INSTALL(TARGETS db_bench DESTINATION bin/hxhim)

# Scripts
# generate_commands.sh requires generate_spo.sh
configure_file(generate_spo.sh ${CMAKE_CURRENT_BINARY_DIR}/generate_spo.sh COPYONLY)
INSTALL(PROGRAMS generate_spo.sh DESTINATION bin/hxhim)

configure_file(generate_commands.sh ${CMAKE_CURRENT_BINARY_DIR}/generate_commands.sh COPYONLY)
INSTALL(PROGRAMS generate_commands.sh DESTINATION bin/hxhim)
