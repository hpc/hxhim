#
# Create the MDHIM examples
#
cmake_minimum_required(VERSION 2.8)

set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${LEVELDB_LIBRARY_DIRS})

add_library(print_results OBJECT print_results.c print_results.h)
add_library(spo_gen OBJECT spo_gen.c spo_gen.h)

add_executable(hx-c hx.c print_results spo_gen)
target_link_libraries(hx-c hxhim.a ${EXEC_LDFLAGS} stdc++)

add_executable(hx-c++ hx.cpp spo_gen)
target_link_libraries(hx-c++ hxhim.a ${EXEC_LDFLAGS})

# add_executable(puts puts.c)
# target_link_libraries(puts hxhim.a ${EXEC_LDFLAGS} stdc++)

add_executable(hxhim-cli cli.cpp print_results)
target_link_libraries(hxhim-cli hxhim.a ${EXEC_LDFLAGS})
set_target_properties(hxhim-cli PROPERTIES OUTPUT_NAME cli)

add_executable(lowest lowest.c)
target_link_libraries(lowest hxhim.a ${EXEC_LDFLAGS} stdc++)

if (LEVELDB_FOUND AND ENABLE_LEVELDB)
  include(TestBigEndian)
  test_big_endian(LEVELDB_IS_BIG_ENDIAN)
  set(LEVELDB_ROOT $ENV{HOME}/leveldb)
  add_executable(db_bench db_bench.cc ${LEVELDB_ROOT}/util/histogram.cc ${LEVELDB_ROOT}/util/testutil.cc)
  target_link_libraries(db_bench hxhim.a ${EXEC_LDFLAGS} stdc++)
  set_target_properties(db_bench PROPERTIES COMPILE_FLAGS "-I${LEVELDB_ROOT} -DLEVELDB_PLATFORM_POSIX -DLEVELDB_IS_BIG_ENDIAN=${LEVELDB_IS_BIG_ENDIAN}")
endif()

# Scripts
# generate_commands.sh requires generate_spo.sh
configure_file(generate_spo.sh ${CMAKE_CURRENT_BINARY_DIR}/generate_spo.sh COPYONLY)

configure_file(generate_commands.sh ${CMAKE_CURRENT_BINARY_DIR}/generate_commands.sh COPYONLY)
