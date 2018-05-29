cmake_minimum_required(VERSION 3.6.3)

configure_file(generate_kv_pairs.sh ${CMAKE_CURRENT_BINARY_DIR}/generate_kv_pairs.sh COPYONLY)
INSTALL(PROGRAMS generate_kv_pairs.sh DESTINATION bin)

configure_file(random.sh ${CMAKE_CURRENT_BINARY_DIR}/random.sh COPYONLY)
INSTALL(PROGRAMS random.sh DESTINATION bin/mdhim)

add_subdirectory(mdhim)
add_subdirectory(hxhim)
