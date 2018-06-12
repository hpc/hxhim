#
# Install configuration files
#
cmake_minimum_required (VERSION 3.6.3)

configure_file(mdhim.conf ${CMAKE_BINARY_DIR} COPYONLY)
configure_file(hxhim.conf ${CMAKE_BINARY_DIR} COPYONLY)
