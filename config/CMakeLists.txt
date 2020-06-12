#
# Install configuration files
#
cmake_minimum_required (VERSION 3.6.3)

configure_file(hxhim.conf ${CMAKE_BINARY_DIR} COPYONLY)

# create the pkg-config file
string(REPLACE ";" " " PC_CFLAGS "${PC_CFLAGS}")
string(REPLACE ";" " " PC_LIBS "${PC_LIBS}")
string(REPLACE ";" " " PC_STATIC_LIBS "${PC_STATIC_LIBS}")
configure_file(hxhim.pc ${CMAKE_BINARY_DIR}/hxhim.pc @ONLY)
install(FILES
  ${CMAKE_BINARY_DIR}/hxhim.pc
  DESTINATION lib/pkgconfig)
