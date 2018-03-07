#
# Create the transport directory
#
set(SOURCES mlog2.c)

#
# Create the transport library
#
file(GLOB LIBTRANSPORT_SOURCES *.c*)
MESSAGE( STATUS "LIBTRANSPORT_SOURCES: " ${LIBMDHIM_SOURCES})
MESSAGE( STATUS "INCLUDE_DIRECTORIES: " ${INCLUDE_DIRECTORIES})
add_library(transport STATIC ${LIBTRANSPORT_SOURCES})
#add_library(mdhim.dylib DYNAMIC ${LIBTRANSPORT_SOURCES})
