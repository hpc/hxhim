#
# Create the transport directory
#
set(SOURCES mlog2.c)

#
# Create the transport library
#
file(GLOB LIBMDHIMTRANSPORT_SOURCES *.c*)
MESSAGE( STATUS "LIBMDHIMTRANSPORT_SOURCES: " ${LIBMDHIMTRANSPORT_SOURCES})
MESSAGE( STATUS "INCLUDE_DIRECTORIES: " ${INCLUDE_DIRECTORIES})
add_library(transport STATIC ${LIBMDHIMTRANSPORT_SOURCES})
INSTALL(TARGETS transport DESTINATION lib)
INSTALL(FILES transport.h transport_constants.h DESTINATION include/mdhim)
