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
add_library(transport.a STATIC ${LIBMDHIMTRANSPORT_SOURCES})
add_library(transport.so SHARED ${LIBMDHIMTRANSPORT_SOURCES})
set_target_properties(transport.a PROPERTIES OUTPUT_NAME transport)
set_target_properties(transport.so PROPERTIES OUTPUT_NAME transport)
INSTALL(TARGETS transport.a transport.so DESTINATION lib)
INSTALL(FILES transport.h transport_constants.h DESTINATION include/mdhim)
