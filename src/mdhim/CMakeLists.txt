#
# Copy the default configuration file to the build directory
#
configure_file(mdhim.conf ${CMAKE_BINARY_DIR} COPYONLY)

#
# Create the mdhim library
#
file(GLOB LIBMDHIM_SOURCES *.cpp)
MESSAGE( STATUS "LIBMDHIM_SOURCES: " ${LIBMDHIM_SOURCES})
MESSAGE( STATUS "INCLUDE_DIRECTORIES: " ${INCLUDE_DIRECTORIES})
add_library(mdhim STATIC ${LIBMDHIM_SOURCES})
#add_library(mdhim.dylib DYNAMIC ${LIBMDHIM_SOURCES})
INSTALL(TARGETS mdhim DESTINATION lib)
INSTALL(FILES mdhim.h index_struct.h mdhim_config.h mdhim_constants.h mdhim_options.h mdhim_options_struct.h mdhim_struct.h range_server_info_struct.h DESTINATION include/mdhim)
