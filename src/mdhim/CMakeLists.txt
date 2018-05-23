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
add_library(mdhim.a STATIC ${LIBMDHIM_SOURCES})
add_library(mdhim.so SHARED ${LIBMDHIM_SOURCES})
set_target_properties(mdhim.a PROPERTIES OUTPUT_NAME mdhim)
set_target_properties(mdhim.so PROPERTIES OUTPUT_NAME mdhim)
INSTALL(TARGETS mdhim.a mdhim.so DESTINATION lib)
INSTALL(FILES mdhim.h index_struct.h mdhim_config.h mdhim_constants.h mdhim_options.h mdhim_options_struct.h mdhim_struct.h range_server_info_struct.h DESTINATION include/mdhim)
