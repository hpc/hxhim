#
# Copy the default configuration file to the build directory
#
configure_file(mdhim.conf ${CMAKE_BINARY_DIR} COPYONLY)

#
# Create the mdhim library
#
file(GLOB LIBMDHIM_SOURCES *.c*)
MESSAGE( STATUS "LIBMDHIM_SOURCES: " ${LIBMDHIM_SOURCES})
MESSAGE( STATUS "INCLUDE_DIRECTORIES: " ${INCLUDE_DIRECTORIES})
add_library(mdhim STATIC ${LIBMDHIM_SOURCES})
#add_library(mdhim.dylib DYNAMIC ${LIBMDHIM_SOURCES})
