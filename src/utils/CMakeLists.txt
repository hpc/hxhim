#
# Create the utils directory
#
set(SOURCES mlog2.c)
#
# Create the mdhim library
#
file(GLOB LIBMDHIMUTILS_SOURCES *.c *.cpp)
MESSAGE( STATUS "LIBMDHIMUTILS_SOURCES: " ${LIBMDHIMUTILS_SOURCES})
MESSAGE( STATUS "INCLUDE_DIRECTORIES: " ${INCLUDE_DIRECTORIES})
add_library(mdhimutils STATIC ${LIBMDHIMUTILS_SOURCES})
#add_library(mdhim.dylib DYNAMIC ${LIBMDHIM_SOURCES})
