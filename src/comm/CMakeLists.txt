#
# Create the comm directory
#
set(SOURCES mlog2.c)

#
# Create the comm library
#
file(GLOB LIBCOMM_SOURCES *.c*)
MESSAGE( STATUS "LIBCOMM_SOURCES: " ${LIBMDHIM_SOURCES})
MESSAGE( STATUS "INCLUDE_DIRECTORIES: " ${INCLUDE_DIRECTORIES})
add_library(comm STATIC ${LIBCOMM_SOURCES})
#add_library(mdhim.dylib DYNAMIC ${LIBCOMM_SOURCES})
