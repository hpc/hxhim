#
# Create the hxhim library
#
file(GLOB LIBHXHIM_SOURCES *.cpp)
MESSAGE( STATUS "LIBHXHIM_SOURCES: " ${LIBHXHIM_SOURCES})
MESSAGE( STATUS "INCLUDE_DIRECTORIES: " ${INCLUDE_DIRECTORIES})
add_library(hxhim STATIC ${LIBHXHIM_SOURCES})

INSTALL(FILES hxhim-types.h hxhim.h hxhim.hpp return.h return.hpp DESTINATION include/hxhim)
