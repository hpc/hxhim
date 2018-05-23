#
# Create the hxhim library
#
file(GLOB LIBHXHIM_SOURCES *.cpp)
MESSAGE( STATUS "LIBHXHIM_SOURCES: " ${LIBHXHIM_SOURCES})
MESSAGE( STATUS "INCLUDE_DIRECTORIES: " ${INCLUDE_DIRECTORIES})
add_library(hxhim.a STATIC ${LIBHXHIM_SOURCES})
add_library(hxhim.so SHARED ${LIBHXHIM_SOURCES})
set_target_properties(hxhim.a PROPERTIES OUTPUT_NAME hxhim)
set_target_properties(hxhim.so PROPERTIES OUTPUT_NAME hxhim)
INSTALL(TARGETS hxhim.a hxhim.so DESTINATION lib)
INSTALL(FILES hxhim-types.h hxhim.h hxhim.hpp return.h return.hpp DESTINATION include/hxhim)
