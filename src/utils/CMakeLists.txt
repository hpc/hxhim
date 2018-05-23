#
# Create the utils directory
#
set(SOURCES mlog2.c)
#
# Create the mdhim library
#
file(GLOB LIBMDHIMUTILS_SOURCES *.c *.cpp)
list(FILTER LIBMDHIMUTILS_SOURCES EXCLUDE REGEX ".*/elen.cpp")
MESSAGE( STATUS "LIBMDHIMUTILS_SOURCES: " ${LIBMDHIMUTILS_SOURCES})
MESSAGE( STATUS "INCLUDE_DIRECTORIES: " ${INCLUDE_DIRECTORIES})
add_library(utils.a STATIC ${LIBMDHIMUTILS_SOURCES})
add_library(utils.so SHARED ${LIBMDHIMUTILS_SOURCES})
set_target_properties(utils.a PROPERTIES OUTPUT_NAME utils)
set_target_properties(utils.so PROPERTIES OUTPUT_NAME utils)
INSTALL(TARGETS utils.a utils.so DESTINATION lib)
INSTALL(FILES mlog2.h mlogfacs2.h uthash.h DESTINATION include/mdhim)
