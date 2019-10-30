cmake_minimum_required (VERSION 3.6.3)

# get source files
add_subdirectory(base)
add_subdirectory(datastore)
add_subdirectory(hxhim)
add_subdirectory(transport)
add_subdirectory(utils)

#
# Create library
#
set(LIBHXHIM_SOURCES ${BASE_SRC} ${DATASTORE_SRC} ${HXHIM_SRC} ${TRANSPORT_SRC} ${UTILS_SRC})
list(REMOVE_ITEM LIBHXHIM_SOURCES **.tpp)
message(STATUS "LIBHXHIM_SOURCES: " ${LIBHXHIM_SOURCES})

add_library(hxhim OBJECT ${LIBHXHIM_SOURCES})
target_include_directories(hxhim PUBLIC ${CMAKE_BINARY_DIR}/include ${CMAKE_SOURCE_DIR}/include)

# create the static library
add_library(hxhim.a STATIC $<TARGET_OBJECTS:hxhim>)
set_target_properties(hxhim.a PROPERTIES OUTPUT_NAME hxhim)
INSTALL(TARGETS
  hxhim.a
  DESTINATION lib)

# create the dynamic library
if (NOT CMAKE_SYSTEM_NAME STREQUAL "CrayLinuxEnvironment")
   set_target_properties(hxhim PROPERTIES POSITION_INDEPENDENT_CODE ON)
   add_library(hxhim.so SHARED $<TARGET_OBJECTS:hxhim>)
   set_target_properties(hxhim.so PROPERTIES OUTPUT_NAME hxhim)

   install(TARGETS
     hxhim.so
     DESTINATION lib)
endif()
