cmake_minimum_required (VERSION 3.6.3)

set(INSTALLED_HEADERS
  constants.hpp
)

set(NOT_INSTALLED_HEADERS
  datastore.hpp
  datastores.hpp
)

foreach(HEADER ${INSTALLED_HEADERS} ${NOT_INSTALLED_HEADERS})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${HEADER})
endforeach()

foreach(HEADER ${INSTALLED_HEADERS})
  install(FILES ${HEADER} DESTINATION include/datastore)
endforeach()
