cmake_minimum_required (VERSION 3.6.3)

set(INSTALLED_HEADERS
  constants.hpp
  transform.h
  transform.hpp
  transform.tpp
)

set(NOT_INSTALLED_HEADERS
  datastore.hpp
  datastores.hpp
)

if (LEVELDB_FOUND AND ENABLE_LEVELDB)
  list(APPEND NOT_INSTALLED_HEADERS
    LevelDB.hpp
  )
endif()

if (ROCKSDB_FOUND AND ENABLE_ROCKSDB)
  list(APPEND NOT_INSTALLED_HEADERS
    RocksDB.hpp
  )
endif()

foreach(HEADER ${INSTALLED_HEADERS} ${NOT_INSTALLED_HEADERS})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${HEADER})
endforeach()

foreach(HEADER ${INSTALLED_HEADERS})
  install(FILES ${HEADER} DESTINATION include/datastore)
endforeach()
