cmake_minimum_required (VERSION 3.6.3)

set(DATASTORE_SOURCES
  datastore.cpp
  datastores.cpp
  InMemory.cpp
  transform.cpp
)

if (LEVELDB_FOUND AND ENABLE_LEVELDB)
  list(APPEND DATASTORE_SOURCES
    leveldb.cpp
  )
endif()

if (ROCKSDB_FOUND AND ENABLE_ROCKSDB)
  list(APPEND DATASTORE_SOURCES
    rocksdb.cpp
  )
endif()

foreach(MESSAGE ${DATASTORE_SOURCES})
  target_sources(hxhim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/${MESSAGE})
endforeach()
