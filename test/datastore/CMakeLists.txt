cmake_minimum_required(VERSION 3.6.3)

set(DS_TEST_SRC
  GetStats.cpp
  Histogram.cpp
  InMemory.cpp
  rm_r.cpp
  rm_r.hpp
  triples.hpp
  triples.cpp
)

if (LEVELDB_FOUND AND ENABLE_LEVELDB)
  list(APPEND DS_TEST_SRC
    LevelDB.cpp
  )
endif()

if (ROCKSDB_FOUND AND ENABLE_ROCKSDB)
  list(APPEND DS_TEST_SRC
    RocksDB.cpp
  )
endif()

foreach(FILE ${DS_TEST_SRC})
  target_sources(googletest PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/${FILE})
endforeach()
