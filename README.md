# HXHIM
## The Hexadimensional Hashing Indexing Middleware
----

### Contact
hxhim@lanl.gov

### Prerequisites
* C++11 Compiler
* CMake 3
* MPI
* LevelDB: https://github.com/google/leveldb
  * `contrib/hxhim_leveldb_install.sh` can be used to download and install LevelDB
  * Add LevelDB pkg-config to PKG_CONFIG_PATH (as shown in script output)
* thallium: https://xgitlab.cels.anl.gov/sds/thallium
  * thallium/share/cmake/thallium/thallium-config.cmake will need a few extra lines:
    ```
    set (THALLIUM_FOUND true)
    set (THALLIUM_INCLUDE_PATH ${CMAKE_CURRENT_LIST_DIR}/../../../include)
    set (THALLIUM_LIBRARY_DIRS ${CMAKE_CURRENT_LIST_DIR}/../../../lib)
    ```

### Build
```
mkdir bld
cd bld
cmake3 ..
make
make install
```
