# HXHIM
## The Hexadimensional Hashing Indexing Middleware.
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

### Build
1. `mkdir bld`
2. `cd bld`
3. `cmake3 ..`
4. `make`
5. `make install`
