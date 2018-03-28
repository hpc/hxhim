# HXHIM
## The Hexadimensional Hashing Indexing Middleware
----

### Contact
hxhim@lanl.gov

### Prerequisites
* C++14 Compiler
* CMake 3
* MPI
* LevelDB: https://github.com/google/leveldb
  * `contrib/hxhim_leveldb_install.sh` can be used to download and install LevelDB
  * Add LevelDB pkg-config to PKG_CONFIG_PATH (as shown in script output)
* thallium: https://xgitlab.cels.anl.gov/sds/thallium

### Build
```
mkdir bld
cd bld
cmake3 ..
make
make install
```
