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
* thallium: https://xgitlab.cels.anl.gov/sds/thallium

LevelDB and thallium can be installed through the `contrib/hxhim_dependencies.sh` script. `PKG_CONFIG_PATH` will need to be modified (specified in the script output)

### Build
```
mkdir bld
cd bld
cmake ..
make
make install
```

### Usage Notes
Always remember to clear out old manifest files if there are overlapping names between runs

All pointers that are passed into HXHIM are still owned by the caller.
All pointers returned by HXHIM must be deleted through HXHIM.
