# HXHIM
## The Hexadimensional Hashing Indexing Middleware
----

### Contact
hxhim@lanl.gov

### Dependencies
* C++14 Compiler
* CMake 3
* MPI
* (optional) LevelDB: https://github.com/google/leveldb
* (optional) thallium: https://xgitlab.cels.anl.gov/sds/thallium

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
* The number of databases must remain the same across all runs for the stored data to make sense.
* All pointers that are passed into HXHIM are still owned by the caller.
* All pointers that come from HXHIM must be freed through their 'hxhim_*_destroy' functions.
