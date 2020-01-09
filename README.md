# HXHIM
## The Hexadimensional Hashing Indexing Middleware
----

### Contact
hxhim@lanl.gov

### Dependencies
* C++11 Compiler
* CMake 3
* MPI
* (optional) LevelDB: https://github.com/google/leveldb
* (optional, requires C++14) thallium: https://xgitlab.cels.anl.gov/sds/thallium

### Build
```
contrib/hxhim_dependencies.sh --{BMI,CCI,OFI,SM} download_dir install_dir
PKG_CONFIG_PATH=<output from script>:$PKG_CONFIG_PATH
mkdir bld
cd bld
cmake ..
make
make install
```

`contrib/hxhim_dependencies.sh` should be run first in order to allow for
CMake to be able to discover them.  If `contrib/hxhim_dependencies.sh` is
not run, the leveldb source must be available on the system (One of
the CMake targets requires files from leveldb that are not built into
the library).

### Usage Notes
* The number of databases must remain the same across all runs for the stored data to make sense.
* All pointers that are passed into HXHIM are still owned by the caller.
* All pointers that come from HXHIM must be freed through their 'hxhim_*_destroy' functions.
