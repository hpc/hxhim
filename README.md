# HXHIM
## The Hexadimensional Hashing Indexing Middleware

[![Build Status](https://app.travis-ci.com/hpc/hxhim.svg?branch=master)](https://app.travis-ci.com/hpc/hxhim)

----

### License
HXHIM is licensed under the OSS and BSD-3 licenses. Please see [LICENSE](LICENSE) for details.

### Contact
hxhim@lanl.gov

### Dependencies
* C++11 Compiler
* CMake 3
* MPI
* (optional, requires C++14) thallium: https://xgitlab.cels.anl.gov/sds/thallium
* (optional) LevelDB: https://github.com/google/leveldb
* (optional) RocksDB: https://github.com/facebook/rocksdb

### Build
```
source contrib/hxhim_dependencies.sh --{BMI,CCI,OFI,SM} download_dir build_name install_dir [PROCS]
mkdir build
cd build
cmake .. -DCMAKE_INSTALL_PREFIX="${HXHIM_PREFIX}"
make
make install
export LD_LIBRARY_PATH="${HXHIM_PREFIX}/lib:${LD_LIBRARY_PATH}"
export PKG_CONFIG_PATH="${HXHIM_PREFIX}/lib/pkgconfig:${PKG_CONFIG_PATH}"
```

`contrib/hxhim_dependencies.sh` should be run first in order make sure
all dependencies are available.  The `build_name` argument of
`contrib/hxhim_dependencies.sh` is a string that will be appended to
`download_dir`, not a full path. `build` and `$(hostname)` are
recommended.

### Usage Notes
* The number of databases must remain the same across all runs for the stored data to make sense.
* All pointers that are passed into HXHIM are still owned by the caller.
* All pointers that come from HXHIM must be freed through their 'hxhim_*_destroy' functions.
