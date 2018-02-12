#!/usr/bin/env bash

function print_usage {
  echo "Usage: $(basename $0) install_path"
  echo "Current working directory should be LevelDB src directory"
  echo "You must have create and write permission to install_path"
}

function emit_pkgconfig {
  prefix=$1
  pkgconfig_fname="${prefix}/lib/leveldb.pc"

  echo "prefix=${prefix}" > $pkgconfig_fname
  echo "exec_prefix=\${prefix}" >> $pkgconfig_fname
  echo "includedir=\${prefix}/include" >> $pkgconfig_fname
  echo "libdir=\${prefix}/lib" >> $pkgconfig_fname
  echo "" >> $pkgconfig_fname
  echo "Name: leveldb" >> $pkgconfig_fname
  echo "Description: The leveldb library" >> $pkgconfig_fname
  echo "Version: Git Master" >> $pkgconfig_fname
  echo "Cflags: -I\${includedir}" >> $pkgconfig_fname
  echo "Libs: -L\${libdir} -lleveldb" >> $pkgconfig_fname

  echo "INFO: Ensure PKG_CONFIG_PATH includes $pkgconfig_fname"
  echo "INFO: Current PKG_CONFIG_PATH="$PKG_CONFIG_PATH
}

function install_leveldb {
  ldbsrc="$1"
  prefix="$2"

  # Copy binaries
  mkdir -p $prefix/bin
  cp $ldbsrc/leveldbutil $prefix/bin

  # Install include files
  mkdir -p $prefix/include
  cp -r $ldbsrc/include/leveldb $prefix/include/

  # Install library files
  mkdir -p $prefix/lib
  cp $ldbsrc/libleveldb* $prefix/lib
  cp $ldbsrc/libmemenv* $prefix/lib

  # Emit a PkgConfig
  emit_pkgconfig $prefix
}

if [ "$1" = "" ]; then
  print_usage
  exit 1
fi

install_leveldb . $1
exit 0
