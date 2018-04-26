#!/usr/bin/env bash

function print_usage {
  echo "Usage: $(basename $0) install_path [leveldb_src]"
  echo "if leveldb_src does not exist, LevelDB will be cloned"
  echo "You must have create and write permission to install_path"
}

function emit_pkgconfig {
  prefix=$1
  pkgconfig_dir="${prefix}/lib"
  pkgconfig_fname="${pkgconfig_dir}/leveldb.pc"

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

  echo "INFO: Ensure PKG_CONFIG_PATH includes $pkgconfig_dir"
  echo "INFO: Current PKG_CONFIG_PATH="$PKG_CONFIG_PATH
}

# Download and build LevelDB
function download_and_build {
  prefix="$1"
  ldbsrc="$2"
    
  if [ ! -d $2 ]; then
    git clone https://github.com/google/leveldb.git $2
  fi
  mkdir -p $2/build
  cd $2/build
  cmake -DCMAKE_INSTALL_PREFIX=$1 ..
  make -j $(nproc --all)
  make -j $(nproc --all) install
  cd ../..
}

function install_leveldb {
  prefix="$1"
  ldbsrc="$2"

  # If the location of LevelDB was not provided,
  # assume that it is in the current directory
  if [ "$ldbsrc" = "" ]; then
    ldbsrc="./leveldb"
  fi

  # Download and build LevelDB
  download_and_build $prefix $ldbsrc

  # Emit a PkgConfig
  emit_pkgconfig "$(realpath $prefix)"
}

if [ "$#" -lt "1" ]; then
  print_usage
  exit 1
fi

install_leveldb $1 $2
exit 0
