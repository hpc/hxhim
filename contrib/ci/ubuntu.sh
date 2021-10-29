#!/usr/bin/env bash

set -e

# openmpi or mpich
mpi="$1"
case "${mpi}" in
    "openmpi" | "mpich")
        ;;
    *)
        echo "Error: Unknown MPI name: ${mpi}" 1>&2
        exit 1
        ;;
esac

apt-get update -y
apt-get install -y software-properties-common

add-apt-repository -y ppa:ubuntu-toolchain-r/test
apt-get update -y

apt-get install -y \
    automake       \
    bzip2          \
    flex           \
    g++-11         \
    gfortran-11    \
    git            \
    libltdl-dev    \
    lib${mpi}-dev  \
    libsnappy-dev  \
    libtool-bin    \
    pkg-config     \
    python3-pip    \
    wget

pip3 install --upgrade cmake

MPI_PREFIX="/usr/lib/x86_64-linux-gnu/${mpi}"
export LD_LIBRARY_PATH="${MPI_PREFIX}/lib:${LD_LIBRARY_PATH}"
export PKG_CONFIG_PATH="${MPI_PREFIX}/lib/pkgconfig:${PKG_CONFIG_PATH}"
