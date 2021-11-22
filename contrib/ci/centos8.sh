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

yum install -y dnf-plugins-core
yum config-manager --set-enabled powertools

yum install -y            \
    automake              \
    bzip2                 \
    cmake                 \
    fileutils             \
    flex                  \
    gcc-toolset-10        \
    git                   \
    libtool               \
    libtool-ltdl-devel    \
    ${mpi}-devel          \
    pkg-config            \
    python3-pip           \
    snappy-devel          \
    wget                  \
    which

source /opt/rh/gcc-toolset-10/enable

MPI_PREFIX="/usr/lib64/${mpi}"
export PATH="${MPI_PREFIX}/bin:${PATH}"
export LD_LIBRARY_PATH="${MPI_PREFIX}/lib:${LD_LIBRARY_PATH}"
export PKG_CONFIG_PATH="${MPI_PREFIX}/lib/pkgconfig:${PKG_CONFIG_PATH}"
