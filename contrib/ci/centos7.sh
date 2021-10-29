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

yum install -y centos-release-scl epel-release

yum install -y           \
    automake             \
    bzip2                \
    cmake3               \
    devtoolset-10        \
    flex                 \
    git                  \
    libtool              \
    libtool-ltdl-devel   \
    ${mpi}-devel         \
    pkgconfig            \
    python3-pip          \
    snappy-devel         \
    wget                 \

cmake3_path="$(which cmake3)"
ln -sf "${cmake3_path}" "$(dirname ${cmake3_path})/cmake"

source /opt/rh/devtoolset-10/enable

MPI_PREFIX="/usr/lib64/${mpi}"
export PATH="${MPI_PREFIX}/bin:${PATH}"
export LD_LIBRARY_PATH="${MPI_PREFIX}/lib:${LD_LIBRARY_PATH}"
