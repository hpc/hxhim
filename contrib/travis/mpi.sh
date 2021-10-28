#!/usr/bin/env bash

if [[ "$#" -lt 3 ]]
then
    echo "Syntax: $0 DL_DIR BUILD_NAME INSTALL_DIR [PROCS]" 1>&2
    exit 1
fi

DL_DIR="$1"
BUILD_NAME="$2"
INSTALL_DIR="$3"
PROCS="$4"

cd "${DL_DIR}"
git clone --recurse-submodules --depth 1 https://github.com/open-mpi/ompi.git
cd ompi
./autogen.pl
mkdir "${BUILD_NAME}"
cd "${BUILD_NAME}"
MPI_PREFIX="${INSTALL_DIR}/openmpi-github"
../configure --prefix="${MPI_PREFIX}" --disable-man-pages --enable-threads=multiple
make -j ${PROCS}
make -j ${PROCS} install

# environment variables are discovered later
