#!/usr/bin/env bash

if [[ "$#" -lt 3 ]]
then
    echo "Syntax: $0 DL_DIR BUILD_NAME INSTALL_DIR" 1>&2
    exit 1
fi

DL_DIR="$1"
BUILD_NAME="$2"
INSTALL_DIR="$3"

cd "${DL_DIR}"
wget https://download.open-mpi.org/release/open-mpi/v4.0/openmpi-4.0.5.tar.bz2
tar -xf openmpi-4.0.5.tar.bz2
cd openmpi-4.0.5
mkdir "${BUILD_NAME}"
cd "${BUILD_NAME}"
MPI_PREFIX="${INSTALL_DIR}/mpi"
../configure --prefix="${MPI_PREFIX}"
make -j $(nproc --all)
make -j $(nproc --all) install

# environment variables are discovered later
