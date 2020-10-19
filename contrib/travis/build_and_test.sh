#!/usr/bin/env bash

set -e

ROOT="$(realpath $(dirname ${BASH_SOURCE[0]}))"
ROOT="$(dirname ${ROOT})"
ROOT="$(dirname ${ROOT})"

export DL_DIR="$(realpath download)"
export BUILD_NAME="travis"
export INSTALL_DIR="$(realpath install)"

export CC="${C_COMPILER}"
export CXX="${CXX_COMPILER}"
export FC="${FORT_COMPILER}"
export GTEST_COLOR=1

# install cmake through pip instead of dnf
pip3 install --upgrade cmake

mkdir -p "${INSTALL_DIR}"

# install mpi manually
contrib/travis/mpi.sh "${DL_DIR}" "${BUILD_NAME}" "${INSTALL_DIR}" > /dev/null

# find mpi and add it to the environment
source contrib/travis/setup_env.sh "${INSTALL_DIR}"

# set up hxhim dependencies
source contrib/hxhim_dependencies.sh --OFI --NO_ROCKSDB --NO_JEMALLOC "${DL_DIR}" "${BUILD_NAME}" "${INSTALL_DIR}" "$(nproc --all)" > /dev/null

cd "${ROOT}"

mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug
make -j $(nproc --all)
mpirun -np 1 --allow-run-as-root test/googletest
