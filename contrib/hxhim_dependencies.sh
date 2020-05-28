#!/usr/bin/env bash
# HXHIM Dependency Installer
#
# This script checks for the files needed by HXHIM.
#
# CC and CXX can be set to change the compiler
#
# Executables (only checked for):
#     autoconf/automake/libtool
#     CMake (3.6.3+)
#     mpi (mpicc, mpicxx, mpirun, mpiexec)
#
# Libraries (only checked for):
#     libtool-ltdl-devel (libltdl.*)
#     mpi (libmpi.*)
#
# Git Repositories (checks and installs):
#     BMI (optional)
#     CCI (optional)
#     OFI (optional)
#     mercury
#     libev-devel (libev.*)
#     argobots
#     margo
#     thallium
#

SOURCE="$(dirname $(realpath $BASH_SOURCE[0]))"

set -e

function usage() {
    echo "Usage: $0 [Options] download_dir install_dir [PROCS]"
    echo ""
    echo "    Options:"
    echo "        --BMI  build the BMI module for mercury"
    echo "        --CCI  build the CCI module for mercury"
    echo "        --OFI  build the OFI module for mercury"
    echo "        --SM   build the SM  module for mercury"
}

# Check that an executable is available; if not, print the name and exit
function check_executable() {
    executable="$@"
    command -v ${executable} > /dev/null 2>&1 || (echo "${executable} not found"; exit 1)
}

function check_autoconf() {
    check_executable autoconf
    check_executable automake
    check_executable libtool
}

function check_cmake() {
    check_executable cmake

    # make sure CMake's version is at least 3.6.3
    cmake_version=$(cmake --version | head -n 1 | awk '{ print $3 }')
    [ "3.6.3" = $(echo -e "3.6.3\n${cmake_version}" | sort -V | head -n 1) ] || exit 1
}

# Check that a library is found; if not, print the library name and exit
function check_library() {
    library=$1
    ldconfig -N -v $(echo "${LD_LIBRARY_PATH}" | sed 's/:/ /g') 2> /dev/null | grep ${library} > /dev/null
    echo "$?"
}

function check_mpi() {
    check_executable mpicc
    check_executable mpicxx
    check_executable mpirun
    check_executable mpiexec
    if [[ $(check_library "libmpi\\..*") -ne 0 ]]
    then
        echo "mpi library not found"
        exit 1
    fi
}

function NA_BMI() {
    name=bmi
    download_dir=${WORKING_DIR}/${name}
    if [[ ! -d "${download_dir}" ]]; then
        git clone --depth 1 https://xgitlab.cels.anl.gov/sds/bmi.git ${download_dir}
    fi

    cd ${download_dir}
    git pull
    if [[ ! (-f configure && -x configure) ]]; then
        ./prepare
    fi
    mkdir -p build
    cd build
    install_dir=${PREFIX}/${name}
    PKG_CONFIG_PATH=${PKG_CONFIG_PATH} ../configure --prefix=${PREFIX}/${name} --enable-shared --enable-bmi-only
    make -j ${PROCS}
    make -j ${PROCS} install
    cd ../..

    BMI_INCLUDE_DIR=${install_dir}/include
    BMI_LIBRARY=${install_dir}/lib/libbmi.so
    PKG_CONFIG_PATH=${PKG_CONFIG_PATH}:${install_dir}/lib/pkgconfig
}

function NA_CCI() {
    name=cci
    download_dir=${WORKING_DIR}/${name}
    if [[ ! -d "${download_dir}" ]]; then
        git clone --depth 1 https://github.com/CCI/cci.git ${download_dir}
    fi

    cd ${download_dir}
    git pull
    if [[ ! (-f configure && -x configure) ]]; then
        ./autogen.pl
    fi
    mkdir -p build
    cd build
    install_dir=${PREFIX}/${name}
    PKG_CONFIG_PATH=${PKG_CONFIG_PATH} ../configure --prefix=${install_dir}
    make -j ${PROCS}
    make -j ${PROCS} install
    cd ../..

    CCI_INCLUDE_DIR=${install_dir}/include
    CCI_LIBRARY=${install_dir}/lib/libcci.so
    PKG_CONFIG_PATH=${PKG_CONFIG_PATH}:${install_dir}/lib/pkgconfig
}

function NA_OFI() {
    name=libfabric
    download_dir=${WORKING_DIR}/${name}
    if [[ ! -d "${download_dir}" ]]; then
        git clone --depth 1 https://github.com/ofiwg/libfabric.git ${download_dir}
    fi

    cd ${download_dir}
    git pull
    if [[ ! (-f configure && -x configure) ]]; then
        ./autogen.sh
    fi
    mkdir -p build
    cd build
    install_dir=${PREFIX}/${name}
    PKG_CONFIG_PATH=${PKG_CONFIG_PATH} ../configure --prefix=${install_dir}
    make -j ${PROCS}
    make -j ${PROCS} install
    cd ../..

    OFI_INCLUDE_DIR=${install_dir}/include
    OFI_LIBRARY=${install_dir}/lib/libfabric.so
    PKG_CONFIG_PATH=${PKG_CONFIG_PATH}:${install_dir}/lib/pkgconfig
}

function NA_SM() {
    return
}

function mercury() {
    check_cmake

    cmake_options=
    if [[ ! -z ${USE_NA_BMI+false} ]]; then
        echo BMI
        NA_BMI
        cmake_options="$cmake_options -DNA_USE_BMI:BOOL=ON -DBMI_INCLUDE_DIR=${BMI_INCLUDE_DIR} -DBMI_LIBRARY=${BMI_LIBRARY}"
    fi

    if [[ ! -z ${USE_NA_CCI+false} ]]; then
        echo CCI
        NA_CCI
        cmake_options="$cmake_options -DNA_USE_CCI:BOOL=ON -DCCI_INCLUDE_DIR=${CCI_INCLUDE_DIR} -DCCI_LIBRARY=${CCI_LIBRARY}"
    fi

    if [[ ! -z ${USE_NA_OFI+false} ]]; then
        echo OFI
        NA_OFI
        cmake_options="$cmake_options -DNA_USE_OFI:BOOL=ON -DOFI_INCLUDE_DIR=${OFI_INCLUDE_DIR} -DOFI_LIBRARY=${OFI_LIBRARY}"
    fi

    if [[ ! -z ${USE_NA_SM+false} ]]; then
        echo SM
        NA_SM
        cmake_options="$cmake_options -DNA_USE_SM:BOOL=ON"
    fi

    name=mercury
    download_dir=${WORKING_DIR}/${name}
    if [[ ! -d "${download_dir}" ]]; then
        git clone --depth 1 --recurse-submodules https://github.com/mercury-hpc/mercury.git ${download_dir}
        git submodule init && git submodule update
    fi

    cd ${download_dir}
    git pull
    mkdir -p build
    cd build
    rm -rf *
    install_dir=${PREFIX}/${name}
    PKG_CONFIG_PATH=${PKG_CONFIG_PATH} cmake -DCMAKE_INSTALL_PREFIX=${install_dir} -DMERCURY_USE_BOOST_PP:BOOL=ON -DBUILD_SHARED_LIBS:BOOL=ON $(echo "$cmake_options") ..
    make -j ${PROCS}
    make -j ${PROCS} install
    cd ../..

    MERCURY_DIR=${install_dir}
    PKG_CONFIG_PATH=${PKG_CONFIG_PATH}:${install_dir}/lib/pkgconfig
}

function libev() {
    # libev-devel
    if [[ $(check_library "libev\\..*") -eq 0 ]]
    then
        return 0;
    fi

    name=libev
    download_dir=${WORKING_DIR}/${name}
    if [[ ! -d "${download_dir}" ]]; then
        git clone --depth 1 https://github.com/enki/libev.git ${download_dir}
    fi

    cd ${download_dir}
    git pull
    if [[ ! (-f configure && -x configure) ]]; then
        ./autogen.sh
    fi
    mkdir -p build
    cd build
    install_dir=${PREFIX}/${name}
    PKG_CONFIG_PATH=${PKG_CONFIG_PATH} ../configure --prefix=${install_dir}
    make -j ${PROCS}
    make -j ${PROCS} install
    cd ../..

    PKG_CONFIG_PATH=${PKG_CONFIG_PATH}:${install_dir}/lib/pkgconfig
}

function argobots() {
    libev

    name=argobots
    download_dir=${WORKING_DIR}/${name}
    if [[ ! -d "${download_dir}" ]]; then
        git clone --depth 1 https://github.com/pmodels/argobots.git ${download_dir}
    fi

    cd ${download_dir}
    git pull
    if [[ ! (-f configure && -x configure) ]]; then
        ./autogen.sh
    fi
    mkdir -p build
    cd build
    install_dir=${PREFIX}/${name}
    PKG_CONFIG_PATH=${PKG_CONFIG_PATH} ../configure --prefix=${install_dir}
    make -j ${PROCS}
    make -j ${PROCS} install
    cd ../..

    PKG_CONFIG_PATH=${PKG_CONFIG_PATH}:${install_dir}/lib/pkgconfig
}

function margo() {
    # libtool-ltdl-devel
    if [[ $(check_library "libltdl\\..*") -ne 0 ]]
    then
        echo "libtool-ltdl not found"
        exit 1
    fi

    argobots
    mercury

    name=margo
    download_dir=${WORKING_DIR}/${name}
    if [[ ! -d "${download_dir}" ]]; then
        git clone --depth 1 https://xgitlab.cels.anl.gov/sds/margo.git ${download_dir}
    fi

    cd ${download_dir}
    git pull
    if [[ ! (-f configure && -x configure) ]]; then
        ./prepare.sh
    fi
    mkdir -p build
    cd build
    install_dir=${PREFIX}/${name}
    PKG_CONFIG_PATH=${PKG_CONFIG_PATH} ../configure --prefix=${install_dir}
    make -j ${PROCS}
    make -j ${PROCS} install
    cd ../..

    PKG_CONFIG_PATH=${PKG_CONFIG_PATH}:${install_dir}/lib/pkgconfig
}

function thallium() {
    check_cmake

    margo

    name=thallium
    download_dir=${WORKING_DIR}/${name}
    if [[ ! -d "${download_dir}" ]]; then
        git clone --depth 1 https://xgitlab.cels.anl.gov/sds/thallium.git ${download_dir}
    fi

    cd ${download_dir}
    git pull
    mkdir -p build
    cd build
    rm -rf *
    install_dir=${PREFIX}/${name}
    PKG_CONFIG_PATH=${PKG_CONFIG_PATH} mercury_DIR=$MERCURY_DIR cmake -DCMAKE_INSTALL_PREFIX=${install_dir} -DCMAKE_CXX_EXTENSIONS:BOOL=OFF ..
    make -j ${PROCS}
    make -j ${PROCS} install
    cd ../..

    PKG_CONFIG_PATH=${PKG_CONFIG_PATH}:${install_dir}/lib/pkgconfig
}

function leveldb_pkgconfig {
    install_dir=$1
    pc="${install_dir}/lib64/leveldb.pc"

    (
        echo "prefix=${install_dir}"
        echo "exec_prefix=\${prefix}"
        echo "includedir=\${prefix}/include"
        echo "libdir=\${prefix}/lib64"
        echo ""
        echo "Name: leveldb"
        echo "Description: The leveldb library"
        echo "Version: Git Master"
        echo "Cflags: -I\${includedir}"
        echo "Libs: -L\${libdir} -lleveldb"
    ) > $pc

    PKG_CONFIG_PATH=${PKG_CONFIG_PATH}:${install_dir}/lib64
}

function leveldb() {
    name=leveldb
    download_dir=${WORKING_DIR}/${name}
    if [[ ! -d "${download_dir}" ]]; then
        git clone --depth 1 https://github.com/google/leveldb.git ${download_dir}
    fi

    cd ${download_dir}
    git pull
    git apply ${SOURCE}/leveldb.patch || true
    mkdir -p build
    cd build
    rm -rf *
    install_dir=${PREFIX}/${name}
    PKG_CONFIG_PATH=${PKG_CONFIG_PATH} cmake -DCMAKE_INSTALL_PREFIX=${install_dir} ..
    make -j ${PROCS}
    make -j ${PROCS} install
    cd ../..

    leveldb_pkgconfig ${install_dir}
}

function jemalloc() {
    name=jemalloc
    download_dir=${WORKING_DIR}/${name}
    if [[ ! -d "${download_dir}" ]]; then
        git clone --depth 1 https://github.com/jemalloc/jemalloc.git ${download_dir}
    fi

    cd ${download_dir}
    git pull

    if [[ ! (-f configure && -x configure) ]]; then
        ./autogen.sh
    fi
    mkdir -p build
    cd build
    install_dir=${PREFIX}/${name}
    PKG_CONFIG_PATH=${PKG_CONFIG_PATH} ../configure --prefix=${install_dir}
    make -j ${PROCS}
    make -j ${PROCS} install
    cd ../..

    PKG_CONFIG_PATH=${PKG_CONFIG_PATH}:${install_dir}/lib/pkgconfig
}

# Parse command line arguments
# https://stackoverflow.com/a/14203146
POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -h|--help)
    usage
    exit 0
    ;;
    --BMI)
    USE_NA_BMI=
    shift # past argument
    ;;
    --CCI)
    USE_NA_CCI=
    shift # past argument
    ;;
    --OFI)
    USE_NA_OFI=
    shift # past argument
    ;;
    --SM)
    USE_NA_SM=
    shift # past argument
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

if [[ "$#" -ne 2 ]]; then
    usage
    exit 1
fi

WORKING_DIR=$1
PREFIX=$2
PROCS=$3 # number of processes make should use
PKG_CONFIG_PATH=

mkdir -p ${WORKING_DIR}
cd ${WORKING_DIR}

if [[ -z "${CC}" ]]; then
    CC="cc"
fi

if [[ -z "${CXX}" ]]; then
    CXX="c++"
fi

# check only
check_executable ${CC}
check_executable ${CXX}
check_executable make
check_autoconf
check_cmake
check_mpi

# check and/or install
thallium
leveldb
jemalloc

echo -e "Please add this to your PKG_CONFIG_PATH:\n${PKG_CONFIG_PATH}"
