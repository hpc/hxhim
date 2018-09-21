#!/usr/bin/env bash
# HXHIM Dependency Installer
#
# This script checks for the files needed by HXHIM. Git
# repositories are installed. yum/rpm repostories are only
# checked for, not installed.
#
# CC and CXX can be set to change the compiler
#
# Packages:
#     autoconf/automake/libtool
#     boost-devel
#     cmake (3+)
#     libev-devel
#     libtool-ltdl-devel
#
# Git Repositories:
#     BMI (optional)
#     CCI (optional)
#     mercury
#     argobots
#     margo
#     thallium
#
# Not Checked/Installed:
#     MPI
#

set -e

function usage() {
    echo "Usage: $0 [Options] download_dir install_dir"
    echo ""
    echo "    Options:"
    echo "        --BMI  build the BMI module for mercury"
    echo "        --CCI  build the CCI module for mercury"
    echo "        --SM   build the SM  module for mercury"
}

# Check that a package is installed; if not, print the name of the package and exit
function check_package() {
    package_name=$1
    rpm -q ${package_name} &> /dev/null || (echo "Package ${package_name} not installed"; exit 1)
}

function check_autoconf() {
    check_package autoconf
}

function check_cmake() {
    check_package cmake
}

function check_boost() {
    check_package boost-devel
}

function NA_BMI() {
    check_autoconf

    name=bmi
    if [[ ! -d "$name" ]]; then
        git clone http://git.mcs.anl.gov/bmi.git $name
    fi

    cd $name
    ./prepare
    mkdir -p build
    cd build
    install_dir=$PREFIX/$name
    PKG_CONFIG_PATH=$PKG_CONFIG_PATH ../configure --prefix=$PREFIX/$name --enable-shared --enable-bmi-only
    make -j ${PROCS}
    make -j ${PROCS} install
    cd ../..

    BMI_INCLUDE_DIR=$install_dir/include
    BMI_LIBRARY=$install_dir/lib/libbmi.so
    PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$install_dir/lib/pkgconfig
}

function NA_CCI() {
    check_autoconf

    name=cci
    if [[ ! -d "$name" ]]; then
        git clone https://github.com/CCI/cci.git $name
    fi

    cd $name
    ./autogen.pl
    mkdir -p build
    cd build
    install_dir=$PREFIX/$name
    PKG_CONFIG_PATH=$PKG_CONFIG_PATH ../configure --prefix=$install_dir
    make -j ${PROCS}
    make -j ${PROCS} install
    cd ../..

    CCI_INCLUDE_DIR=$install_dir/include
    CCI_LIBRARY=$install_dir/lib/libcci.so
    PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$install_dir/lib/pkgconfig
}

function NA_SM() {
    return
}

function mercury() {
    check_cmake
    check_boost

    cmake_options=
    if [[ ! -z ${USE_NA_BMI+false} ]]; then
        echo BMI
        NA_BMI
        cmake_options="$cmake_options -DNA_USE_BMI:BOOL=ON -DBMI_INCLUDE_DIR=$BMI_INCLUDE_DIR -DBMI_LIBRARY=$BMI_LIBRARY"
    fi

    if [[ ! -z ${USE_NA_CCI+false} ]]; then
        echo CCI
        NA_CCI
        cmake_options="$cmake_options -DNA_USE_CCI:BOOL=ON -DCCI_INCLUDE_DIR=$CCI_INCLUDE_DIR -DCCI_LIBRARY=$CCI_LIBRARY"
    fi

    if [[ ! -z ${USE_NA_SM+false} ]]; then
        echo SM
        NA_SM
        cmake_options="$cmake_options -DNA_USE_SM:BOOL=ON"
    fi

    name=mercury
    if [[ ! -d "$name" ]]; then
        git clone --recurse-submodules https://github.com/mercury-hpc/mercury.git $name
    fi

    cd $name
    mkdir -p build
    cd build
    install_dir=$PREFIX/$name
    PKG_CONFIG_PATH=$PKG_CONFIG_PATH cmake -DCMAKE_INSTALL_PREFIX=$install_dir -DMERCURY_USE_BOOST_PP:BOOL=ON -DMERCURY_USE_SYSTEM_BOOST:BOOL=ON -DBUILD_SHARED_LIBS:BOOL=ON $(echo "$cmake_options") ..
    make -j ${PROCS}
    make -j ${PROCS} install
    cd ../..

    MERCURY_DIR=$install_dir
    PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$install_dir/lib/pkgconfig
}

function check_libev() {
    check_package libev-devel
}

function argobots() {
    check_autoconf
    # check_libev

    name=argobots
    if [[ ! -d "$name" ]]; then
        git clone https://github.com/pmodels/argobots.git $name
    fi

    cd $name
    ./autogen.sh
    mkdir -p build
    cd build
    install_dir=$PREFIX/$name
    PKG_CONFIG_PATH=$PKG_CONFIG_PATH ../configure --prefix=$install_dir
    make -j ${PROCS}
    make -j ${PROCS} install
    cd ../..

    PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$install_dir/lib/pkgconfig
}

function check_libtool_ltdl() {
    check_package libtool-ltdl-devel
}

function margo() {
    check_autoconf
    # check_libtool_ltdl

    argobots
    mercury

    name=margo
    if [[ ! -d "$name" ]]; then
        git clone https://xgitlab.cels.anl.gov/sds/margo.git $name
    fi

    cd $name
    mkdir -p build
    ./prepare.sh
    cd build
    install_dir=$PREFIX/$name
    PKG_CONFIG_PATH=$PKG_CONFIG_PATH PKG_CONFIG_PATH=$PKG_CONFIG_PATH ../configure --prefix=$install_dir
    make -j ${PROCS}
    make -j ${PROCS} install
    cd ../..

    PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$install_dir/lib/pkgconfig
}

function thallium() {
    check_cmake

    margo

    name=thallium
    if [[ ! -d "$name" ]]; then
        git clone https://xgitlab.cels.anl.gov/sds/thallium.git $name
    fi

    cd $name
    mkdir -p build
    cd build
    install_dir=$PREFIX/$name
    PKG_CONFIG_PATH=$PKG_CONFIG_PATH mercury_DIR=$MERCURY_DIR cmake -DCMAKE_INSTALL_PREFIX=$install_dir -DCMAKE_CXX_EXTENSIONS:BOOL=OFF ..
    make -j ${PROCS}
    make -j ${PROCS} install
    cd ../..

    PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$install_dir/lib/pkgconfig
}

function leveldb_pkgconfig {
    install_dir=$1
    pc="$install_dir/lib64/leveldb.pc"

    (
        echo "prefix=$install_dir"
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

    PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$install_dir/lib64
}

function leveldb() {
    name=leveldb
    if [[ ! -d "$name" ]]; then
        git clone https://github.com/google/leveldb.git $name
    fi

    cd $name
    mkdir -p build
    cd build
    install_dir=$PREFIX/$name
    PKG_CONFIG_PATH=$PKG_CONFIG_PATH cmake -DCMAKE_INSTALL_PREFIX=$install_dir ..
    make -j ${PROCS}
    make -j ${PROCS} install
    cd ../..

    leveldb_pkgconfig $install_dir
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
    exit 0
fi

WORKING_DIR=$1
PREFIX=$2
PKG_CONFIG_PATH=

mkdir -p $WORKING_DIR
cd $WORKING_DIR

thallium
leveldb

echo -e "Please add this to your PKG_CONFIG_PATH:\n$PKG_CONFIG_PATH"
