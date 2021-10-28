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
#     snappy
#
# Git Repositories (checks and installs):
#     BMI (optional)
#     CCI (optional)
#     OFI (optional)
#     mercury
#     libev-devel (libev.*)
#     argobots
#     json-c
#     margo
#     thallium
#     jemalloc (optional)
#

SOURCE="$(dirname $(realpath $BASH_SOURCE[0]))"

set -e

START_DIR="$(pwd)"

function return_to_start_dir() {
    ret="$?"
    cd "${START_DIR}"
    exit "${ret}"
}

trap return_to_start_dir EXIT

function usage() {
    echo "Usage: $0 [Options] download_dir build_name install_dir [PROCS]"
    echo ""
    echo "    Options:"
    echo "        --BMI           build the BMI module for mercury"
    echo "        --CCI           build the CCI module for mercury"
    echo "        --OFI           build the OFI module for mercury"
    echo "        --SM            build the SM  module for mercury"
    echo
    echo "        --NO_DL         do not download repositories"
    echo "        --UPDATE        update source if downloading"
    echo "        --DL_ONLY       stop after downloading and updating (if the source needed to be downloaded)"
    echo "        --NO_LEVELDB    do not download/build/install leveldb"
    echo "        --NO_ROCKSDB    do not download/build/install rocksdb"
    echo "        --NO_JEMALLOC   do not download/build/install jemalloc"
    echo "        --cmake <bin>   name of CMake executable (default: cmake)"
}

# Check that an executable is available; if not, print the name and exit
function check_executable() {
    executable="$@"
    command -v ${executable} > /dev/null 1>&2 || (echo "${executable} not found"; exit 1)
}

function check_autoconf() {
    check_executable autoconf
    check_executable automake
    check_executable libtool
}

function check_cmake() {
    check_executable "${CMAKE}"

    # make sure CMake's version is at least 3.6.3
    version=$("${CMAKE}" --version | head -n 1 | awk '{ print $3 }')
    [ "3.6.3" = $(echo -e "3.6.3\n${version}" | sort -V | head -n 1) ] || (echo "Need CMake 3.6.3+. Have ${version}."; exit 1)
}

# Check that a library is found; if not, print the library name and exit
function check_library() {
    library=$1
    ldconfig -N -v $(echo "${LD_LIBRARY_PATH}" | sed 's/:/ /g') 2> /dev/null | grep ${library} > /dev/null
}

function check_mpi() {
    check_executable mpicc
    check_executable mpicxx
    check_executable mpirun
    check_executable mpiexec
    if ! check_library "libmpi.*"
    then
        echo "mpi library not found"
        exit 1
    fi
}

function install() {
    # all arguments after download are optional
    name="$1"             # name of package
    download="$2"         # function to download package
    update="$3"           # function to update downloaded files
    setup="$4"            # run this within the package root
    cbi="$5"              # configure, build, and install in the ${BUILD} directory
    post_install="$6"     # run this after installing

    echo "${name} Start" 1>&2

    install_dir="${PREFIX}/${name}"

    # check if the install directory exists
    if [[ "${UPDATE}" -eq "1" ]] || [[ ! -d "${install_dir}" ]]
    then
        # check if the download directory exists
        download_dir="${WORKING_DIR}/${name}"
        if [[ "${DL}" -eq "1" ]]
        then
            # repo directory doesn't exist
            if [[ ! -d "${download_dir}" ]]
            then
                ${download} "${download_dir}"
                echo "${name} Downloaded" 1>&2
            fi

            cd "${download_dir}"

            # update
            if [[ "${UPDATE}" -eq "1" ]]
            then
                ${update}
                echo "${name} Updated" 1>&2
            fi
        else
            # don't download, but directory doesn't exist
            if [[ ! -d "${download_dir}" ]]
            then
                echo "Error: ${name} not available locally but also not downloaded"
                return 1
            fi
        fi

        # if only downloading, stop here
        if [[ "${DL_ONLY}" -eq "1" ]]
        then
            return 0;
        fi

        # ${download_dir} exists by this point
        cd "${download_dir}"

        # check if the build directory exists
        build_dir="${download_dir}/${BUILD}"
        if [[ ! -d "${build_dir}" ]]
        then
            ${setup}

            mkdir -p "${build_dir}"
        fi

        # ${build_dir} exists at this point
        cd "${build_dir}"

        LD_LIBRARY_PATH="${ld_library_path}:${LD_LIBRARY_PATH}" PKG_CONFIG_PATH="${pkg_config_path}:${PKG_CONFIG_PATH}" ${cbi}

        echo "${name} Built and Installed" 1>&2

        # add to ld_library_path and pkg_config_path
        dep_lib=""
        if [[ -d "${install_dir}/lib" ]]
        then
            dep_lib="${install_dir}/lib"
        elif [[ -d "${install_dir}/lib64" ]]
        then
            dep_lib="${install_dir}/lib64"
        fi

        if [[ -z "${dep_lib}" ]]
        then
            echo "Could not find ${name}'s library directory" 1>&2
            return 1
        fi

        # make sure the pkgconfig directory exists
        dep_pcp="${dep_lib}/pkgconfig"
        mkdir -p "${dep_pcp}"

        ld_library_path="${ld_library_path}:${dep_lib}"
        pkg_config_path="${pkg_config_path}:${dep_pcp}"

        ${post_install}
    fi

    echo "${name} Done" 1>&2
}

function git_pull() {
    git pull
}

function NA_BMI() {
    function dl_bmi() {
        git clone --depth 1 https://xgitlab.cels.anl.gov/sds/bmi.git "$1"
    }

    function setup_bmi() {
        ./prepare
    }

    function cbi_bmi() {
        if [[ ! -f Makefile ]]
        then
            ../configure --prefix="${install_dir}" --enable-shared --enable-bmi-only
        fi

        make -j ${PROCS}
        make -j ${PROCS} install
    }

    function setup_bmi_vars() {
        BMI_INCLUDE_DIR="${install_dir}/include"
        BMI_LIBRARY="${install_dir}/lib/libbmi.so"
        cmake_options="${cmake_options} -DNA_USE_BMI:BOOL=On -DBMI_INCLUDE_DIR=${BMI_INCLUDE_DIR} -DBMI_LIBRARY=${BMI_LIBRARY}"
    }

    install "bmi" dl_bmi git_pull setup_bmi cbi_bmi setup_bmi_vars
}

function NA_CCI() {
    function dl_cci() {
        git clone --depth 1 https://github.com/CCI/cci.git "$1"
    }

    function setup_cci() {
        ./autogen.pl
    }

    function cbi_cci() {
        if [[ ! -f Makefile ]]
        then
            ../configure --prefix="${install_dir}"
        fi

        make -j ${PROCS}
        make -j ${PROCS} install
    }

    function setup_cci_vars() {
        CCI_INCLUDE_DIR="${install_dir}/include"
        CCI_LIBRARY="${install_dir}/lib/libcci.so"
        cmake_options="${cmake_options} -DNA_USE_CCI:BOOL=On -DCCI_INCLUDE_DIR=${CCI_INCLUDE_DIR} -DCCI_LIBRARY=${CCI_LIBRARY}"
    }

    install "cci" dl_cci git_pull setup_cci cbi_cci setup_cci_vars
}

function NA_OFI() {
    function dl_ofi() {
        git clone --depth 1 https://github.com/ofiwg/libfabric.git "$1"
    }

    function setup_ofi() {
        ./autogen.sh
    }

    function cbi_ofi() {
        if [[ ! -f Makefile ]]
        then
            ../configure --prefix="${install_dir}"
        fi

        make -j ${PROCS}
        make -j ${PROCS} install
    }

    function setup_ofi_vars() {
        OFI_INCLUDE_DIR="${install_dir}/include"
        OFI_LIBRARY="${install_dir}/lib/libfabric.so"
        cmake_options="${cmake_options} -DNA_USE_OFI:BOOL=On -DOFI_INCLUDE_DIR=${OFI_INCLUDE_DIR} -DOFI_LIBRARY=${OFI_LIBRARY}"
    }

    install "libfabric" dl_ofi git_pull setup_ofi cbi_ofi setup_ofi_vars
}

function NA_SM() {
    cmake_options="${cmake_options} -DNA_USE_SM:BOOL=ON"
}

function mercury() {
    check_cmake

    cmake_options=
    if [[ "${USE_NA_BMI}" -eq "1" ]]; then NA_BMI; fi
    if [[ "${USE_NA_CCI}" -eq "1" ]]; then NA_CCI; fi
    if [[ "${USE_NA_OFI}" -eq "1" ]]; then NA_OFI; fi
    if [[ "${USE_NA_SM}"  -eq "1" ]]; then NA_SM;  fi

    function dl_hg() {
        git clone --depth 1 https://github.com/mercury-hpc/mercury.git "$1"
    }

    function setup_hg() {
        git submodule init && git submodule update
    }

    function cbi_hg() {
        if [[ ! -f Makefile ]]
        then
            "${CMAKE}" .. -DCMAKE_INSTALL_PREFIX="${install_dir}" -DMERCURY_USE_BOOST_PP:BOOL=ON -DBUILD_SHARED_LIBS:BOOL=ON ${cmake_options}
        fi

        make -j ${PROCS}
        make -j ${PROCS} install
    }

    function setup_hg_vars() {
        MERCURY_DIR="${install_dir}"
    }

    install "mercury" dl_hg git_pull setup_hg cbi_hg setup_hg_vars
}

function libev() {
    # libev-devel
    if ! check_library "libev\\..*"
    then
        return 0;
    fi

    function dl_libev() {
        git clone --depth 1 https://github.com/enki/libev.git "$1"
    }

    function setup_libev() {
        chmod +x ./autogen.sh
        ./autogen.sh
    }

    function cbi_libev() {
        if [[ ! -f Makefile ]]
        then
            ../configure --prefix="${install_dir}"
        fi

        make -j ${PROCS}
        make -j ${PROCS} install
    }

    install "libev" dl_libev git_pull setup_libev cbi_libev
}

function argobots() {
    libev

    function dl_argobots() {
        git clone --depth 1 https://github.com/pmodels/argobots.git "$1"
    }


    function setup_argobots() {
        ./autogen.sh
    }

    function cbi_argobots() {
        if [[ ! -f Makefile ]]
        then
            ../configure --prefix="${install_dir}"
        fi

        make -j ${PROCS}
        make -j ${PROCS} install
    }

    install "argobots" dl_argobots git_pull setup_argobots cbi_argobots
}

function json-c() {
    function dl_json-c() {
        git clone --depth 1 https://github.com/json-c/json-c.git "$1"
    }

    function cbi_json-c() {
        if [[ ! -f Makefile ]]
        then
            "${CMAKE}" .. -DCMAKE_INSTALL_PREFIX="${install_dir}"
        fi

        make -j ${PROCS}
        make -j ${PROCS} install
    }

    install "json-c" dl_json-c git_pull "" cbi_json-c
}

function margo() {
    # libtool-ltdl-devel
    if ! check_library "libltdl\\..*"
    then
        echo "libtool-ltdl not found"
        exit 1
    fi

    argobots
    mercury
    json-c

    function dl_margo() {
        git clone --depth 1 https://xgitlab.cels.anl.gov/sds/margo.git "$1"
    }

    function setup_margo() {
        ./prepare.sh
    }

    function cbi_margo() {
        if [[ ! -f Makefile ]]
        then
            ../configure --prefix="${install_dir}"
        fi

        make -j ${PROCS}
        make -j ${PROCS} install
    }

    install "margo" dl_margo git_pull setup_margo cbi_margo
}

function thallium() {
    check_cmake

    margo

    function dl_thallium() {
        git clone --depth 1 https://xgitlab.cels.anl.gov/sds/thallium.git "$1"
    }

    function cbi_thallium() {
        if [[ ! -f Makefile ]]
        then
            mercury_DIR="${MERCURY_DIR}" "${CMAKE}" .. -DCMAKE_INSTALL_PREFIX="${install_dir}" -DCMAKE_CXX_EXTENSIONS:BOOL=OFF
        fi

        make -j ${PROCS}
        make -j ${PROCS} install
    }

    install "thallium" dl_thallium git_pull "" cbi_thallium
}

function leveldb() {
    check_library snappy

    function dl_leveldb() {
        git clone --depth 1 https://github.com/google/leveldb.git "$1"
    }

    function setup_leveldb() {
        git submodule update --init --recursive
        git reset --hard HEAD
        git apply ${SOURCE}/leveldb.patch || true
    }

    function cbi_leveldb() {
        if [[ ! -f Makefile ]]
        then
            "${CMAKE}" .. -DCMAKE_INSTALL_PREFIX="${install_dir}" -DHAVE_SNAPPY=On
        fi

        make -j ${PROCS}
        make -j ${PROCS} install
    }

    function create_leveldb_pkgconfig() {
        pc="${dep_pcp}/leveldb.pc"

        (
            echo "prefix=${install_dir}"
            echo "exec_prefix=\${prefix}"
            echo "includedir=\${prefix}/include"
            echo "libdir=${dep_lib}"
            echo ""
            echo "Name: leveldb"
            echo "Description: The leveldb library"
            echo "Version: Git Master"
            echo "Cflags: -I\${includedir}"
            echo "Libs: -L\${libdir} -lleveldb -lsnappy"
        ) > "${pc}"
    }

    install "leveldb" dl_leveldb git_pull setup_leveldb cbi_leveldb create_leveldb_pkgconfig
}

function rocksdb() {
    function dl_rocksdb() {
        git clone --depth 1 https://github.com/facebook/rocksdb.git "$1"
    }

    function cbi_rocksdb() {
        if [[ ! -f Makefile ]]
        then
            "${CMAKE}" .. -DCMAKE_INSTALL_PREFIX="${install_dir}" -DWITH_GFLAGS=Off
        fi

        make -j ${PROCS}
        make -j ${PROCS} install
    }

    function create_rocksdb_pkgconfig() {
        pc="${dep_pcp}/rocksdb.pc"

        (
            echo "prefix=${install_dir}"
            echo "exec_prefix=\${prefix}"
            echo "includedir=\${prefix}/include"
            echo "libdir=${dep_lib}"
            echo ""
            echo "Name: rocksdb"
            echo "Description: The RocksDB library"
            echo "Version: Git Master"
            echo "Cflags: -I\${includedir}"
            echo "Libs: -L\${libdir} -lrocksdb"
        ) > "${pc}"
    }

    install "rocksdb" dl_rocksdb git_pull "" cbi_rocksdb create_rocksdb_pkgconfig
}

function jemalloc() {
    function dl_jemalloc() {
        git clone --depth 1 https://github.com/jemalloc/jemalloc.git "$1"
    }

    function setup_jemalloc() {
        ./autogen.sh
    }

    function cbi_jemalloc() {
        if [[ ! -f Makefile ]]
        then
            ../configure --prefix="${install_dir}"
        fi

        make -j ${PROCS}
        make -ij ${PROCS} install
    }

    install "jemalloc" dl_jemalloc git_pull setup_jemalloc cbi_jemalloc
}

DL=1
DL_ONLY=0
UPDATE=0
LEVELDB=1
ROCKSDB=1
JEMALLOC=1
CMAKE="cmake"

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
        USE_NA_BMI=1
        shift # past argument
        ;;
    --CCI)
        USE_NA_CCI=1
        shift # past argument
        ;;
    --OFI)
        USE_NA_OFI=1
        shift # past argument
        ;;
    --SM)
        USE_NA_SM=1
        shift # past argument
        ;;
    --NO_DL)
        DL=0
        shift # past argument
        ;;
    --DL_ONLY)
        DL_ONLY=1
        shift # past argument
        ;;
    --UPDATE)
        UPDATE=1
        shift # past argument
        ;;
    --NO_LEVELDB)
        LEVELDB=0
        shift # past argument
        ;;
    --NO_ROCKSDB)
        ROCKSDB=0
        shift # past argument
        ;;
    --NO_JEMALLOC)
        JEMALLOC=0
        shift # past argument
        ;;
    --cmake)
        CMAKE="$2"
        shift # past argument
        shift # past value
        ;;
    *)    # unknown option
        POSITIONAL+=("$1") # save it in an array for later
        shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

if [[ "$#" -lt 3 ]]; then
    usage
    exit 1
fi

WORKING_DIR="$(realpath $1)"
BUILD="$2"
PREFIX="$(realpath $3)"
PROCS="$4"       # number of processes make should use
pkg_config_path= # internal PKG_CONFIG_PATH so available paths are not hidden
ld_library_path= # internal LD_LIBRARY_PATH so available paths are not hidden

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

if [[ "${LEVELDB}" -eq "1" ]]
then
    leveldb
fi

if [[ "${ROCKSDB}" -eq "1" ]]
then
    rocksdb
fi

if [[ "${JEMALLOC}" -eq "1" ]]
then
    jemalloc
fi

if [[ "${DL_ONLY}" -eq "0" ]]
then
    export LD_LIBRARY_PATH="${ld_library_path}:${LD_LIBRARY_PATH}"
    export PKG_CONFIG_PATH="${pkg_config_path}:${PKG_CONFIG_PATH}"
    echo -e "Add to LD_LIBRARY_PATH:\n${ld_library_path}"
    echo -e "Add to PKG_CONFIG_PATH:\n${pkg_config_path}"
    echo
    echo "Source this script to get prepend LD_LIBRARY_PATH and PKG_CONFIG_PATH with these values"
fi
