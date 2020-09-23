#!/usr/bin/env bash

if [[ "$#" -lt "1" ]]
then
    echo "Syntax: $0 INSTALL_DIR" 1>&2
    exit 1
fi

INSTALL_DIR="$1"

# set PATH, LD_LIBRARY_PATH, and PKG_CONFIG_PATH
for name in mpi
do
    prefix="${INSTALL_DIR}/${name}"

    if [[ -d "${prefix}/bin" ]]
    then
        export PATH="${prefix}/bin:${PATH}"
    fi

    lib=""
    if [[ -d "${prefix}/lib" ]]
    then
        lib="${prefix}/lib"
    elif [[ -d "${prefix}/lib64" ]]
    then
        lib="${prefix}/lib64"
    fi

    if [[ ! -z "${lib}" ]]
    then
        export LD_LIBRARY_PATH="${lib}:${LD_LIBRARY_PATH}"
        if [[ -d "${lib}/pkgconfig" ]]
        then
            export PKG_CONFIG_PATH="${lib}/pkgconfig:${PKG_CONFIG_PATH}"
        fi
    fi
done
