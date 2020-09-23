#!/usr/bin/env bash

set -e

dnf install -y         \
    automake           \
    bzip2              \
    gcc-toolset-9      \
    git                \
    libtool            \
    libtool-ltdl-devel \
    pkg-config         \
    python3-pip        \
    wget               \

source /opt/rh/gcc-toolset-9/enable

contrib/travis/build_and_test.sh
