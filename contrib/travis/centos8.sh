#!/usr/bin/env bash

set -e

yum install -y dnf-plugins-core
yum config-manager --set-enabled PowerTools

yum install -y         \
    automake           \
    bzip2              \
    gcc-toolset-9      \
    git                \
    libtool            \
    libtool-ltdl-devel \
    pkg-config         \
    python3-pip        \
    snappy-devel       \
    wget               \

source /opt/rh/gcc-toolset-9/enable

contrib/travis/build_and_test.sh
