#!/usr/bin/env bash

set -e

yum install -y centos-release-scl

yum install -y         \
    automake           \
    bzip2              \
    devtoolset-9       \
    git                \
    libtool            \
    libtool-ltdl-devel \
    pkgconfig          \
    python3-pip        \
    snappy-devel       \
    wget               \

source /opt/rh/devtoolset-9/enable

contrib/travis/build_and_test.sh
