#!/usr/bin/env bash

set -e

apt-get update -y
apt-get install -y software-properties-common

add-apt-repository -y ppa:ubuntu-toolchain-r/test
apt-get update -y

apt-get install -y \
    automake       \
    bzip2          \
    flex           \
    g++-9          \
    gfortran-9     \
    git            \
    libltdl-dev    \
    libsnappy-dev  \
    libtool-bin    \
    pkg-config     \
    python3-pip    \
    wget
