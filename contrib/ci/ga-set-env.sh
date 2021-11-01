#!/usr/bin/env bash

echo "export PATH=${PATH}"
echo "export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}"
echo "export PKG_CONFIG_PATH=${PKG_CONFIG_PATH}"

while [[ $# -gt 0 ]]
do
    echo "export $1"
    shift
done
