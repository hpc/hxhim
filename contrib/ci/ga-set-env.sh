#!/usr/bin/env bash

echo "PATH=${PATH}" >> ${GITHUB_ENV}
echo "LD_LIBRARY_PATH=${LD_LIBRARY_PATH}" >> ${GITHUB_ENV}
echo "PKG_CONFIG_PATH=${PKG_CONFIG_PATH}" >> ${GITHUB_ENV}

while [[ $# -gt 0 ]]
do
    echo "$1" >> ${GITHUB_ENV}
    shift
done
