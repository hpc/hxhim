#!/usr/bin/env bash
# different_num_processes.sh
# This script runs a fixed number of MDHIM
# databases with variable numbers of processes
# to make sure that MDHIM is able to handle
# process count changes.
#
# Randomly generated key-value pairs are
# PUT into d databases with the configuration
# of d processes with 1 database each. Then,
# The keys are extracted using GET and BGET
# while running MDHIM with r ranks, where
# 1 <= r <= d and d % r == 0.
#

DATABASES=12
MDHIM_CONFIG=${MDHIM_CONFIG:-"mdhim.conf"}
PRINT=true

function help() {
    echo "Usage: $(basename $0) [Options] kv_source"
    echo
    echo "    Options:"
    echo "        -h, --help      show help"
    echo "        -d, --databases the number of databases             ($DATABASES)"
    echo "        --print         print to stdout                     ($PRINT)"
}

# Check for positional parameters
if [[ "$#" -eq "0" ]] ; then
    help
    exit 0
fi

# Parse command line arguments
# https://stackoverflow.com/a/14203146
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            help
            exit 0
            ;;
        -d|--databases)
            DATABASES=$2
            shift
            shift
            ;;
        --print)
            PRINT=$2
            shift
            shift
            ;;
        *)    # unknown option
            POSITIONAL+=("$1") # save it in an array for later
            shift # past argument
            ;;
    esac
done

KV_SOURCE="${POSITIONAL[0]}"
[[ ! -e "$KV_SOURCE" ]] && echo "Cannot open $KV_SOURCE" && exit 1

# Read the key value pairs in
COUNT=0
KEYS=()
VALUES=()
while read key value ; do
    KEYS+=($key)
    VALUES+=($value)
    COUNT=$((COUNT+1))
done < $KV_SOURCE

# Print configuration
echo "Number of Databases: $DATABASES"
echo "Reading config from: $(realpath $MDHIM_CONFIG)"

# Put some keys into the databases
config=${MDHIM_CONFIG}.tmp
cp $MDHIM_CONFIG $config
echo "DBS_PER_RSERVER 1" >> $config
(
    for i in ${!KEYS[@]}; do
        echo "PUT ${KEYS[$i]} ${VALUES[$i]}"
    done
) | MDHIM_CONFIG=$config mpirun -np $DATABASES $(dirname $0)/cli $PRINT

# Pass the commands to the CLI
for ranks in $(seq 1 $DATABASES); do
    if [[ $(($DATABASES % $ranks)) -eq 0 ]]; then
        echo "DBS_PER_RSERVER $(($DATABASES / $ranks))" >> $config
        (
            for i in ${!KEYS[@]}; do
                echo "GET ${KEYS[$i]}"
            done

            echo -n "BGET $COUNT";
            for i in ${!KEYS[@]}; do
                echo -n " ${KEYS[$i]}"
            done
        ) | MDHIM_CONFIG=$config mpirun -np $ranks $(dirname $0)/cli $PRINT
    fi
done

# Clean up
rm -f $config
