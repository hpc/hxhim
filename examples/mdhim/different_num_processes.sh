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

MIN_KEY_LEN=1
MAX_KEY_LEN=10
MIN_VALUE_LEN=1
MAX_VALUE_LEN=10
COUNT=10
DATABASES=12
MDHIM_CONFIG=${MDHIM_CONFIG:-"mdhim.conf"}

function help() {
    echo "Usage: $(basename $0) [Options]"
    echo
    echo "    Options:"
    echo "        -h, --help      show help"
    echo "        -n, --count     number of key value pairs     ($COUNT)"
    echo "        -d, --databases the number of databases       ($DATABASES)"
    echo "        --min_key_len   the minimum length of a key   ($MIN_KEY_LEN)"
    echo "        --max_key_len   the maximum length of a key   ($MAX_KEY_LEN)"
    echo "        --min_value_len the minimum length of a value ($MIN_VALUE_LEN)"
    echo "        --max_value_len the maximum length of a value ($MAX_VALUE_LEN)"
}

# Parse command line arguments
# https://stackoverflow.com/a/14203146
POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -h|--help)
    help
    exit 0
    ;;
    -n|--count)
    COUNT=$2
    shift # past argument
    shift # past value
    ;;
    -d|--databases)
    DATABASES=$2
    shift # past argument
    shift # past value
    ;;
    --min_key_len)
    MIN_KEY_LEN=$2
    shift # past argument
    shift # past value
    ;;
    --max_key_len)
    MAX_KEY_LEN=$2
    shift # past argument
    shift # past value
    ;;
    --min_value_len)
    MIN_VALUE_LEN=$2
    shift # past argument
    shift # past value
    ;;
    --max_value_len)
    MAX_VALUE_LEN=$2
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

# Generate key value pairs
kv_pairs=$($(dirname $0)/generate_kv_pairs.sh --count $COUNT --min_key_len $MIN_KEY_LEN --max_key_len $MAX_KEY_LEN --min_value_len $MIN_VALUE_LEN --max_value_len $MAX_VALUE_LEN)
if [[ "$?" -eq "1" ]] ; then
    echo $kv_pairs
    exit 1
fi

# Read the key value pairs in
declare -A KEYPAIRS
while read key value ; do
    KEYPAIRS[$key]=$value
done < <(echo "$kv_pairs")

# Generate commands using the key value pairs
declare -A OPS
OPS["PUT"]=$(for key in ${!KEYPAIRS[@]}; do
                 echo "PUT $key ${KEYPAIRS[$key]}"
             done)
OPS["GET"]=$(for key in ${!KEYPAIRS[@]}; do
                 echo "GET $key"
             done)
OPS["DEL"]=$(for key in ${!KEYPAIRS[@]}; do
                 echo "DEL $key"
             done)
OPS["BPUT"]=$(echo -n "BPUT $COUNT";
              for key in ${!KEYPAIRS[@]}; do
                  echo -n " $key ${KEYPAIRS[$key]}"
              done)
OPS["BGET"]=$(echo -n "BGET $COUNT";
              for key in ${!KEYPAIRS[@]}; do
                  echo -n " $key"
              done)
OPS["BDEL"]=$(echo -n "BDEL $COUNT";
              for key in ${!KEYPAIRS[@]}; do
                  echo -n " $key"
              done)

# Print configuration
echo "Key Length:          [$MIN_KEY_LEN, $MAX_KEY_LEN]"
echo "Value Length:        [$MIN_VALUE_LEN, $MAX_VALUE_LEN]"
echo "Number of KV Pairs:  $COUNT"
echo "Number of Databases: $DATABASES"
echo "Reading config from: $(realpath $MDHIM_CONFIG)"

# Put some keys into the databases
config=${MDHIM_CONFIG}.tmp
cp $MDHIM_CONFIG $config
echo "${OPS[PUT]}" | MDHIM_CONFIG=$config mpirun -np $DATABASES examples/cli

# Pass the commands to the CLI
for ranks in $(seq 1 $DATABASES); do
    if [[ $(($DATABASES % $ranks)) -eq 0 ]]; then
        echo "DBS_PER_RSERVER $(($DATABASES / $ranks))" >> $config
        (
            echo "${OPS[GET]}"
            echo "${OPS[BGET]}"
        ) | MDHIM_CONFIG=$config mpirun -np $ranks $(dirname $0)/cli
    fi
done

# Clean upp
rm -f $config
