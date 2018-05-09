#!/usr/bin/env bash
# random.sh
# This script generates random key value pairs performs
# user provided operations with them on an MDHIM instance
# using the cli.
#

MIN_KEY_LEN=1
MAX_KEY_LEN=10
MIN_VALUE_LEN=1
MAX_VALUE_LEN=10
COUNT=10
RANKS=10
MDHIM_CONFIG=${MDHIM_CONFIG:-"mdhim.conf"}

function help() {
    echo "Usage: $(basename $0) [Options] PUT|GET|DEL|BPUT|BGET|BDEL|COMMIT|WHICH|BWHICH ..."
    echo
    echo "    Options:"
    echo "        -h, --help       show help"
    echo "        -n, --count      number of key value pairs     ($COUNT)"
    echo "        -np, --ranks     the number of MPI ranks       ($RANKS)"
    echo "        --min_key_len    the minimum length of a key   ($MIN_KEY_LEN)"
    echo "        --max_key_len    the maximum length of a key   ($MAX_KEY_LEN)"
    echo "        --min_value_len  the minimum length of a value ($MIN_VALUE_LEN)"
    echo "        --max_value_len  the maximum length of a value ($MAX_VALUE_LEN)"
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
    -np|--ranks)
    RANKS=$2
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

# Check for positional parameters
if [[ "$#" -eq "0" ]] ; then
    help
    exit 0
fi

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
OPS["COMMIT"]=$(echo -n "COMMIT")
OPS["WHICH"]=$(for key in ${!KEYPAIRS[@]}; do
                    echo "WHICH $key"
                done)
OPS["BWHICH"]=$(echo -n "BWHICH $COUNT";
                for key in ${!KEYPAIRS[@]}; do
                    echo -n " $key"
                done)

# Print configuration
echo "Key Length:          [$MIN_KEY_LEN, $MAX_KEY_LEN]"
echo "Value Length:        [$MIN_VALUE_LEN, $MAX_VALUE_LEN]"
echo "Number of KV Pairs:  $COUNT"
echo "Number of MPI ranks: $RANKS"
echo "Reading config from: $(realpath $MDHIM_CONFIG)"

# Pass the commands to the CLI
(
    for op in "${POSITIONAL[@]}"; do
        echo "${OPS[$op]}"
    done
) | MDHIM_CONFIG=$MDHIM_CONFIG mpirun -np $RANKS examples/cli
