#!/usr/bin/env bash

MIN_KEY_LEN=1
MAX_KEY_LEN=10
MIN_VALUE_LEN=1
MAX_VALUE_LEN=10
COUNT=10
RANKS=10
MDHIM_CONFIG=${MDHIM_CONFIG:-"mdhim.conf"}

function help() {
    echo "Usage: $(basename $0) [Options] PUT|GET|DEL|BPUT|BGET|BDEL ..."
    echo
    echo "    Options:"
    echo "        -h, --help       show help"
    echo "        -n, --count      number of key value pairs"
    echo "        -np, --ranks     the number of MPI ranks"
    echo "        --min_key_len    the minimum length of a key"
    echo "        --max_key_len    the maximum length of a key"
    echo "        --min_value_len  the minimum length of a value"
    echo "        --max_value_len  the maximum length of a value"
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
    Max_KEY_LEN=$2
    shift # past argument
    shift # past value
    ;;
    --min_value_len)
    MIN_VALUE_LEN=$2
    shift # past argument
    shift # past value
    ;;
    --max_value_len)
    Max_VALUE_LEN=$2
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

# Check for arguments
if [[ "$#" -eq "0" ]] ; then
    help
    exit 0
fi

# Generate key valuepairs
declare -A KEYPAIRS
for i in $(seq 1 $COUNT); do
    KEYPAIRS[$(pwgen $((MIN_KEY_LEN+RANDOM%MAX_KEY_LEN)) 1)]=$(pwgen $((MIN_VALUE_LEN+RANDOM%MAX_VALUE_LEN)) 1)
done

# Generate commands using the key value pairs
declare -A OPS
OPS["PUT"]=$(for key in ${!KEYPAIRS[@]}; do
                 printf "PUT $key ${KEYPAIRS[$key]}\n"
             done)
OPS["GET"]=$(for key in ${!KEYPAIRS[@]}; do
                 printf "GET $key\n"
             done)
OPS["DEL"]=$(for key in ${!KEYPAIRS[@]}; do
                 printf "DELETE $key\n"
             done)
OPS["BPUT"]=$(printf "BPUT $COUNT";
              for key in ${!KEYPAIRS[@]}; do
                  printf " $key ${KEYPAIRS[$key]}"
              done)
OPS["BGET"]=$(printf "BGET $COUNT";
              for key in ${!KEYPAIRS[@]}; do
                  printf " $key"
              done)
OPS["BDEL"]=$(printf "BDELETE $COUNT";
              for key in ${!KEYPAIRS[@]}; do
                  printf " $key"
              done)

# Pass the commands to the CLI
(
    for op in "${POSITIONAL[@]}"; do
        printf "${OPS[$op]}\n"
    done
) | MDHIM_CONFIF=$MDHIM_CONFIG mpirun -np $RANKS examples/cli
