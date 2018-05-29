#!/usr/bin/env bash
# random.sh
# This script generates random key value pairs and prints them to stdout.
# The output is formatted for HXHIM and MDHIM use.

MIN_KEY_LEN=1
MAX_KEY_LEN=10
MIN_VALUE_LEN=1
MAX_VALUE_LEN=10
COUNT=10
PRINT=true

function help() {
    echo "Usage: $(basename $0) [Options] PUT|GET|DEL|BPUT|BGET|BDEL|COMMIT|WHICH|BWHICH ..."
    echo
    echo "    Options:"
    echo "        -h, --help         show help"
    echo "        -n, --count        number of key value pairs     ($COUNT)"
    echo "        --print            print to stdout               ($PRINT)"
    echo "        --min_key_len      the minimum length of a key   ($MIN_KEY_LEN)"
    echo "        --max_key_len      the maximum length of a key   ($MAX_KEY_LEN)"
    echo "        --min_value_len    the minimum length of a value ($MIN_VALUE_LEN)"
    echo "        --max_value_len    the maximum length of a value ($MAX_VALUE_LEN)"
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
    --print)
    PRINT=$2
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

# Get $COUNT key value pairs
declare -A KEYPAIRS
while [[ "${#KEYPAIRS[@]}" -lt "$COUNT" ]] ; do
    # Generate key value pairs (there are potentially duplicates)
    kv_pairs=$($(dirname $0)/generate_kv_pairs.sh --count $(($COUNT - ${#KEYPAIRS[@]})) --min_key_len $MIN_KEY_LEN --max_key_len $MAX_KEY_LEN --min_value_len $MIN_VALUE_LEN --max_value_len $MAX_VALUE_LEN)
    if [[ "$?" -eq "1" ]] ; then
        echo $kv_pairs
        exit 1
    fi

    # Read the key value pairs in
    while read key value ; do
        KEYPAIRS[$key]=$value
    done < <(echo "$kv_pairs")
done

# Print configuration
echo "Key Length:          [$MIN_KEY_LEN, $MAX_KEY_LEN]"     1>&2
echo "Value Length:        [$MIN_VALUE_LEN, $MAX_VALUE_LEN]" 1>&2
echo "Number of KV Pairs:  ${#KEYPAIRS[@]}"                  1>&2

# Generate commands for each input operation here to prevent variables from becomming too big for echoing
for op in "${POSITIONAL[@]}"; do
    case $op in
        PUT)
            for key in ${!KEYPAIRS[@]}; do
                echo "PUT $key ${KEYPAIRS[$key]}"
            done
            ;;
        GET)
            for key in ${!KEYPAIRS[@]}; do
                echo "GET $key"
            done
            ;;
        DEL)
            for key in ${!KEYPAIRS[@]}; do
                echo "DEL $key"
            done
            ;;
        BPUT)
            echo -n "BPUT $COUNT";
            for key in ${!KEYPAIRS[@]}; do
                echo -n " $key ${KEYPAIRS[$key]}"
            done
            echo
            ;;
        BGET)
            echo -n "BGET $COUNT";
            for key in ${!KEYPAIRS[@]}; do
                echo -n " $key"
            done
            echo
            ;;
        BDEL)
            echo -n "BDEL $COUNT";
            for key in ${!KEYPAIRS[@]}; do
                echo -n " $key"
            done
            echo
            ;;
        COMMIT)
            echo "COMMIT"
            ;;
        WHICH)
            for key in ${!KEYPAIRS[@]}; do
                echo "WHICH $key"
            done
            ;;
        BWHICH)
            echo -n "BWHICH $COUNT";
            for key in ${!KEYPAIRS[@]}; do
                echo -n " $key"
            done
            echo
            ;;
    esac
done
