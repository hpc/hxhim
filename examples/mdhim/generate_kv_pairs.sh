#!/usr/bin/env bash
# generate_kv_pairs.sh
# This script generates random key value pairs
# using the password generator program pwgen.
#

MIN_KEY_LEN=1
MAX_KEY_LEN=10
MIN_VALUE_LEN=1
MAX_VALUE_LEN=10
COUNT=10

function help() {
    echo "Usage: $(basename $0) [Options]"
    echo
    echo "    Options:"
    echo "        -h, --help       show help"
    echo "        -n, --count      number of key value pairs     ($COUNT)"
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
    shift # past argument
    ;;
esac
done

#Make sure optional arguments make sense
if [[ "$MAX_KEY_LEN" -lt 1 ]] ; then
    echo "Maximum Key Length ($MAX_KEY_LEN) is too small"
    exit 1
fi

if [[ "$MIN_KEY_LEN" -gt "$MAX_KEY_LEN" ]] ; then
    echo "Minimum Key Length ($MIN_KEY_LEN) is larger than Maximum Key Length ($MAX_KEY_LEN)"
    exit 1
fi

if [[ "$MAX_VALUE_LEN" -lt 1 ]] ; then
    echo "Maximum Value Length ($MAX_VALUE_LEN) is too small"
    exit 1
fi

if [[ "$MIN_VALUE_LEN" -gt "$MAX_VALUE_LEN" ]] ; then
    echo "Minimum Value Length ($MIN_VALUE_LEN) is larger than Maximum Value Length ($MAX_VALUE_LEN)"
    exit 1
fi

if [[ "$COUNT" -lt 0 ]] ; then
    echo "Too few key value pairs: $COUNT"
    exit 1
fi

# Generate key value pairs
for i in $(seq 1 $COUNT); do
    echo $(pwgen $(shuf -i $MIN_KEY_LEN-$MAX_KEY_LEN -n 1) 1) $(pwgen $(shuf -i $MIN_VALUE_LEN-$MAX_VALUE_LEN -n 1) 1)
done
