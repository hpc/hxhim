#!/usr/bin/env bash
# generate_spo.sh
# This script generates random subject-predicate-object triples
# using the password generator program pwgen.
#

MIN_SUBJECT_LEN=1
MAX_SUBJECT_LEN=10
MIN_PREDICATE_LEN=1
MAX_PREDICATE_LEN=10
MIN_OBJECT_LEN=1
MAX_OBJECT_LEN=10
COUNT=10

function help() {
    echo "Usage: $(basename $0) [Options]"
    echo
    echo "    Options:"
    echo "        -h, --help           show help"
    echo "        -n, --count          number of key object pairs        ($COUNT)"
    echo "        --min_subject_len    the minimum length of a subject   ($MIN_SUBJECT_LEN)"
    echo "        --max_subject_len    the maximum length of a subject   ($MAX_SUBJECT_LEN)"
    echo "        --min_predicate_len  the minimum length of a predicate ($MIN_PREDICATE_LEN)"
    echo "        --max_predicate_len  the maximum length of a predicate ($MAX_PREDICATE_LEN)"
    echo "        --min_object_len     the minimum length of a object    ($MIN_OBJECT_LEN)"
    echo "        --max_object_len     the maximum length of a object    ($MAX_OBJECT_LEN)"
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
    shift # past object
    ;;
    --min_subject_len)
    MIN_SUBJECT_LEN=$2
    shift # past argument
    shift # past object
    ;;
    --max_subject_len)
    MAX_SUBJECT_LEN=$2
    shift # past argument
    shift # past object
    ;;
    --min_predicate_len)
    MIN_PREDICATE_LEN=$2
    shift # past argument
    shift # past object
    ;;
    --max_predicate_len)
    MAX_PREDICATE_LEN=$2
    shift # past argument
    shift # past object
    ;;
    --min_object_len)
    MIN_OBJECT_LEN=$2
    shift # past argument
    shift # past object
    ;;
    --max_object_len)
    MAX_OBJECT_LEN=$2
    shift # past argument
    shift # past object
    ;;
    *)    # unknown option
    shift # past argument
    ;;
esac
done

#Make sure optional arguments make sense
if [[ "$MAX_SUBJECT_LEN" -lt 1 ]] ; then
    echo "Maximum Subject Length ($MAX_SUBJECT_LEN) is too small"
    exit 1
fi

if [[ "$MIN_SUBJECT_LEN" -gt "$MAX_SUBJECT_LEN" ]] ; then
    echo "Minimum Subject Length ($MIN_SUBJECT_LEN) is larger than Maximum Subject Length ($MAX_SUBJECT_LEN)"
    exit 1
fi

if [[ "$MAX_PREDICATE_LEN" -lt 1 ]] ; then
    echo "Maximum Predicate Length ($MAX_PREDICATE_LEN) is too small"
    exit 1
fi

if [[ "$MIN_PREDICATE_LEN" -gt "$MAX_PREDICATE_LEN" ]] ; then
    echo "Minimum Predicate Length ($MIN_PREDICATE_LEN) is larger than Maximum Predicate Length ($MAX_PREDICATE_LEN)"
    exit 1
fi

if [[ "$MAX_OBJECT_LEN" -lt 1 ]] ; then
    echo "Maximum Object Length ($MAX_OBJECT_LEN) is too small"
    exit 1
fi

if [[ "$MIN_OBJECT_LEN" -gt "$MAX_OBJECT_LEN" ]] ; then
    echo "Minimum Object Length ($MIN_OBJECT_LEN) is larger than Maximum Object Length ($MAX_OBJECT_LEN)"
    exit 1
fi

if [[ "$COUNT" -lt 0 ]] ; then
    echo "Too few key object pairs: $COUNT"
    exit 1
fi

# Generate key object pairs
for i in $(seq 1 $COUNT); do
    echo $(pwgen $(shuf -i $MIN_SUBJECT_LEN-$MAX_SUBJECT_LEN -n 1) 1) $(pwgen $(shuf -i $MIN_PREDICATE_LEN-$MAX_PREDICATE_LEN -n 1) 1) $(pwgen $(shuf -i $MIN_OBJECT_LEN-$MAX_OBJECT_LEN -n 1) 1)
done
