#!/usr/bin/env bash
# generate_commands.sh
# This script generates commands that can be passed into the HXHIM cli.

function help() {
    echo "Usage: $(basename $0) [-h | --help] [-spo filename] [PUT | GET | DEL | BPUT | BGET | BDEL | FLUSH ...]"
    echo
    echo "Subject Predicate Object triples are space delimited and should come from stdin"
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
        --kv)
            FILE=$2
            shift
            shift
            ;;
        *)    # unknown option
            POSITIONAL+=("$1") # save it in an array for later
            shift # past argument
            ;;
    esac
done

# Read the key value pairs in from stdin
COUNT=0
declare -A SUBJECTS
declare -A PREDICATES
declare -A OBJECTS
while read subject predicate object ; do
    SUBJECTS["$COUNT"]=$subject
    PREDICATES["$COUNT"]=$predicate
    OBJECTS["$COUNT"]=$object
    COUNT=$((COUNT+1))
done < ${FILE:-/dev/stdin}

# Generate commands for each input operation here to prevent variables from becomming too big for echoing
for op in "${POSITIONAL[@]}"; do
    case $op in
        PUT)
            for i in ${!SUBJECTS[@]}; do
                echo "PUT ${SUBJECTS[$i]} ${PREDICATES[$i]} ${OBJECTS[$i]}"
            done
            ;;
        GET)
            for i in ${!SUBJECTS[@]}; do
                echo "GET ${SUBJECTS[$i]} ${PREDICATES[$i]}"
            done
            ;;
        DEL)
            for i in ${!SUBJECTS[@]}; do
                echo "DEL ${SUBJECTS[$i]} ${PREDICATES[$i]}"
            done
            ;;
        BPUT)
            echo -n "BPUT $COUNT";
            for i in ${!SUBJECTS[@]}; do
                echo -n " ${SUBJECTS[$i]} ${PREDICATES[$i]} ${OBJECTS[$i]}"
            done
            echo
            ;;
        BGET)
            echo -n "BGET $COUNT";
            for i in ${!SUBJECTS[@]}; do
                echo -n " ${SUBJECTS[$i]} ${PREDICATES[$i]}"
            done
            echo
            ;;
        BDEL)
            echo -n "BDEL $COUNT";
            for i in ${!SUBJECTS[@]}; do
                echo -n " ${SUBJECTS[$i]} ${PREDICATES[$i]}"
            done
            echo
            ;;
        FLUSH)
            echo "FLUSH"
            ;;
    esac
done
