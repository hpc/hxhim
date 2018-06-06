#!/usr/bin/env bash
# generate_commands.sh
# This script generates commands that can be passed into the MDHIM cli.

function help() {
    echo "Usage: $(basename $0) [-h | --help] [--kv filename] [PUT | GET | DEL | BPUT | BGET | BDEL | COMMIT | WHICH | BWHICH ...]"
    echo
    echo "Key Value pairs are space delimited and should come from a file or stdin"
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

# Read the key value pairs in
COUNT=0
KEYS=()
VALUES=()
while read key value ; do
    KEYS+=($key)
    VALUES+=($value)
    COUNT=$((COUNT+1))
done < ${FILE:-/dev/stdin}

# Generate commands for each input operation here to prevent variables from becomming too big for echoing
for op in "${POSITIONAL[@]}"; do
    case $op in
        PUT)
            for i in ${!KEYS[@]}; do
                echo "PUT ${KEYS[$i]} ${VALUES[$i]}"
            done
            ;;
        GET)
            for i in ${!KEYS[@]}; do
                echo "GET ${KEYS[$i]}"
            done
            ;;
        DEL)
            for i in ${!KEYS[@]}; do
                echo "DEL ${KEYS[$i]}"
            done
            ;;
        BPUT)
            echo -n "BPUT $COUNT";
            for i in ${!KEYS[@]}; do
                echo -n " ${KEYS[$i]} ${VALUES[$i]}"
            done
            echo
            ;;
        BGET)
            echo -n "BGET $COUNT";
            for i in ${!KEYS[@]}; do
                echo -n " ${KEYS[$i]}"
            done
            echo
            ;;
        BDEL)
            echo -n "BDEL $COUNT";
            for i in ${!KEYS[@]}; do
                echo -n " ${KEYS[$i]}"
            done
            echo
            ;;
        COMMIT)
            echo "COMMIT"
            ;;
        WHICH)
            for i in ${!KEYS[@]}; do
                echo "WHICH ${KEYS[$i]}"
            done
            ;;
        BWHICH)
            echo -n "BWHICH $COUNT";
            for i in ${!KEYS[@]}; do
                echo -n " ${KEYS[$i]}"
            done
            echo
            ;;
    esac
done
