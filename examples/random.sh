#!/usr/bin/env bash

MIN_KEY_LEN=1
MAX_KEY_LEN=10
MIN_VALUE_LEN=1
MAX_VALUE_LEN=10
COUNT=10
RANKS=10
MDHIM_CONFIG="mdhim.conf"

# Generate key value pairs
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

# delete old database
rm -rf mdhimTstDB--0-* mdhim_manifest*

# run the CLI
export MDHIM_CONFIF=$MDHIM_CONFIG
(
    for op in "$@"; do
        printf "${OPS[$op]}\n"
    done
) | mpirun -np $RANKS examples/cli
