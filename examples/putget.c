#include <stdio.h>
#include <stdlib.h>

#include "mdhim.h"

// Some common values used across this code
typedef int Key_t;
typedef int Value_t;
const Key_t   MDHIM_PUT_GET_PRIMARY_KEY = 13579;
const Value_t MDHIM_PUT_GET_VALUE       = 24680;

// A quick and dirty cleanup function
void cleanup(mdhim_t *md, mdhim_options_t *opts) {
    mdhimClose(md);
    mdhim_options_destroy(opts);
    MPI_Finalize();
}

int main(int argc, char *argv[]){
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    srand(time(NULL));

    // Initialize options
    mdhim_options_t opts;
    mdhim_t md;

    // initialize options with default values
    if (mdhim_options_init(&opts) != MDHIM_SUCCESS) {
        cleanup(&md, &opts);
        return MDHIM_ERROR;
    }


    // if there are more arguments after argv[0], use thallium
    if (argc != 1) {
      mdhim_options_set_transporttype(&opts, MDHIM_TRANSPORT_THALLIUM);
    }

    // initialize mdhim context
    if (mdhimInit(&md, &opts) != MDHIM_SUCCESS) {
        cleanup(&md, &opts);
        return MDHIM_ERROR;
    }

    // Use arbitrary rank to do put
    if (rank == rand() % size) {
        // Put the key-value pair
        mdhim_brm_t *brm = mdhimPut(&md,
                                    (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                    (void *)&MDHIM_PUT_GET_VALUE, sizeof(MDHIM_PUT_GET_VALUE),
                                    NULL, NULL);
        int error = MDHIM_ERROR;
        if ((mdhim_brm_error(brm, &error) != MDHIM_SUCCESS) ||
            (error != MDHIM_SUCCESS)) {
            printf("Rank %d: Could not put\n", rank);
            mdhim_brm_destroy(brm);
            cleanup(&md, &opts);
            return MDHIM_ERROR;
        }

        mdhim_brm_destroy(brm);

        // Commit changes
        // Pass NULL here to use md->p->primary_index
        if (mdhimCommit(&md, NULL) != MDHIM_SUCCESS) {
            printf("Rank %d: Could not commit\n", rank);
            cleanup(&md, &opts);
            return MDHIM_ERROR;
        }

        printf("Rank %d put: %d -> %d\n", rank, MDHIM_PUT_GET_PRIMARY_KEY, MDHIM_PUT_GET_VALUE);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    // Every rank gets the value back
    {
        // Pass NULL here to use md->p->primary_index
        mdhim_getrm_t *grm = mdhimGet(&md, NULL,
                                      (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                      GET_EQ);
        int error = MDHIM_ERROR;
        if ((mdhim_grm_error(grm, &error) != MDHIM_SUCCESS) ||
            (error != MDHIM_SUCCESS)) {
            printf("Rank %d: Bad return value\n", rank);
            mdhim_grm_destroy(grm);
            cleanup(&md, &opts);
            return MDHIM_ERROR;
        }

        // Extract the keys from the returned value (do not free)
        Key_t *key = NULL;
        if (mdhim_grm_key(grm, (void **) &key, NULL) != MDHIM_SUCCESS) {
            printf("Rank %d: Could not extract key\n", rank);
            mdhim_grm_destroy(grm);
            cleanup(&md, &opts);
            return MDHIM_ERROR;
        }

        // Extract the values from the returned value (do not free)
        Value_t *value = NULL;
        if (mdhim_grm_value(grm, (void **) &value, NULL) != MDHIM_SUCCESS) {
            printf("Rank %d: Could not extract value\n", rank);
            mdhim_grm_destroy(grm);
            cleanup(&md, &opts);
            return MDHIM_ERROR;
        }

        // Print value gotten back
        printf("Rank %d got: %d -> %d\n", rank, *key, *value);

        mdhim_grm_destroy(grm);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    cleanup(&md, &opts);

    return MDHIM_SUCCESS;
}
