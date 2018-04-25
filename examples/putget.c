#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "mdhim.h"

typedef int Key_t;
typedef int Value_t;

// A quick and dirty cleanup function
void cleanup(mdhim_t *md, mdhim_options_t *opts) {
    mdhimClose(md);
    mdhim_options_destroy(opts);
    MPI_Finalize();
}

int main(int argc, char *argv[]){
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    srand(time(NULL));

    // initialize options through config
    mdhim_options_t opts;
    if (mdhim_default_config_reader(&opts, MPI_COMM_WORLD) != MDHIM_SUCCESS) {
        printf("Error Reading Configuration\n");
        cleanup(NULL, &opts);
        return MDHIM_ERROR;
    }

    // initialize mdhim context
    mdhim_t md;
    if (mdhimInit(&md, &opts) != MDHIM_SUCCESS) {
        printf("Error Initializng MDHIM\n");
        cleanup(&md, &opts);
        return MDHIM_ERROR;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    // Generate a random key value pair
    const Key_t   MDHIM_PUT_GET_PRIMARY_KEY = rand();
    const Value_t MDHIM_PUT_GET_VALUE       = rand();

    // Use arbitrary rank to do put
    if (md.rank == rand() % md.size) {
        // Put the key-value pair
        mdhim_rm_t *rm = mdhimPut(&md, NULL,
                                  (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                  (void *)&MDHIM_PUT_GET_VALUE, sizeof(MDHIM_PUT_GET_VALUE));
        int error = MDHIM_ERROR;
        if ((mdhim_rm_error(rm, &error) != MDHIM_SUCCESS) ||
            (error != MDHIM_SUCCESS)) {
            printf("Rank %d: Could not put\n", md.rank);
            mdhim_rm_destroy(rm);
            cleanup(&md, &opts);
            return MDHIM_ERROR;
        }

        int src = -1;
        if (mdhim_rm_src(rm, &src) != MDHIM_SUCCESS) {
            printf("Rank %d: Could not get source of put response\n", md.rank);
            mdhim_rm_destroy(rm);
            cleanup(&md, &opts);
            return MDHIM_ERROR;
        }

        mdhim_rm_destroy(rm);

        // Commit changes
        // Pass NULL here to use md->p->primary_index
        if (mdhimCommit(&md, NULL) != MDHIM_SUCCESS) {
            printf("Rank %d: Could not commit\n", md.rank);
            cleanup(&md, &opts);
            return MDHIM_ERROR;
        }

        printf("Rank %d put: %d -> %d  to  range server %d\n", md.rank, MDHIM_PUT_GET_PRIMARY_KEY, MDHIM_PUT_GET_VALUE, src);
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
            printf("Rank %d: Bad return value\n", md.rank);
            mdhim_grm_destroy(grm);
            cleanup(&md, &opts);
            return MDHIM_ERROR;
        }

        // Extract the keys from the returned value (do not free)
        Key_t *key = NULL;
        if (mdhim_grm_key(grm, (void **) &key, NULL) != MDHIM_SUCCESS) {
            printf("Rank %d: Could not extract key\n", md.rank);
            mdhim_grm_destroy(grm);
            cleanup(&md, &opts);
            return MDHIM_ERROR;
        }

        // Extract the values from the returned value (do not free)
        Value_t *value = NULL;
        if (mdhim_grm_value(grm, (void **) &value, NULL) != MDHIM_SUCCESS) {
            printf("Rank %d: Could not extract value\n", md.rank);
            mdhim_grm_destroy(grm);
            cleanup(&md, &opts);
            return MDHIM_ERROR;
        }

        int src = -1;
        if (mdhim_grm_src(grm, &src) != MDHIM_SUCCESS) {
            printf("Rank %d: Could not get source of get response\n", md.rank);
            mdhim_grm_destroy(grm);
            cleanup(&md, &opts);
            return MDHIM_ERROR;
        }

        // Print value gotten back
        printf("Rank %d got: %d -> %d from range server %d\n", md.rank, *key, *value, src);

        mdhim_grm_destroy(grm);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    cleanup(&md, &opts);

    return MDHIM_SUCCESS;
}
