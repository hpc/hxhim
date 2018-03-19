#include <stdio.h>
#include <stdlib.h>

#include "mdhim.h"

/*
 * cleanup
 * A quick and dirty cleanup function
 *
 * param md mdhim instance to close
 */
void cleanup(mdhim_t *md) {
    mdhimClose(md);
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

    mdhim_options_t opts;
    mdhim_t md;

    if (mdhim_options_init(&opts) != MDHIM_SUCCESS) {
        cleanup(&md);
        return MDHIM_ERROR;
    }

    //Set some other values inside mdhim_options_t
    opts.comm = MPI_COMM_WORLD;
    mdhim_options_set_db_path(&opts, "./");
    mdhim_options_set_db_name(&opts, "mdhim");
    mdhim_options_set_db_type(&opts, LEVELDB);
    mdhim_options_set_key_type(&opts, MDHIM_INT_KEY);
    mdhim_options_set_debug_level(&opts, MLOG_CRIT);
    mdhim_options_set_server_factor(&opts, 1);

    if (mdhimInit(&md, &opts) != MDHIM_SUCCESS) {
        cleanup(&md);
        return MDHIM_ERROR;
    }

    typedef int Key_t;
    typedef int Value_t;
    const Key_t   MDHIM_PUT_GET_PRIMARY_KEY = 13579;
    const Value_t MDHIM_PUT_GET_VALUE       = 24680;

    //Use arbitrary rank to do put
    if (rank == rand() % size) {
        //Put the key-value pair
        mdhim_brm_t *brm = mdhimPut(&md,
                                    (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                    (void *)&MDHIM_PUT_GET_VALUE, sizeof(MDHIM_PUT_GET_VALUE),
                                    NULL, NULL);
        if (mdhim_brm_error(brm) != MDHIM_SUCCESS) {
            mdhim_brm_destroy(brm);
            cleanup(&md);
            return MDHIM_ERROR;
        }

        mdhim_brm_destroy(brm);

        //Commit changes
        //Pass NULL here to use md->p->primary_index
        if (mdhimCommit(&md, NULL) != MDHIM_SUCCESS) {
            cleanup(&md);
            return MDHIM_ERROR;
        }

        printf("Rank %d put: %d -> %d\n", rank, MDHIM_PUT_GET_PRIMARY_KEY, MDHIM_PUT_GET_VALUE);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    //Every rank gets the value back
    {
        //Pass NULL here to use md->p->primary_index
        mdhim_bgetrm_t *bgrm = mdhimGet(&md, NULL,
                                        (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                        GET_EQ);
        if (mdhim_bgrm_error(bgrm) != MDHIM_SUCCESS) {
            mdhim_bgrm_destroy(bgrm);
            cleanup(&md);
            return MDHIM_ERROR;
        }

        // Extract the keys from the returned value (do not free)
        Key_t **keys = NULL;
        if (mdhim_bgrm_keys(bgrm, (void ***) &keys, NULL) != MDHIM_SUCCESS) {
            mdhim_bgrm_destroy(bgrm);
            cleanup(&md);
            return MDHIM_ERROR;
        }

        // Extract the values from the returned value (do not free)
        Value_t **values = NULL;
        if (mdhim_bgrm_values(bgrm, (void ***) &values, NULL) != MDHIM_SUCCESS) {
            mdhim_bgrm_destroy(bgrm);
            cleanup(&md);
            return MDHIM_ERROR;
        }

        //Print value gotten back
        printf("Rank %d got: %d -> %d\n", rank, **keys, **values);

        mdhim_bgrm_destroy(bgrm);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    cleanup(&md);

    return MDHIM_SUCCESS;
}
