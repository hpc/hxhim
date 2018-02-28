#include <stdio.h>
#include <assert.h>
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

    mdhim_options_t opts;
    mdhim_t md;

    if (mdhim_options_init(&opts) != MDHIM_SUCCESS) {
        cleanup(&md);
        return MDHIM_ERROR;
    }

    // set some other values inside mdhim_options_t
    opts.comm = MPI_COMM_WORLD;
    mdhim_options_set_db_path(&opts, "./");
    mdhim_options_set_db_name(&opts, "mdhim");
    mdhim_options_set_db_type(&opts, LEVELDB);
    mdhim_options_set_key_type(&opts, MDHIM_INT_KEY); //Key_type = 1 (int)
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

    //Put the key-value pair
    struct mdhim_brm_t *brm = mdhimPut(&md,
                                       (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                       (void *)&MDHIM_PUT_GET_VALUE, sizeof(MDHIM_PUT_GET_VALUE),
                                       NULL, NULL);
    if (!brm || brm->error != MDHIM_SUCCESS) {
        cleanup(&md);
        mdhim_full_release_msg(brm);
        return MDHIM_ERROR;
    }
    mdhim_full_release_msg(brm);

    //Commit changes
    //Pass NULL here to use md->p->primary_index
    if (mdhimCommit(&md, NULL) != MDHIM_SUCCESS) {
        cleanup(&md);
        return MDHIM_ERROR;
    }

    //Get value back
    {
        //Pass NULL here to use md->p->primary_index
        struct mdhim_bgetrm_t *bgrm = mdhimGet(&md, NULL,
                                               (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                               MDHIM_GET_EQ);

        if (!bgrm || (bgrm->error != MDHIM_SUCCESS)) {
            cleanup(&md);
            return MDHIM_ERROR;
        }

        //Make sure value gotten back is correct
        if ((bgrm->num_keys != 1)                                 ||
            (*(Key_t *) *bgrm->keys != MDHIM_PUT_GET_PRIMARY_KEY) ||
            (*(Value_t *) *bgrm->values != MDHIM_PUT_GET_VALUE)) {
            mdhim_full_release_msg(bgrm);
            cleanup(&md);
            return MDHIM_ERROR;
        }

        printf("Original: %d -> %d\n", MDHIM_PUT_GET_PRIMARY_KEY, MDHIM_PUT_GET_VALUE);
        printf("Got back: %d -> %d\n", *(Key_t *) *bgrm->keys, *(Value_t *) *bgrm->values);
        mdhim_full_release_msg(bgrm);
    }

    cleanup(&md);

    return MDHIM_SUCCESS;
  }
