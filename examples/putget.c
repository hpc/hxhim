#include <stdio.h>
#include <assert.h>
#include "mdhim.h"

int main(int argc, char *argv[]){
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    mdhim_options_t opts;
    /* mdhim_t md; // not supported */
    mdhim_t *md = mdhimAllocate();

    assert(mdhim_options_init(&opts) == MDHIM_SUCCESS);
    opts.comm = MPI_COMM_WORLD;
    mdhim_options_set_db_path(&opts, "./");
    mdhim_options_set_db_name(&opts, "mdhim");
    mdhim_options_set_db_type(&opts, LEVELDB);
    mdhim_options_set_key_type(&opts, MDHIM_INT_KEY); //Key_type = 1 (int)
    mdhim_options_set_debug_level(&opts, MLOG_CRIT);
    mdhim_options_set_server_factor(&opts, 1);

    assert(mdhimInit(md, &opts) == MDHIM_SUCCESS);

    typedef int Key_t;
    typedef int Value_t;
    const Key_t   MDHIM_PUT_GET_PRIMARY_KEY = 13579;
    const Value_t MDHIM_PUT_GET_VALUE       = 24680;

    //Put the key-value pair
    struct mdhim_brm_t *brm = mdhimPut(md,
                                       (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                       (void *)&MDHIM_PUT_GET_VALUE, sizeof(MDHIM_PUT_GET_VALUE),
                                       NULL, NULL);
    assert(brm);
    assert(brm->error == MDHIM_SUCCESS);
    mdhim_full_release_msg(brm);

    //Commit changes
    // pass NULL here to use md->primary_index
    assert(mdhimCommit(md, NULL) == MDHIM_SUCCESS);

    //Get value back
    {
        // pass NULL here to use md->primary_index
        struct mdhim_bgetrm_t *bgrm = mdhimGet(md, NULL,
                                               (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                               MDHIM_GET_EQ);
        assert(bgrm);
        assert(bgrm->error == MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        assert(bgrm->num_keys == 1);
        assert(*(Key_t *) *bgrm->keys == MDHIM_PUT_GET_PRIMARY_KEY);
        assert(*(Value_t *) *bgrm->values == MDHIM_PUT_GET_VALUE);

        printf("Original: %d -> %d\n", MDHIM_PUT_GET_PRIMARY_KEY, MDHIM_PUT_GET_VALUE);
        printf("Got back: %d -> %d\n", *(Key_t *) *bgrm->keys, *(Value_t *) *bgrm->values);
        mdhim_full_release_msg(bgrm);
    }

    assert(mdhimClose(md) == MDHIM_SUCCESS);
    mdhimDestroy(&md);

    MPI_Finalize();

    return 0;
  }
