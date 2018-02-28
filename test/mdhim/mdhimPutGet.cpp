#include <gtest/gtest.h>
#include <mpi.h>

#include "mdhim.h"
#include "mdhim_private.h" // needed to access private members for testing
#include "fill_db_opts.h"

//Constants used across all mdhimPutGet tests
typedef int Key_t;
typedef int Value_t;
const Key_t   MDHIM_PUT_GET_PRIMARY_KEY = 13579;
const Value_t MDHIM_PUT_GET_VALUE       = 24680;

typedef uint32_t sgk_t; // secondary global key type
typedef uint32_t slk_t; // secondary local key type

//Put and Get a key-value pair without secondary indexes
TEST(mdhimPutGet, no_secondary) {
    mdhim_options_t opts;
    mdhim_t md;

    EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    fill_db_opts(opts);

    EXPECT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    //Put the key-value pair
    struct mdhim_brm_t *brm = mdhimPut(&md,
                                       (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                       (void *)&MDHIM_PUT_GET_VALUE, sizeof(MDHIM_PUT_GET_VALUE),
                                       NULL, NULL);
    ASSERT_NE(brm, nullptr);

    EXPECT_EQ(brm->error, MDHIM_SUCCESS);
    mdhim_full_release_msg(brm);

    //Commit changes
    EXPECT_EQ(mdhimCommit(&md, md.p->primary_index), MDHIM_SUCCESS);

    //Get value back
    {
        struct mdhim_bgetrm_t *bgrm = mdhimGet(&md, md.p->primary_index,
                                               (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                               MDHIM_GET_EQ);
        ASSERT_NE(bgrm, nullptr);
        EXPECT_EQ(bgrm->error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        EXPECT_EQ(bgrm->num_keys, 1);
        EXPECT_EQ(*(Key_t *) *bgrm->keys, MDHIM_PUT_GET_PRIMARY_KEY);
        EXPECT_EQ(*(Value_t *) *bgrm->values, MDHIM_PUT_GET_VALUE);

        mdhim_full_release_msg(bgrm);
    }

    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
}

TEST(mdhimPutGet, secondary_global) {
    mdhim_options_t opts;
    mdhim_t md;

    EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    fill_db_opts(opts);

    EXPECT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    // secondary global key
    sgk_t sgk = md.p->mdhim_rank + 1;
    sgk_t *sgk_ptr = &sgk;
    int sgk_len = sizeof(sgk);

    //Create the secondary remote index
    struct index_t *sg_index = create_global_index(&md, 2, 5, LEVELDB, MDHIM_INT_KEY, NULL);
    ASSERT_NE(sg_index, nullptr);

    //Create the secondary info struct
    struct secondary_info *sg_info = mdhimCreateSecondaryInfo(sg_index,
                                                              (void **) &sgk_ptr, &sgk_len,
                                                              1, SECONDARY_GLOBAL_INFO);
    ASSERT_NE(sg_info, nullptr);

    //Put the key-value pair
    struct mdhim_brm_t *brm = mdhimPut(&md,
                                       (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                       (void *)&MDHIM_PUT_GET_VALUE, sizeof(MDHIM_PUT_GET_VALUE),
                                       sg_info, NULL);

    ASSERT_NE(brm, nullptr);
    EXPECT_EQ(brm->error, MDHIM_SUCCESS);
    mdhim_full_release_msg(brm);

    //Commit changes
    EXPECT_EQ(mdhimCommit(&md, md.p->primary_index), MDHIM_SUCCESS);

    //Get key back using the secondary global index
    struct mdhim_bgetrm_t *sg_ret = nullptr;
    {
        sg_ret = mdhimGet(&md, sg_index, (void *)sgk_ptr,
                          sgk_len, MDHIM_GET_EQ);

        ASSERT_NE(sg_ret, nullptr);
        EXPECT_EQ(sg_ret->error, MDHIM_SUCCESS);

        EXPECT_EQ(sg_ret->num_keys, 1);
        EXPECT_EQ(*(sgk_t *) *sg_ret->keys, sgk);
        EXPECT_EQ(*(Key_t *) *sg_ret->values, MDHIM_PUT_GET_PRIMARY_KEY);
    }

    //Get value back using the returned key
    {
        struct mdhim_bgetrm_t *bgrm = mdhimGet(&md, md.p->primary_index,
                                               (void *) *sg_ret->values, *sg_ret->value_lens,
                                               MDHIM_GET_EQ);
        ASSERT_NE(bgrm, nullptr);
        EXPECT_EQ(bgrm->error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        EXPECT_EQ(bgrm->num_keys, 1);
        EXPECT_EQ(*(Key_t *) *bgrm->keys, MDHIM_PUT_GET_PRIMARY_KEY);
        EXPECT_EQ(*(Value_t *) *bgrm->values, MDHIM_PUT_GET_VALUE);

        mdhim_full_release_msg(bgrm);
    }

    mdhim_full_release_msg(sg_ret);
    free(sg_info);

    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
}

TEST(mdhimPutGet, secondary_local) {
    mdhim_options_t opts;
    mdhim_t md;

    EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    fill_db_opts(opts);

    EXPECT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    // secondary local key
    slk_t slk = md.p->mdhim_rank + 1;
    slk_t *slk_ptr = &slk;
    int slk_len = sizeof(slk);

    //Create the secondary local index
    struct index_t *sl_index = create_local_index(&md, LEVELDB, MDHIM_INT_KEY, "local");
    ASSERT_NE(sl_index, nullptr);

    //Create the secondary info struct
    struct secondary_info *sl_info = mdhimCreateSecondaryInfo(sl_index,
                                                              (void **) &slk_ptr, &slk_len,
                                                              1, SECONDARY_GLOBAL_INFO);
    ASSERT_NE(sl_info, nullptr);

    struct mdhim_brm_t *brm = mdhimPut(&md,
                                       (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                       (void *)&MDHIM_PUT_GET_VALUE, sizeof(MDHIM_PUT_GET_VALUE),
                                       NULL, sl_info);

    ASSERT_NE(brm, nullptr);
    EXPECT_EQ(brm->error, MDHIM_SUCCESS);
    mdhim_full_release_msg(brm);

    //Commit changes
    EXPECT_EQ(mdhimCommit(&md, md.p->primary_index), MDHIM_SUCCESS);

    //Get the primary key from the secondary local index
    struct mdhim_bgetrm_t *sl_ret = nullptr;
    {
        //Get the stats for the secondary index so the client figures out who to query
        EXPECT_EQ(mdhimStatFlush(&md, sl_index), MDHIM_SUCCESS);

        sl_ret = mdhimGet(&md, sl_index, slk_ptr,
                          slk_len, MDHIM_GET_PRIMARY_EQ);
        ASSERT_NE(sl_ret, nullptr);
        EXPECT_EQ(sl_ret->error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        EXPECT_EQ(sl_ret->num_keys, 1);
        EXPECT_EQ(*(Key_t *) *sl_ret->keys, slk);
        EXPECT_EQ(*(Value_t *) *sl_ret->values, MDHIM_PUT_GET_PRIMARY_KEY);
    }

    //Get value back using the returned key
    {
        struct mdhim_bgetrm_t *bgrm = mdhimGet(&md, md.p->primary_index,
                                               (void *) *sl_ret->values, *sl_ret->value_lens,
                                               MDHIM_GET_EQ);
        ASSERT_NE(bgrm, nullptr);
        EXPECT_EQ(bgrm->error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        EXPECT_EQ(bgrm->num_keys, 1);
        EXPECT_EQ(*(slk_t *) *bgrm->keys, MDHIM_PUT_GET_PRIMARY_KEY);
        EXPECT_EQ(*(Key_t *) *bgrm->values, MDHIM_PUT_GET_VALUE);

        mdhim_full_release_msg(bgrm);
    }

    mdhim_full_release_msg(sl_ret);
    free(sl_info);

    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
}

TEST(mdhimPutGet, secondary_global_and_local) {
    mdhim_options_t opts;
    mdhim_t md;

    EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    fill_db_opts(opts);

    EXPECT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    // secondary global key
    sgk_t sgk = md.p->mdhim_rank + 1;
    sgk_t *sgk_ptr = &sgk;
    int sgk_len = sizeof(sgk);

    //Create the secondary remote index
    struct index_t *sg_index = create_global_index(&md, 2, 5, LEVELDB, MDHIM_INT_KEY, NULL);
    ASSERT_NE(sg_index, nullptr);

    //Create the secondary info struct
    struct secondary_info *sg_info = mdhimCreateSecondaryInfo(sg_index,
                                                              (void **) &sgk_ptr, &sgk_len,
                                                              1, SECONDARY_GLOBAL_INFO);
    ASSERT_NE(sg_info, nullptr);

    // secondary local key
    slk_t slk = md.p->mdhim_rank + 1;
    slk_t *slk_ptr = &slk;
    int slk_len = sizeof(slk);

    //Create the secondary local index
    struct index_t *sl_index = create_local_index(&md, LEVELDB, MDHIM_INT_KEY, "local");
    ASSERT_NE(sl_index, nullptr);

    //Create the secondary info struct
    struct secondary_info *sl_info = mdhimCreateSecondaryInfo(sl_index,
                                                              (void **) &slk_ptr, &slk_len,
                                                              1, SECONDARY_GLOBAL_INFO);
    ASSERT_NE(sl_info, nullptr);

    //Put the key-value pair
    struct mdhim_brm_t *brm = mdhimPut(&md,
                                       (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                       (void *)&MDHIM_PUT_GET_VALUE, sizeof(MDHIM_PUT_GET_VALUE),
                                       sg_info, sl_info);
    ASSERT_NE(brm, nullptr);

    EXPECT_EQ(brm->error, MDHIM_SUCCESS);
    mdhim_full_release_msg(brm);

    //Commit changes
    EXPECT_EQ(mdhimCommit(&md, md.p->primary_index), MDHIM_SUCCESS);

    //Get value back using primary key
    {
        struct mdhim_bgetrm_t *bgrm = mdhimGet(&md, md.p->primary_index,
                                               (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                               MDHIM_GET_EQ);
        ASSERT_NE(bgrm, nullptr);
        EXPECT_EQ(bgrm->error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        EXPECT_EQ(bgrm->num_keys, 1);
        EXPECT_EQ(*(Key_t *) *bgrm->keys, MDHIM_PUT_GET_PRIMARY_KEY);
        EXPECT_EQ(*(Value_t *) *bgrm->values, MDHIM_PUT_GET_VALUE);

        mdhim_full_release_msg(bgrm);
    }

    // secondary global index
    {
        //Get the primary key
        struct mdhim_bgetrm_t *sg_ret = nullptr;
        {
            sg_ret = mdhimGet(&md, sg_index, (void *)sgk_ptr,
                              sgk_len, MDHIM_GET_EQ);
            ASSERT_NE(sg_ret, nullptr);
            EXPECT_EQ(sg_ret->error, MDHIM_SUCCESS);

            EXPECT_EQ(sg_ret->num_keys, 1);
            EXPECT_EQ(*(sgk_t *) *sg_ret->keys, sgk);
            EXPECT_EQ(*(Key_t *) *sg_ret->values, MDHIM_PUT_GET_PRIMARY_KEY);
        }

        //Get value back using the returned key
        {
            struct mdhim_bgetrm_t *bgrm = mdhimGet(&md, md.p->primary_index,
                                                   (void *) *sg_ret->values, *sg_ret->value_lens,
                                                   MDHIM_GET_EQ);
            ASSERT_NE(bgrm, nullptr);
            EXPECT_EQ(bgrm->error, MDHIM_SUCCESS);

            //Make sure value gotten back is correct
            EXPECT_EQ(bgrm->num_keys, 1);
            EXPECT_EQ(*(Key_t *) *bgrm->keys, MDHIM_PUT_GET_PRIMARY_KEY);
            EXPECT_EQ(*(Value_t *) *bgrm->values, MDHIM_PUT_GET_VALUE);

            mdhim_full_release_msg(bgrm);
        }

        mdhim_full_release_msg(sg_ret);
        free(sg_info);
    }

    // secondary local index
    {
        //Get the primary key
        struct mdhim_bgetrm_t *sl_ret = nullptr;
        {
            //Get the stats for the secondary index so the client figures out who to query
            EXPECT_EQ(mdhimStatFlush(&md, sl_index), MDHIM_SUCCESS);

            sl_ret = mdhimGet(&md, sl_index, slk_ptr,
                              slk_len, MDHIM_GET_PRIMARY_EQ);
            ASSERT_NE(sl_ret, nullptr);
            EXPECT_EQ(sl_ret->error, MDHIM_SUCCESS);

            //Make sure value gotten back is correct
            EXPECT_EQ(sl_ret->num_keys, 1);
            EXPECT_EQ(*(slk_t *) *sl_ret->keys, slk);
            EXPECT_EQ(*(Key_t *) *sl_ret->values, MDHIM_PUT_GET_PRIMARY_KEY);
        }

        //Get value back using the returned key
        {
            struct mdhim_bgetrm_t *bgrm = mdhimGet(&md, md.p->primary_index,
                                                   (void *) *sl_ret->values, *sl_ret->value_lens,
                                                   MDHIM_GET_EQ);
            ASSERT_NE(bgrm, nullptr);
            EXPECT_EQ(bgrm->error, MDHIM_SUCCESS);

            //Make sure value gotten back is correct
            EXPECT_EQ(bgrm->num_keys, 1);
            EXPECT_EQ(*(Key_t *) *bgrm->keys, MDHIM_PUT_GET_PRIMARY_KEY);
            EXPECT_EQ(*(Value_t *) *bgrm->values, MDHIM_PUT_GET_VALUE);

            mdhim_full_release_msg(bgrm);
        }

        mdhim_full_release_msg(sl_ret);
        free(sl_info);
    }

    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
}
