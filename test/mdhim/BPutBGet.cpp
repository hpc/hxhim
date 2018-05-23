#include <gtest/gtest.h>
#include <mpi.h>

#include "mdhim.h"

typedef int Key_t;
typedef int Value_t;
static const Key_t   MDHIM_BPUT_BGET_PRIMARY_KEY = 13579;
static const Value_t MDHIM_BPUT_BGET_VALUE       = 24680;

TEST(mdhim, BPutBGet) {
    mdhim_options_t opts;
    mdhim_t md;

    ASSERT_EQ(mdhim_options_init(&opts, MPI_COMM_WORLD, true, true), MDHIM_SUCCESS);
    ASSERT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    //Place the key and value into arrays
    void **keys = new void *[1]();
    std::size_t *key_lens = new std::size_t[1]();
    void **values = new void *[1]();
    std::size_t *value_lens = new std::size_t[1]();

    keys[0] = (void *)&MDHIM_BPUT_BGET_PRIMARY_KEY;
    key_lens[0] = sizeof(MDHIM_BPUT_BGET_PRIMARY_KEY);
    values[0] = (void *)&MDHIM_BPUT_BGET_VALUE;
    value_lens[0] = sizeof(MDHIM_BPUT_BGET_VALUE);

    //BPut the key-value pair
    {
        mdhim_brm_t *brm = mdhimBPut(&md, nullptr,
                                     keys, key_lens,
                                     values, value_lens,
                                     1);
        ASSERT_NE(brm, nullptr);

        int error = MDHIM_ERROR;
        ASSERT_EQ(mdhim_brm_error(brm, &error), MDHIM_SUCCESS);
        ASSERT_EQ(error, MDHIM_SUCCESS);

        mdhim_brm_destroy(brm);
    }

    //Commit changes
    EXPECT_EQ(mdhimCommit(&md, nullptr), MDHIM_SUCCESS);

    //bget value back
    {
        mdhim_bgrm_t *bgrm = mdhimBGet(&md, nullptr,
                                       keys, key_lens,
                                       1,
                                       TransportGetMessageOp::GET_EQ);
        ASSERT_NE(bgrm, nullptr);

        int error = MDHIM_ERROR;
        ASSERT_EQ(mdhim_bgrm_error(bgrm, &error), MDHIM_SUCCESS);
        ASSERT_EQ(error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        std::size_t num_keys = 0;
        ASSERT_EQ(mdhim_bgrm_num_keys(bgrm, &num_keys), MDHIM_SUCCESS);
        ASSERT_EQ(num_keys, 1);

        Key_t **ks = new Key_t *[num_keys]();
        ASSERT_EQ(mdhim_bgrm_keys(bgrm, (void ***)&ks, nullptr), MDHIM_SUCCESS);
        EXPECT_EQ(*ks[0], MDHIM_BPUT_BGET_PRIMARY_KEY);

        Value_t **vs = new Value_t *[num_keys]();
        ASSERT_EQ(mdhim_bgrm_values(bgrm, (void ***)&vs, nullptr), MDHIM_SUCCESS);
        EXPECT_EQ(*vs[0], MDHIM_BPUT_BGET_VALUE);

        delete [] ks;
        delete [] vs;

        mdhim_bgrm_destroy(bgrm);
    }

    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);

    delete [] keys;
    delete [] key_lens;
    delete [] values;
    delete [] value_lens;
}
