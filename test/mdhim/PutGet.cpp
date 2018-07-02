#include <gtest/gtest.h>
#include <mpi.h>

#include "mdhim/mdhim.h"

typedef int Key_t;
typedef int Value_t;
static const Key_t   MDHIM_PUT_GET_PRIMARY_KEY = 13579;
static const Value_t MDHIM_PUT_GET_VALUE       = 24680;

TEST(mdhim, PutGet) {
    mdhim_options_t opts;
    mdhim_t md;

    ASSERT_EQ(mdhim_options_init(&opts, MPI_COMM_WORLD, true, true), MDHIM_SUCCESS);
    ASSERT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    //Put the key-value pair
    {
        mdhim_rm_t *rm = mdhimPut(&md, nullptr,
                                  (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                  (void *)&MDHIM_PUT_GET_VALUE, sizeof(MDHIM_PUT_GET_VALUE));
        ASSERT_NE(rm, nullptr);

        int error = MDHIM_ERROR;
        ASSERT_EQ(mdhim_rm_error(rm, &error), MDHIM_SUCCESS);
        EXPECT_EQ(error, MDHIM_SUCCESS);

        mdhim_rm_destroy(rm);
    }

    //Commit changes
    EXPECT_EQ(mdhimCommit(&md, nullptr), MDHIM_SUCCESS);

    //Get value back
    {
        mdhim_grm_t *grm = mdhimGet(&md, nullptr,
                                    (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                    TransportGetMessageOp::GET_EQ);
        ASSERT_NE(grm, nullptr);

        int error = MDHIM_ERROR;
        ASSERT_EQ(mdhim_grm_error(grm, &error), MDHIM_SUCCESS);
        ASSERT_EQ(error, MDHIM_SUCCESS);

        //Make sure key gotten back is correct
        Key_t *key = nullptr;
        ASSERT_EQ(mdhim_grm_key(grm, (void **)&key, nullptr), MDHIM_SUCCESS);
        EXPECT_EQ(*key, MDHIM_PUT_GET_PRIMARY_KEY);

        //Make sure value gotten back is correct
        Value_t *value = nullptr;
        ASSERT_EQ(mdhim_grm_value(grm, (void **)&value, nullptr), MDHIM_SUCCESS);
        EXPECT_EQ(*value, MDHIM_PUT_GET_VALUE);

        mdhim_grm_destroy(grm);
    }

    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}
