/**
 * mdhim Unsafe PUT GET test
 * Do not call this with the other tests. 2 databases are created instead
 * of 1, which will cause mdhimInit to fail for the other tests.
 */
#include <gtest/gtest.h>
#include <mpi.h>

#include "mdhim/mdhim.h"

typedef int Key_t;
typedef int Value_t;
static const Key_t   MDHIM_UNSAFE_PUT_GET_PRIMARY_KEY = 13579;
static const Value_t MDHIM_UNSAFE_PUT_GET_VALUE       = 24680;
static const int     DATABASE_COUNT                   = 2;

TEST(mdhim, UnsafePutGet) {
    mdhim_options_t opts;
    mdhim_t md;

    ASSERT_EQ(mdhim_options_init(&opts, MPI_COMM_WORLD, true, true), MDHIM_SUCCESS);
    ASSERT_EQ(mdhim_options_set_dbs_per_server(&opts, DATABASE_COUNT), MDHIM_SUCCESS);
    ASSERT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    // Make sure the number of databases there are matches the value set in the options
    const int num_dbs = mdhimDBCount(&md);
    ASSERT_EQ(num_dbs, DATABASE_COUNT);

    // Figure out where the key is supposed to go
    const int db = mdhimWhichDB(&md, (void *)&MDHIM_UNSAFE_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_UNSAFE_PUT_GET_PRIMARY_KEY));
    ASSERT_NE(db, MDHIM_ERROR);

    // Select a database different than the correct one to place the key-value pair into
    const int wrong_db = (db + 1) % num_dbs;
    ASSERT_NE(db, wrong_db);

    // Put the key-value pair
    {
        mdhim_rm_t *rm = mdhimUnsafePut(&md, nullptr,
                                        (void *)&MDHIM_UNSAFE_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_UNSAFE_PUT_GET_PRIMARY_KEY),
                                        (void *)&MDHIM_UNSAFE_PUT_GET_VALUE, sizeof(MDHIM_UNSAFE_PUT_GET_VALUE),
                                        wrong_db);
        ASSERT_NE(rm, nullptr);

        int error = MDHIM_ERROR;
        ASSERT_EQ(mdhim_rm_error(rm, &error), MDHIM_SUCCESS);
        EXPECT_EQ(error, MDHIM_SUCCESS);

        mdhim_rm_destroy(rm);
    }

    // Commit changes
    EXPECT_EQ(mdhimCommit(&md, nullptr), MDHIM_SUCCESS);

    // Fail to get value back from the correct database
    {
        mdhim_grm_t *grm = mdhimGet(&md, nullptr,
                                    (void *)&MDHIM_UNSAFE_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_UNSAFE_PUT_GET_PRIMARY_KEY),
                                    TransportGetMessageOp::GET_EQ);
        ASSERT_NE(grm, nullptr);

        int error = MDHIM_ERROR;
        ASSERT_EQ(mdhim_grm_error(grm, &error), MDHIM_SUCCESS);
        ASSERT_NE(error, MDHIM_SUCCESS);

        mdhim_grm_destroy(grm);
    }

    // Fail to get value back from the correct database using unsafe GET
    {
        mdhim_grm_t *grm = mdhimUnsafeGet(&md, nullptr,
                                          (void *)&MDHIM_UNSAFE_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_UNSAFE_PUT_GET_PRIMARY_KEY),
                                          db,
                                          TransportGetMessageOp::GET_EQ);
        ASSERT_NE(grm, nullptr);

        int error = MDHIM_ERROR;
        ASSERT_EQ(mdhim_grm_error(grm, &error), MDHIM_SUCCESS);
        ASSERT_NE(error, MDHIM_SUCCESS);

        mdhim_grm_destroy(grm);
    }

    // Get value back from the wrong database
    {
        mdhim_grm_t *grm = mdhimUnsafeGet(&md, nullptr,
                                          (void *)&MDHIM_UNSAFE_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_UNSAFE_PUT_GET_PRIMARY_KEY),
                                          wrong_db,
                                          TransportGetMessageOp::GET_EQ);
        ASSERT_NE(grm, nullptr);

        int error = MDHIM_ERROR;
        ASSERT_EQ(mdhim_grm_error(grm, &error), MDHIM_SUCCESS);
        ASSERT_EQ(error, MDHIM_SUCCESS);

        // Make sure key gotten back is correct
        Key_t *key = nullptr;
        ASSERT_EQ(mdhim_grm_key(grm, (void **)&key, nullptr), MDHIM_SUCCESS);
        EXPECT_EQ(*key, MDHIM_UNSAFE_PUT_GET_PRIMARY_KEY);

        // Make sure value gotten back is correct
        Value_t *value = nullptr;
        ASSERT_EQ(mdhim_grm_value(grm, (void **)&value, nullptr), MDHIM_SUCCESS);
        EXPECT_EQ(*value, MDHIM_UNSAFE_PUT_GET_VALUE);

        mdhim_grm_destroy(grm);
    }

    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}
