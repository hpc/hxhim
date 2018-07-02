/**
 * mdhim Unsafe BPUT BGET test
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

TEST(mdhim, UnsafeBPutBGet) {
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

    Key_t *key_ptr = (Key_t *) &MDHIM_UNSAFE_PUT_GET_PRIMARY_KEY;
    size_t key_len = sizeof(MDHIM_UNSAFE_PUT_GET_PRIMARY_KEY);

    // Put the key-value pair
    {
        const Value_t *value_ptr = &MDHIM_UNSAFE_PUT_GET_VALUE;
        size_t value_len = sizeof(MDHIM_UNSAFE_PUT_GET_VALUE);

        mdhim_brm_t *brm = mdhimUnsafeBPut(&md, nullptr,
                                           (void **) &key_ptr, &key_len,
                                           (void **) &value_ptr, &value_len,
                                           &wrong_db,
                                           1);
        ASSERT_NE(brm, nullptr);

        int error = MDHIM_ERROR;
        ASSERT_EQ(mdhim_brm_error(brm, &error), MDHIM_SUCCESS);
        EXPECT_EQ(error, MDHIM_SUCCESS);

        mdhim_brm_destroy(brm);
    }

    // Commit changes
    EXPECT_EQ(mdhimCommit(&md, nullptr), MDHIM_SUCCESS);

    // Fail to get value back from the correct database
    {
        mdhim_bgrm_t *bgrm = mdhimBGet(&md, nullptr,
                                       (void **) &key_ptr, &key_len,
                                       1,
                                       TransportGetMessageOp::GET_EQ);
        ASSERT_NE(bgrm, nullptr);

        int error = MDHIM_ERROR;
        ASSERT_EQ(mdhim_bgrm_error(bgrm, &error), MDHIM_SUCCESS);
        ASSERT_NE(error, MDHIM_SUCCESS);

        mdhim_bgrm_destroy(bgrm);
    }

    // Fail to get value back from the correct database using unsafe GET
    {
        mdhim_bgrm_t *bgrm = mdhimUnsafeBGet(&md, nullptr,
                                             (void **) &key_ptr, &key_len,
                                             &db,
                                             1,
                                             TransportGetMessageOp::GET_EQ);
        ASSERT_NE(bgrm, nullptr);

        int error = MDHIM_ERROR;
        ASSERT_EQ(mdhim_bgrm_error(bgrm, &error), MDHIM_SUCCESS);
        ASSERT_NE(error, MDHIM_SUCCESS);

        mdhim_bgrm_destroy(bgrm);
    }

    // Get value back from the wrong database
    {
        mdhim_bgrm_t *bgrm = mdhimUnsafeBGet(&md, nullptr,
                                             (void **) &key_ptr, &key_len,
                                             &wrong_db,
                                             1,
                                             TransportGetMessageOp::GET_EQ);
        ASSERT_NE(bgrm, nullptr);

        int error = MDHIM_ERROR;
        ASSERT_EQ(mdhim_bgrm_error(bgrm, &error), MDHIM_SUCCESS);
        ASSERT_EQ(error, MDHIM_SUCCESS);

        size_t num_keys = -1;
        ASSERT_EQ(mdhim_bgrm_num_keys(bgrm, &num_keys), MDHIM_SUCCESS);
        ASSERT_EQ(num_keys, 1);

        // Make sure key gotten back is correct
        Key_t **keys = new Key_t *[num_keys]();
        ASSERT_EQ(mdhim_bgrm_keys(bgrm, (void ***)&keys, nullptr), MDHIM_SUCCESS);
        EXPECT_EQ(*keys[0], MDHIM_UNSAFE_PUT_GET_PRIMARY_KEY);
        delete [] keys;

        // Make sure value gotten back is correct
        Value_t **values = new Value_t *[num_keys]();
        ASSERT_EQ(mdhim_bgrm_values(bgrm, (void ***)&values, nullptr), MDHIM_SUCCESS);
        EXPECT_EQ(*values[0], MDHIM_UNSAFE_PUT_GET_VALUE);
        delete [] values;

        mdhim_bgrm_destroy(bgrm);
    }

    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}
