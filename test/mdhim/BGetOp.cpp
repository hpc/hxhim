#include <gtest/gtest.h>
#include <mpi.h>

#include "mdhim.h"

typedef int Key_t;
typedef double Value_t;
static const std::size_t MDHIM_BGET_OP_NUM_RECORDS = 5;
static const Key_t       MDHIM_BGET_OP_KEYS[]   = {0, 1, 2, 3, 4};
static const Value_t     MDHIM_BGET_OP_VALUES[] = {0, 1, 2, 3, 4};

#define SETUP(md, opts) do {                                                            \
    ASSERT_EQ(mdhim_options_init(&(opts), MPI_COMM_WORLD, true, true), MDHIM_SUCCESS);  \
    ASSERT_EQ(mdhimInit(&(md), &(opts)), MDHIM_SUCCESS);                                \
                                                                                        \
    void **keys = new void *[MDHIM_BGET_OP_NUM_RECORDS]();                              \
    std::size_t *key_lens = new std::size_t[MDHIM_BGET_OP_NUM_RECORDS]();               \
    void **values = new void *[MDHIM_BGET_OP_NUM_RECORDS]();                            \
    std::size_t *value_lens = new std::size_t[MDHIM_BGET_OP_NUM_RECORDS]();             \
    for(std::size_t i = 0; i < MDHIM_BGET_OP_NUM_RECORDS; i++) {                        \
        keys[i] = (void *) &MDHIM_BGET_OP_KEYS[i];                                      \
        key_lens[i] = sizeof(Key_t);                                                    \
        values[i] = (void *) &MDHIM_BGET_OP_VALUES[i];                                  \
        value_lens[i] = sizeof(Value_t);                                                \
    }                                                                                   \
                                                                                        \
    /* BPUT the key value pairs */                                                      \
    {                                                                                   \
        mdhim_brm_t *brm = mdhimBPut(&(md), nullptr,                                    \
                                     keys, key_lens,                                    \
                                     values, value_lens,                                \
                                     MDHIM_BGET_OP_NUM_RECORDS);                        \
        ASSERT_NE(brm, nullptr);                                                        \
                                                                                        \
        int error = MDHIM_ERROR;                                                        \
        ASSERT_EQ(mdhim_brm_error(brm, &error), MDHIM_SUCCESS);                         \
        ASSERT_EQ(error, MDHIM_SUCCESS);                                                \
                                                                                        \
        mdhim_brm_destroy(brm);                                                         \
    }                                                                                   \
                                                                                        \
    /* Commit the changes */                                                            \
    EXPECT_EQ(mdhimCommit(&(md), nullptr), MDHIM_SUCCESS);                              \
                                                                                        \
    /* Flush the stats */                                                               \
    mdhimStatFlush(&(md), nullptr);                                                     \
                                                                                        \
    delete [] keys;                                                                     \
    delete [] key_lens;                                                                 \
    delete [] values;                                                                   \
    delete [] value_lens;                                                               \
} while (0)

TEST(mdhim, GET_FIRST) {
    mdhim_options_t opts;
    mdhim_t md;

    SETUP(md, opts);

    //bget value back
    {
        mdhim_bgrm_t *bgrm = mdhimBGetOp(&md, nullptr,
                                         (void *)&MDHIM_BGET_OP_KEYS[0], sizeof(MDHIM_BGET_OP_KEYS[0]),
                                         MDHIM_BGET_OP_NUM_RECORDS,
                                         TransportGetMessageOp::GET_FIRST);
        ASSERT_NE(bgrm, nullptr);

        int error = MDHIM_ERROR;
        ASSERT_EQ(mdhim_bgrm_error(bgrm, &error), MDHIM_SUCCESS);
        ASSERT_EQ(error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        std::size_t num_keys = 0;
        ASSERT_EQ(mdhim_bgrm_num_keys(bgrm, &num_keys), MDHIM_SUCCESS);
        ASSERT_EQ(num_keys, MDHIM_BGET_OP_NUM_RECORDS);

        Key_t **ks = new Key_t *[num_keys]();
        ASSERT_EQ(mdhim_bgrm_keys(bgrm, (void ***)&ks, nullptr), MDHIM_SUCCESS);
        for(std::size_t i = 0; i < MDHIM_BGET_OP_NUM_RECORDS; i++) {
            EXPECT_EQ(*ks[i], MDHIM_BGET_OP_KEYS[i]);
        }
        delete [] ks;

        Value_t **vs = new Value_t *[num_keys]();
        ASSERT_EQ(mdhim_bgrm_values(bgrm, (void ***)&vs, nullptr), MDHIM_SUCCESS);
        for(std::size_t i = 0; i < MDHIM_BGET_OP_NUM_RECORDS; i++) {
            EXPECT_EQ(*vs[i], MDHIM_BGET_OP_VALUES[i]);
        }
        delete [] vs;

        mdhim_bgrm_destroy(bgrm);
    }

    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}

TEST(mdhim, GET_NEXT) {
    mdhim_options_t opts;
    mdhim_t md;

    SETUP(md, opts);

    //bget value back
    {
        mdhim_bgrm_t *bgrm = mdhimBGetOp(&md, nullptr,
                                         (void *)&MDHIM_BGET_OP_KEYS[0], sizeof(MDHIM_BGET_OP_KEYS[0]),
                                         MDHIM_BGET_OP_NUM_RECORDS,
                                         TransportGetMessageOp::GET_NEXT);
        ASSERT_NE(bgrm, nullptr);

        int error = MDHIM_ERROR;
        ASSERT_EQ(mdhim_bgrm_error(bgrm, &error), MDHIM_SUCCESS);
        ASSERT_EQ(error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        std::size_t num_keys = 0;
        ASSERT_EQ(mdhim_bgrm_num_keys(bgrm, &num_keys), MDHIM_SUCCESS);
        ASSERT_EQ(num_keys, MDHIM_BGET_OP_NUM_RECORDS);

        Key_t **ks = new Key_t *[num_keys]();
        ASSERT_EQ(mdhim_bgrm_keys(bgrm, (void ***)&ks, nullptr), MDHIM_SUCCESS);
        for(std::size_t i = 0; i < MDHIM_BGET_OP_NUM_RECORDS; i++) {
            EXPECT_EQ(*ks[i], MDHIM_BGET_OP_KEYS[i]);
        }
        delete [] ks;

        Value_t **vs = new Value_t *[num_keys]();
        ASSERT_EQ(mdhim_bgrm_values(bgrm, (void ***)&vs, nullptr), MDHIM_SUCCESS);
        for(std::size_t i = 0; i < MDHIM_BGET_OP_NUM_RECORDS; i++) {
            EXPECT_EQ(*vs[i], MDHIM_BGET_OP_VALUES[i]);
        }
        delete [] vs;

        mdhim_bgrm_destroy(bgrm);
    }

    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}

TEST(mdhim, GET_LAST) {
    mdhim_options_t opts;
    mdhim_t md;

    SETUP(md, opts);

    //bget value back
    {
        mdhim_bgrm_t *bgrm = mdhimBGetOp(&md, nullptr,
                                         (void *) &MDHIM_BGET_OP_KEYS[MDHIM_BGET_OP_NUM_RECORDS - 1], sizeof(MDHIM_BGET_OP_KEYS[MDHIM_BGET_OP_NUM_RECORDS - 1]),
                                         MDHIM_BGET_OP_NUM_RECORDS,
                                         TransportGetMessageOp::GET_LAST);
        ASSERT_NE(bgrm, nullptr);

        int error = MDHIM_ERROR;
        ASSERT_EQ(mdhim_bgrm_error(bgrm, &error), MDHIM_SUCCESS);
        ASSERT_EQ(error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        std::size_t num_keys = 0;
        ASSERT_EQ(mdhim_bgrm_num_keys(bgrm, &num_keys), MDHIM_SUCCESS);
        ASSERT_EQ(num_keys, MDHIM_BGET_OP_NUM_RECORDS);

        Key_t **ks = new Key_t *[num_keys]();
        ASSERT_EQ(mdhim_bgrm_keys(bgrm, (void ***)&ks, nullptr), MDHIM_SUCCESS);
        for(std::size_t i = 0; i < MDHIM_BGET_OP_NUM_RECORDS; i++) {
            EXPECT_EQ(*ks[i], MDHIM_BGET_OP_KEYS[MDHIM_BGET_OP_NUM_RECORDS - i - 1]);
        }
        delete [] ks;

        Value_t **vs = new Value_t *[num_keys]();
        ASSERT_EQ(mdhim_bgrm_values(bgrm, (void ***)&vs, nullptr), MDHIM_SUCCESS);
        for(std::size_t i = 0; i < MDHIM_BGET_OP_NUM_RECORDS; i++) {
            EXPECT_EQ(*vs[i], MDHIM_BGET_OP_VALUES[MDHIM_BGET_OP_NUM_RECORDS - i - 1]);
        }
        delete [] vs;

        mdhim_bgrm_destroy(bgrm);
    }

    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}

TEST(mdhim, GET_PREV) {
    mdhim_options_t opts;
    mdhim_t md;

    SETUP(md, opts);

    //bget value back
    {
        mdhim_bgrm_t *bgrm = mdhimBGetOp(&md, nullptr,
                                         (void *) &MDHIM_BGET_OP_KEYS[MDHIM_BGET_OP_NUM_RECORDS - 1], sizeof(MDHIM_BGET_OP_KEYS[MDHIM_BGET_OP_NUM_RECORDS - 1]),
                                         MDHIM_BGET_OP_NUM_RECORDS,
                                         TransportGetMessageOp::GET_PREV);
        ASSERT_NE(bgrm, nullptr);

        int error = MDHIM_ERROR;
        ASSERT_EQ(mdhim_bgrm_error(bgrm, &error), MDHIM_SUCCESS);
        ASSERT_EQ(error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        std::size_t num_keys = 0;
        ASSERT_EQ(mdhim_bgrm_num_keys(bgrm, &num_keys), MDHIM_SUCCESS);
        ASSERT_EQ(num_keys, MDHIM_BGET_OP_NUM_RECORDS);

        Key_t **ks = new Key_t *[num_keys]();
        ASSERT_EQ(mdhim_bgrm_keys(bgrm, (void ***)&ks, nullptr), MDHIM_SUCCESS);
        for(std::size_t i = 0; i < MDHIM_BGET_OP_NUM_RECORDS; i++) {
            EXPECT_EQ(*ks[i], MDHIM_BGET_OP_KEYS[MDHIM_BGET_OP_NUM_RECORDS - i - 1]);
        }
        delete [] ks;

        Value_t **vs = new Value_t *[num_keys]();
        ASSERT_EQ(mdhim_bgrm_values(bgrm, (void ***)&vs, nullptr), MDHIM_SUCCESS);
        for(std::size_t i = 0; i < MDHIM_BGET_OP_NUM_RECORDS; i++) {
            EXPECT_EQ(*vs[i], MDHIM_BGET_OP_VALUES[MDHIM_BGET_OP_NUM_RECORDS - i - 1]);
        }
        delete [] vs;

        mdhim_bgrm_destroy(bgrm);
    }

    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}
