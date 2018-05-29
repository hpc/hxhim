#include <gtest/gtest.h>
#include <mpi.h>

#include "hxhim.h"

typedef int Key_t;
typedef int Value_t;
static const Key_t   HXHIM_PUT_GET_PRIMARY_KEY = 13579;
static const Value_t HXHIM_PUT_GET_VALUE       = 24680;

TEST(hxhim, PutGet) {
    hxhim_t hx;

    ASSERT_EQ(hxhimOpen(&hx, MPI_COMM_WORLD, "mdhim.conf"), HXHIM_SUCCESS);

    // Add key value pairs for putting
    EXPECT_EQ(hxhimPut(&hx,
                         (void *)&HXHIM_PUT_GET_PRIMARY_KEY, sizeof(HXHIM_PUT_GET_PRIMARY_KEY),
                         (void *)&HXHIM_PUT_GET_VALUE, sizeof(HXHIM_PUT_GET_VALUE)), HXHIM_SUCCESS);

    // Only flush safe PUTs
    hxhim_return_t *put_results = hxhimFlushPuts(&hx);
    ASSERT_NE(put_results, nullptr);

    hxhim_return_destroy(put_results);

    // Add keys to get back
    EXPECT_EQ(hxhimGet(&hx,
                       (void *)&HXHIM_PUT_GET_PRIMARY_KEY, sizeof(HXHIM_PUT_GET_PRIMARY_KEY)), HXHIM_SUCCESS);

    // only flush safe GETs
    hxhim_return_t *get_results = hxhimFlushGets(&hx);
    ASSERT_NE(get_results, nullptr);

    // go to first range server
    ASSERT_EQ(hxhim_return_move_to_first_rs(get_results), HXHIM_SUCCESS);

    // make sure that this range server is valid
    int valid_rs = HXHIM_ERROR;
    ASSERT_EQ(hxhim_return_valid_rs(get_results, &valid_rs), HXHIM_SUCCESS);
    EXPECT_EQ(valid_rs, HXHIM_SUCCESS);

    // go to first key value pair
    ASSERT_EQ(hxhim_return_move_to_first_kv(get_results), HXHIM_SUCCESS);

    // make sure that the key value pairs can be obtained
    int error = HXHIM_ERROR;
    ASSERT_EQ(hxhim_return_get_error(get_results, &error), HXHIM_SUCCESS);
    EXPECT_EQ(error, HXHIM_SUCCESS);

    // get the key value pair back
    int valid_kv = HXHIM_ERROR;
    ASSERT_EQ(hxhim_return_valid_kv(get_results, &valid_kv), HXHIM_SUCCESS);
    ASSERT_EQ(valid_kv, HXHIM_SUCCESS);
    Key_t *key; size_t key_len;
    Value_t *value; size_t value_len;
    ASSERT_EQ(hxhim_return_get_kv(get_results, (void **) &key, &key_len, (void **) &value, &value_len), HXHIM_SUCCESS);
    EXPECT_EQ(*key, HXHIM_PUT_GET_PRIMARY_KEY);
    EXPECT_EQ(key_len, sizeof(HXHIM_PUT_GET_PRIMARY_KEY));
    EXPECT_EQ(*value, HXHIM_PUT_GET_VALUE);
    EXPECT_EQ(value_len, sizeof(HXHIM_PUT_GET_VALUE));

    // go to the next key value pair
    EXPECT_EQ(hxhim_return_next_kv(get_results), HXHIM_ERROR);

    // go to the next range server
    EXPECT_EQ(hxhim_return_next_rs(get_results), HXHIM_ERROR);

    // go to the next response
    EXPECT_EQ(hxhim_return_next(get_results), HXHIM_ERROR);

    hxhim_return_destroy(get_results);

    EXPECT_EQ(hxhimClose(&hx), HXHIM_SUCCESS);
}
