#include <gtest/gtest.h>

#include "TestDatastore.hpp"

TEST(datastore, GetStats) {
    TestDatastore ds(-1);

    uint64_t put_time = 0;
    std::size_t num_puts = 0;
    uint64_t get_time = 0;
    std::size_t num_gets = 0;

    // no stats yet
    {
        EXPECT_EQ(ds.GetStats(&put_time, &num_puts, &get_time, &num_gets), HXHIM_SUCCESS);
        EXPECT_EQ(put_time, 0);
        EXPECT_EQ(num_puts, 0);
        EXPECT_EQ(get_time, 0);
        EXPECT_EQ(num_gets, 0);
    }

    // do a few puts and gets
    // can use EXPECT_EQ since all uint64_ts are small integers
    for(std::size_t i = 1; i <= 10; i++) {
        const uint64_t total_put_time = PUT_TIME_UINT64 * i;
        const uint64_t total_get_time = GET_TIME_UINT64 * i;

        // do a put
        {
            ds.operate(static_cast<Transport::Request::BPut *>(nullptr));
            EXPECT_EQ(ds.GetStats(&put_time, &num_puts, &get_time, &num_gets), HXHIM_SUCCESS);
            EXPECT_EQ(put_time, total_put_time);
            EXPECT_EQ(num_puts, i);
            EXPECT_EQ(get_time, (i - 1) * GET_TIME_UINT64);
            EXPECT_EQ(num_gets, i - 1);
        }

        // do a get
        {
            ds.operate(static_cast<Transport::Request::BGet *>(nullptr));
            EXPECT_EQ(ds.GetStats(&put_time, &num_puts, &get_time, &num_gets), HXHIM_SUCCESS);
            EXPECT_EQ(put_time, total_put_time);
            EXPECT_EQ(num_puts, i);
            EXPECT_EQ(get_time, total_get_time);
            EXPECT_EQ(num_gets, i);
        }
    }
}
