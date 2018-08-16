#include <cstring>

#include <gtest/gtest.h>

#include "hxhim/hxhim.hpp"
#include "hxhim/private.hpp"

struct TestPut : hxhim::Results::Result {
    TestPut()
        : Result(HXHIM_RESULT_PUT)
    {}
};

struct TestGet : hxhim::Results::Result {
    TestGet()
        : Result(HXHIM_RESULT_GET)
    {}
};

struct TestDelete : hxhim::Results::Result {
    TestDelete()
        : Result(HXHIM_RESULT_DEL)
    {}
};

TEST(Results, PUT_GET_DEL) {
    hxhim_options_t opts;
    ASSERT_EQ(hxhim_options_init(&opts), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_mpi_bootstrap(&opts, MPI_COMM_WORLD),                    HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_datastore_in_memory(&opts),                              HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_datastores_per_range_server(&opts, 1),                   HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_hash_name(&opts, RANK.c_str()),                          HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_transport_thallium(&opts, "na+sm"),                      HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_queued_bputs(&opts, 1),                                  HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_first_n(&opts, 10),                            HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_bucket_gen_method(&opts, TEN_BUCKETS.c_str()), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    {
        hxhim::Results results(&hx);

        // nothing works yet since there is no data
        EXPECT_EQ(results.GoToHead(), nullptr);
        EXPECT_EQ(results.GoToNext(), nullptr);
        EXPECT_EQ(results.Valid(), false);

        // add some data
        hxhim::Results::Result *put = results.Add(hx.p->memory_pools.result->acquire<TestPut>());
        EXPECT_NE(put, nullptr);
        hxhim::Results::Result *get = results.Add(hx.p->memory_pools.result->acquire<TestGet>());
        EXPECT_NE(get, nullptr);
        hxhim::Results::Result *del = results.Add(hx.p->memory_pools.result->acquire<TestDelete>());
        EXPECT_NE(del, nullptr);

        EXPECT_EQ(results.Valid(), false);  // still not valid because current result has not been set yet
        EXPECT_EQ(results.GoToHead(), put);
        EXPECT_EQ(results.Valid(), true);   // valid now because current result is pointing to the head of the list
        EXPECT_EQ(results.GoToNext(), get);
        EXPECT_EQ(results.GoToNext(), del);
    }

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}

TEST(Results, Loop) {
    hxhim_options_t opts;
    ASSERT_EQ(hxhim_options_init(&opts), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_mpi_bootstrap(&opts, MPI_COMM_WORLD),                    HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_datastore_in_memory(&opts),                              HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_datastores_per_range_server(&opts, 1),                   HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_hash_name(&opts, RANK.c_str()),                          HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_transport_thallium(&opts, "na+sm"),                      HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_queued_bputs(&opts, 1),                                  HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_first_n(&opts, 10),                            HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_bucket_gen_method(&opts, TEN_BUCKETS.c_str()), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    {
        hxhim::Results results(&hx);

        // add some data
        const std::size_t puts = 10;
        for(std::size_t i = 0; i < puts; i++) {
            hxhim::Results::Result *put = results.Add(hx.p->memory_pools.result->acquire<TestPut>());
            EXPECT_NE(put, nullptr);
        }

        // check the data
        std::size_t count = 0;
        for(results.GoToHead(); results.Valid(); results.GoToNext()) {
            EXPECT_EQ(results.Curr()->GetType(), HXHIM_RESULT_PUT);
            count++;
        }

        EXPECT_EQ(count, puts);
    }

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}

TEST(Results, Append_Empty) {
    hxhim_options_t opts;
    ASSERT_EQ(hxhim_options_init(&opts), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_mpi_bootstrap(&opts, MPI_COMM_WORLD),                    HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_datastore_in_memory(&opts),                              HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_datastores_per_range_server(&opts, 1),                   HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_hash_name(&opts, RANK.c_str()),                          HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_transport_thallium(&opts, "na+sm"),                      HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_queued_bputs(&opts, 1),                                  HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_first_n(&opts, 10),                            HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_bucket_gen_method(&opts, TEN_BUCKETS.c_str()), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    {
        hxhim::Results results(&hx);

        // add some data
        hxhim::Results::Result *put = results.Add(hx.p->memory_pools.result->acquire<TestPut>());
        EXPECT_NE(put, nullptr);
        hxhim::Results::Result *get = results.Add(hx.p->memory_pools.result->acquire<TestGet>());
        EXPECT_NE(get, nullptr);
        hxhim::Results::Result *del = results.Add(hx.p->memory_pools.result->acquire<TestDelete>());
        EXPECT_NE(del, nullptr);

        // append empty set of results
        hxhim::Results empty(&hx);
        results.Append(&empty);

        EXPECT_EQ(results.GoToHead(), put);     // first result is PUT
        EXPECT_EQ(results.GoToNext(), get);     // next result is GET
        EXPECT_EQ(results.GoToNext(), del);     // next result is DEL
        EXPECT_EQ(results.GoToNext(), nullptr); // next result does not exist
        EXPECT_EQ(results.Valid(), false);
    }

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}

TEST(Results, Empty_Append) {
    hxhim_options_t opts;
    ASSERT_EQ(hxhim_options_init(&opts), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_mpi_bootstrap(&opts, MPI_COMM_WORLD),                    HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_datastore_in_memory(&opts),                              HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_datastores_per_range_server(&opts, 1),                   HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_hash_name(&opts, RANK.c_str()),                          HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_transport_thallium(&opts, "na+sm"),                      HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_queued_bputs(&opts, 1),                                  HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_first_n(&opts, 10),                            HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_bucket_gen_method(&opts, TEN_BUCKETS.c_str()), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    {
        hxhim::Results results(&hx);

        // add some data to the non-empty results
        hxhim::Results::Result *put = results.Add(hx.p->memory_pools.result->acquire<TestPut>());
        EXPECT_NE(put, nullptr);
        hxhim::Results::Result *get = results.Add(hx.p->memory_pools.result->acquire<TestGet>());
        EXPECT_NE(get, nullptr);
        hxhim::Results::Result *del = results.Add(hx.p->memory_pools.result->acquire<TestDelete>());
        EXPECT_NE(del, nullptr);

        // empty append set of results
        hxhim::Results empty(&hx);
        empty.Append(&results);

        EXPECT_EQ(empty.GoToHead(), put);     // first result is PUT
        EXPECT_EQ(empty.GoToNext(), get);     // next result is GET
        EXPECT_EQ(empty.GoToNext(), del);     // next result is DEL
        EXPECT_EQ(empty.GoToNext(), nullptr); // next result does not exist
        EXPECT_EQ(empty.Valid(), false);

        // appending moves the contents of the result list
        EXPECT_EQ(results.GoToHead(), nullptr);
    }

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
