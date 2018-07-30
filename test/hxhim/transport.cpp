#include <cstdlib>
#include <ctime>
#include <type_traits>

#include <gtest/gtest.h>
#include <mpi.h>

#include "hxhim/backend/backends.hpp"
#include "hxhim/config.hpp"
#include "hxhim/hxhim.hpp"

TEST(transport, MPI) {
    hxhim_options_t opts;
    ASSERT_EQ(hxhim_options_init(&opts), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_mpi_bootstrap(&opts, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_database_in_memory(&opts), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_databases_per_range_server(&opts, 1), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_hash(&opts, SUM_MOD_DATABASES.c_str()), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_transport_mpi(&opts, 16777216, 96, 10), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_queued_bputs(&opts, 1), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_first_n(&opts, 10), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_bucket_gen_method(&opts, TEN_BUCKETS.c_str()), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}

TEST(transport, thallium_na_sm) {
    hxhim_options_t opts;
    ASSERT_EQ(hxhim_options_init(&opts), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_mpi_bootstrap(&opts, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_database_in_memory(&opts), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_databases_per_range_server(&opts, 1), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_hash(&opts, SUM_MOD_DATABASES.c_str()), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_transport_thallium(&opts, "na+sm"), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_queued_bputs(&opts, 1), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_first_n(&opts, 10), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_bucket_gen_method(&opts, TEN_BUCKETS.c_str()), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}

TEST(transport, thallium_tcp) {
    hxhim_options_t opts;
    ASSERT_EQ(hxhim_options_init(&opts), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_mpi_bootstrap(&opts, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_database_in_memory(&opts), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_databases_per_range_server(&opts, 1), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_hash(&opts, SUM_MOD_DATABASES.c_str()), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_transport_thallium(&opts, "tcp"), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_queued_bputs(&opts, 1), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_first_n(&opts, 10), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_bucket_gen_method(&opts, TEN_BUCKETS.c_str()), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
