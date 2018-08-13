#include <cstdlib>
#include <ctime>
#include <type_traits>

#include <gtest/gtest.h>
#include <mpi.h>

#include "datastore/InMemory.hpp"
#include "hxhim/config.hpp"
#include "hxhim/hxhim.hpp"
#include "hxhim/private.hpp"

typedef uint64_t Subject_t;
typedef uint64_t Predicate_t;

TEST(hxhim, BadGet) {
    srand(time(NULL));

    const Subject_t SUBJECT     = (((Subject_t) rand()) << 32) | rand();
    const Predicate_t PREDICATE = (((Predicate_t) rand()) << 32) | rand();

    hxhim_options_t opts;
    ASSERT_EQ(hxhim_options_init(&opts), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_mpi_bootstrap(&opts, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_datastore_in_memory(&opts), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_datastores_per_range_server(&opts, 1), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_hash(&opts, RANK.c_str()), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_transport_thallium(&opts, "na+sm"), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_queued_bputs(&opts, 1), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_first_n(&opts, 10), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_bucket_gen_method(&opts, TEN_BUCKETS.c_str()), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    // Add nonexistant subject-predicate to get
    EXPECT_EQ(hxhim::GetDouble(&hx,
                               (void *)&SUBJECT, sizeof(SUBJECT),
                               (void *)&PREDICATE, sizeof(PREDICATE)),
              HXHIM_SUCCESS);

    // Flush all queued items
    hxhim::Results *get_results = hxhim::Flush(&hx);
    ASSERT_NE(get_results, nullptr);

    // get the results and compare them with the original data
    std::size_t count = 0;
    for(get_results->GoToHead(); get_results->Valid(); get_results->GoToNext()) {
        hxhim::Results::Result *res = get_results->Curr();
        ASSERT_NE(res, nullptr);

        EXPECT_NE(res->GetStatus(), HXHIM_SUCCESS);
        EXPECT_EQ(res->GetType(), HXHIM_RESULT_GET);

        count++;
    }

    delete get_results;
    EXPECT_EQ(count, 1);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
