#include <cstdlib>
#include <ctime>
#include <type_traits>

#include <gtest/gtest.h>
#include <mpi.h>

#include "datastore/InMemory.hpp"
#include "hxhim/config.hpp"
#include "hxhim/hxhim.hpp"

static const std::size_t subjects[]   = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
static const std::size_t predicates[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
static const double      objects[]    = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

TEST(hxhim, Histogram) {
    hxhim_options_t opts;
    ASSERT_EQ(hxhim_options_init(&opts), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_mpi_bootstrap(&opts, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_datastore_in_memory(&opts), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_datastores_per_range_server(&opts, 1), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_hash_name(&opts, RANK.c_str()), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_transport_thallium(&opts, "na+sm"), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_queued_bputs(&opts, 1), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_first_n(&opts, 10), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_bucket_gen_method(&opts, TEN_BUCKETS.c_str()), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    // Put triples
    for(std::size_t i = 0; i < 11; i++) {
        EXPECT_EQ(hxhim::Put(&hx,
                             (void *)&subjects[i], sizeof(subjects[i]),
                             (void *)&predicates[i], sizeof(predicates[i]),
                             HXHIM_DOUBLE_TYPE, (void *)&objects[i], sizeof(objects[i])),
                  HXHIM_SUCCESS);
    }

    // Flush all queued items
    hxhim::Results *put_results = hxhim::Flush(&hx);
    ASSERT_NE(put_results, nullptr);
    delete put_results;

    hxhim::Results *histogram = hxhim::GetHistogram(&hx, 0);
    if (!histogram) {
        EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
        EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
    }
    ASSERT_NE(histogram, nullptr);

    for(histogram->GoToHead(); histogram->Valid(); histogram->GoToNext()) {
        hxhim::Results::Result *res = histogram->Curr();
        ASSERT_NE(res, nullptr);

        EXPECT_EQ(res->GetStatus(), HXHIM_SUCCESS);
        EXPECT_EQ(res->GetType(), HXHIM_RESULT_HISTOGRAM);

        hxhim::Results::Histogram *h = static_cast<hxhim::Results::Histogram *>(res);

        EXPECT_EQ(h->GetHistogram().size(), 10);

        // First 9 buckets have 1 value
        std::map<double, std::size_t>::const_iterator it = h->GetHistogram().begin();
        for(std::size_t i = 0; i < 9; i++) {
            EXPECT_EQ(it->second, 1);
            it++;
        }

        // Last bucket has 2 values
        EXPECT_EQ(it->second, 2);
    }

    delete histogram;
    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}

static int test_hash(hxhim_t *, void *subject, const size_t, void *, const size_t, void *) {
    const int ds = (int) * (std::size_t *) subject;
    return (ds < 10)?ds:9;
}

static Histogram::BucketGen::generator test_buckets = [](const std::list<double> &, std::map<double, std::size_t> &histogram, void *) -> int {
    histogram[0] = 0;
    return Histogram::SUCCESS;
};

TEST(hxhim, BHistogram) {
    hxhim_options_t opts;
    ASSERT_EQ(hxhim_options_init(&opts), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_mpi_bootstrap(&opts, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_datastore_in_memory(&opts), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_datastores_per_range_server(&opts, 10), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_hash_function(&opts, test_hash, nullptr), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_transport_thallium(&opts, "na+sm"), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_queued_bputs(&opts, 1), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_first_n(&opts, 1), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_bucket_gen_method(&opts, test_buckets, nullptr), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    // Put triples
    for(std::size_t i = 0; i < 11; i++) {
        EXPECT_EQ(hxhim::Put(&hx,
                             (void *)&subjects[i], sizeof(subjects[i]),
                             (void *)&predicates[i], sizeof(predicates[i]),
                             HXHIM_DOUBLE_TYPE, (void *)&objects[i], sizeof(objects[i])),
                  HXHIM_SUCCESS);
    }

    // Flush all queued items
    hxhim::Results *put_results = hxhim::Flush(&hx);
    ASSERT_NE(put_results, nullptr);
    delete put_results;

    const int srcs[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    const std::size_t src_count = sizeof(srcs) / sizeof(*srcs);
    const std::size_t expected_counts[] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 2};       // each datastore gets 1 value except the last one, which gets 2

    hxhim::Results *bhistogram = hxhim::GetBHistogram(&hx, srcs, src_count);
    if (!bhistogram) {
        EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
        EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
    }
    ASSERT_NE(bhistogram, nullptr);

    std::size_t count = 0;
    for(bhistogram->GoToHead(); bhistogram->Valid(); bhistogram->GoToNext()) {
        hxhim::Results::Result *res = bhistogram->Curr();
        ASSERT_NE(res, nullptr);

        EXPECT_EQ(res->GetStatus(), HXHIM_SUCCESS);
        EXPECT_EQ(res->GetType(), HXHIM_RESULT_HISTOGRAM);

        hxhim::Results::Histogram *h = static_cast<hxhim::Results::Histogram *>(res);

        EXPECT_EQ(h->GetHistogram().size(), 1);                                 // each histogram has 1 bucket
        EXPECT_EQ(h->GetHistogram().begin()->second, expected_counts[count]);   // the buckets have different counts in them

        count++;
    }

    EXPECT_EQ(count, src_count);

    delete bhistogram;
    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
