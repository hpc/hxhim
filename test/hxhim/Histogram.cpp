#include <gtest/gtest.h>
#include <mpi.h>

#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"

static const std::size_t subjects[]   = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
static const std::size_t predicates[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
static const double      objects[]    = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

static int test_hash(hxhim_t *, void *subject, const size_t, void *, const size_t, void *) {
    const int ds = (int) * (std::size_t *) subject;
    return (ds < 10)?ds:9;
}

static HistogramBucketGenerator_t test_buckets = [](const double *, const size_t,
                                                    double **buckets, size_t *size,
                                                    void *) -> int {
    if (!buckets || !size) {
        return HISTOGRAM_ERROR;
    }

    *size = 1;
    *buckets = new double[*size]();
    (*buckets)[0] = 0;

    return HISTOGRAM_SUCCESS;
};

TEST(hxhim, Histogram) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);
    ASSERT_EQ(hxhim_options_set_datastores_per_range_server(&opts, 10), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_hash_function(&opts, "test hash", test_hash, nullptr), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_first_n(&opts, 0), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_bucket_gen_method(&opts, test_buckets, nullptr), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    // Put triples
    for(std::size_t i = 0; i < 11; i++) {
        EXPECT_EQ(hxhim::PutDouble(&hx,
                                   (void *)&subjects[i], sizeof(subjects[i]),
                                   (void *)&predicates[i], sizeof(predicates[i]),
                                   (double *) &objects[i]),
                  HXHIM_SUCCESS);
    }

    // Flush all queued items
    hxhim::Results *put_results = hxhim::Flush(&hx);
    ASSERT_NE(put_results, nullptr);
    hxhim::Results::Destroy(put_results);

    const int srcs[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    const std::size_t src_count = sizeof(srcs) / sizeof(*srcs);
    const std::size_t expected_counts[] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 2};       // each datastore gets 1 value except the last one, which gets 2

    Histogram::Histogram **hists = nullptr;
    std::size_t count = 0;
    ASSERT_EQ(hxhim::GetHistograms(&hx, 0, &hists, &count), HXHIM_SUCCESS);
    ASSERT_NE(hists, nullptr);
    EXPECT_EQ(count, src_count);

    for(std::size_t i = 0; i < src_count; i++) {
        double *buckets = nullptr;
        std::size_t *counts = nullptr;
        std::size_t size = 0;

        EXPECT_EQ(hists[i]->get(&buckets, &counts, &size), HISTOGRAM_SUCCESS);
        EXPECT_EQ(size, (std::size_t) 1);

        for(std::size_t j = 0; j < size; j++) {
            EXPECT_EQ(buckets[j], 0);
            EXPECT_EQ(counts[j], expected_counts[i]);
        }
    }

    EXPECT_EQ(hxhim::DestroyHistograms(hists, count), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
