#include <gtest/gtest.h>
#include <mpi.h>

#include "check_memory.hpp"
#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"

static const std::size_t subjects[]   = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
static const std::size_t predicates[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
static const double      objects[]    = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

TEST(hxhim, Histogram) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);

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
    hxhim::Results::Destroy(&hx, put_results);

    hxhim::Results *histogram = hxhim::GetHistogram(&hx, 0);
    if (!histogram) {
        EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
        EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
    }
    ASSERT_NE(histogram, nullptr);

    for(histogram->GoToHead(); histogram->Valid(); histogram->GoToNext()) {
        hxhim::Results::Result *res = histogram->Curr();
        ASSERT_NE(res, nullptr);

        EXPECT_EQ(res->status, HXHIM_SUCCESS);
        EXPECT_EQ(res->type, HXHIM_RESULT_HISTOGRAM);

        hxhim::Results::Histogram *h = static_cast<hxhim::Results::Histogram *>(res);

        std::size_t size = h->size;
        EXPECT_EQ(size, 10);

        ASSERT_NE(h->counts, nullptr);

        // First 9 buckets have 1 value
        for(std::size_t i = 0; i < 9; i++) {
            EXPECT_EQ(h->counts[i], 1);
        }

        // Last bucket has 2 values
        EXPECT_EQ(h->counts[9], 2);
    }

    hxhim::Results::Destroy(&hx, histogram);

    CHECK_MEMORY(&hx);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}

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

    return HISTOGRAM_SUCCESS;
};

TEST(hxhim, BHistogram) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);
    ASSERT_EQ(hxhim_options_set_datastores_per_range_server(&opts, 10), HXHIM_SUCCESS);
    // ASSERT_EQ(hxhim_options_set_maximum_ops_per_send(&opts, 10), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_hash_function(&opts, "", test_hash, nullptr), HXHIM_SUCCESS);
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
    hxhim::Results::Destroy(&hx, put_results);

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

        EXPECT_EQ(res->status, HXHIM_SUCCESS);
        EXPECT_EQ(res->type, HXHIM_RESULT_HISTOGRAM);

        hxhim::Results::Histogram *h = static_cast<hxhim::Results::Histogram *>(res);

        EXPECT_EQ(h->size, 1);                                  // each histogram has 1 bucket

        ASSERT_NE(h->counts, nullptr);

        EXPECT_EQ(h->counts[0], expected_counts[count]);        // the buckets have different counts in them

        count++;
    }

    EXPECT_EQ(src_count, count);

    hxhim::Results::Destroy(&hx, bhistogram);

    CHECK_MEMORY(&hx);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
