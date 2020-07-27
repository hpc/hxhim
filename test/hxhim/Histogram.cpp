#include <sstream>
#include <vector>

#include <gtest/gtest.h>
#include <mpi.h>

#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"

static const std::size_t DS_PER_RS = 10;
static const std::size_t TRIPLES = 10;

static int test_hash(hxhim_t *hx,
                     void *, const size_t,
                     void *predicate, const size_t,
                     void *) {
    int rank = -1;
    hxhim::GetMPI(hx, nullptr, &rank, nullptr);
    const std::size_t offset = * (std::size_t *) predicate;
    return (rank * DS_PER_RS) + (int) ((offset < DS_PER_RS)?offset:(DS_PER_RS - 1));
}

static HistogramBucketGenerator_t test_buckets = [](const double *, const size_t,
                                                    double **buckets, size_t *size,
                                                    void *) -> int {
    if (!buckets || !size) {
        return HISTOGRAM_ERROR;
    }

    *size = 1;
    *buckets = alloc_array<double>(*size);
    (*buckets)[0] = 0;

    return HISTOGRAM_SUCCESS;
};

TEST(hxhim, Histogram) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);
    ASSERT_EQ(hxhim_options_set_datastores_per_range_server(&opts, DS_PER_RS), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_hash_function(&opts, "test hash", test_hash, nullptr), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_first_n(&opts, 0), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_bucket_gen_function(&opts, test_buckets, nullptr), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    int rank = -1;
    int size = -1;
    EXPECT_EQ(hxhim::GetMPI(&hx, nullptr, &rank, &size), HXHIM_SUCCESS);

    // get this value early on just in case it is modified later on (should not happen)
    std::size_t total_ds = 0;
    EXPECT_EQ(hxhim::GetDatastoreCount(&hx, &total_ds), HXHIM_SUCCESS);

    std::vector<std::size_t> subjects  (TRIPLES + 1);
    std::vector<std::size_t> predicates(TRIPLES + 1);
    std::vector<double>      objects   (TRIPLES + 1);

    // PUT triples
    // The first TRIPLES - 1 buckets will have 1 item each
    // The last bucket will have 2 items
    for(std::size_t i = 0; i < (TRIPLES + 1); i++) {
        subjects[i] = rank;
        predicates[i] = i;
        objects[i] = i;

        EXPECT_EQ(hxhim::PutDouble(&hx,
                                   (void *)&subjects[i], sizeof(subjects[i]),
                                   (void *)&predicates[i], sizeof(predicates[i]),
                                   &objects[i]),
                  HXHIM_SUCCESS);
    }

    // flush all queued items
    hxhim::Results *put_results = hxhim::Flush(&hx);
    ASSERT_NE(put_results, nullptr);
    hxhim::Results::Destroy(put_results);

    // request all histograms
    for(std::size_t i = 0; i < total_ds; i++) {
        EXPECT_EQ(hxhim::Histogram(&hx, i), HXHIM_SUCCESS);
    }

    hxhim::Results *hist_results = hxhim::Flush(&hx);
    ASSERT_NE(hist_results, nullptr);
    EXPECT_EQ(hist_results->Size(), (std::size_t) total_ds);

    std::size_t i = 0;
    for(hist_results->GoToHead(); hist_results->ValidIterator(); hist_results->GoToNext()) {
        hxhim_op_t op = hxhim_op_t::HXHIM_INVALID;
        EXPECT_EQ(hist_results->Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_HISTOGRAM);

        int status = HXHIM_ERROR;
        EXPECT_EQ(hist_results->Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_SUCCESS);

        double *buckets = nullptr;
        std::size_t *counts = nullptr;
        std::size_t size = 0;
        EXPECT_EQ(hist_results->Histogram(&buckets, &counts, &size), HXHIM_SUCCESS);

        EXPECT_EQ(size, (std::size_t) 1);

        if (i < (TRIPLES - 1)) {
            EXPECT_EQ(buckets[0], 0);
            EXPECT_EQ(counts[0], 1);
        }
        else {
            EXPECT_EQ(buckets[0], 0);
            EXPECT_EQ(counts[0], 2);
        }

        i++;
    }

    hxhim::Results::Destroy(hist_results);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
