#include <vector>

#include <gtest/gtest.h>

#include "TestHistogram.hpp"
#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"

static const std::size_t TRIPLES = 10;

static int test_hash(hxhim_t *hx,
                     void *, const size_t,
                     void *, const size_t,
                     void *) {
    int rank = -1;
    hxhim::GetMPI(hx, nullptr, &rank, nullptr);
    return rank;
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
    ASSERT_EQ(hxhim_options_set_hash_function(&opts, "test hash", test_hash, nullptr), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_first_n(&opts, 0), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_bucket_gen_function(&opts, test_buckets, nullptr), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_add_histogram_track_predicate(&opts, TEST_HIST_NAME), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    int rank = -1;
    int size = -1;
    EXPECT_EQ(hxhim::GetMPI(&hx, nullptr, &rank, &size), HXHIM_SUCCESS);

    // get this value early on just in case it is modified later on (should not happen)
    std::size_t total_rs = 0;
    EXPECT_EQ(hxhim::GetRangeServerCount(&hx, &total_rs), HXHIM_SUCCESS);

    std::vector<double>      subjects  (TRIPLES + 1);
    std::vector<std::string> predicates(TRIPLES + 1);
    std::vector<void *>      objects   (TRIPLES + 1);

    // PUT triples
    // The first TRIPLES - 1 buckets will have 1 item each
    // The last bucket will have 2 items
    for(std::size_t i = 0; i < TRIPLES; i++) {
        subjects[i] = rank;
        predicates[i] = TEST_HIST_NAME;
        objects[i] = nullptr;

        EXPECT_EQ(hxhim::Put(&hx,
                             (void *)&subjects[i],         sizeof(subjects[i]),  hxhim_data_t::HXHIM_DATA_DOUBLE,
                             (void *)predicates[i].data(), predicates[i].size(), hxhim_data_t::HXHIM_DATA_BYTE,
                             (void *)&objects[i],          sizeof(subjects[i]),  hxhim_data_t::HXHIM_DATA_POINTER,
                             HXHIM_PUT_SPO),
                  HXHIM_SUCCESS);
    }

    // flush all queued items
    hxhim::Results *put_results = hxhim::Flush(&hx);
    ASSERT_NE(put_results, nullptr);
    EXPECT_EQ(put_results->Size(), TRIPLES);

    HXHIM_CXX_RESULTS_LOOP(put_results) {
        hxhim_op_t op = hxhim_op_t::HXHIM_INVALID;
        EXPECT_EQ(put_results->Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_PUT);

        int rs = -1;
        EXPECT_EQ(put_results->RangeServer(&rs), HXHIM_SUCCESS);

        int status = HXHIM_ERROR;
        EXPECT_EQ(put_results->Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_SUCCESS);

        void *subject = nullptr;
        std::size_t subject_len = 0;
        EXPECT_EQ(put_results->Subject(&subject, &subject_len, nullptr), HXHIM_SUCCESS);
        EXPECT_NE(subject, nullptr);
        EXPECT_NE(subject_len, 0);
        // type changes depending on which PUT permutation this result came from

        void *predicate = nullptr;
        std::size_t predicate_len = 0;
        EXPECT_EQ(put_results->Predicate(&predicate, &predicate_len, nullptr), HXHIM_SUCCESS);
        EXPECT_NE(predicate, nullptr);
        EXPECT_NE(predicate_len, 0);
        // type changes depending on which PUT permutation this result came from
    }

    hxhim::Results::Destroy(put_results);

    // a single histogram from all ranks
    for(std::size_t i = 0; i < total_rs; i++) {
        EXPECT_EQ(hxhim::Histogram(&hx, i, TEST_HIST_NAME.data(), TEST_HIST_NAME.size()), HXHIM_SUCCESS);
    }

    hxhim::Results *hist_results = hxhim::Flush(&hx);
    ASSERT_NE(hist_results, nullptr);
    EXPECT_EQ(hist_results->Size(), (std::size_t) total_rs);

    HXHIM_CXX_RESULTS_LOOP(hist_results) {
        hxhim_op_t op = hxhim_op_t::HXHIM_INVALID;
        EXPECT_EQ(hist_results->Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_HISTOGRAM);

        int rs = -1;
        EXPECT_EQ(hist_results->RangeServer(&rs), HXHIM_SUCCESS);

        int status = HXHIM_ERROR;
        EXPECT_EQ(hist_results->Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_SUCCESS);

        const char *name = nullptr;
        std::size_t name_len = 0;
        double *buckets = nullptr;
        std::size_t *counts = nullptr;
        std::size_t size = 0;
        ASSERT_EQ(hist_results->Histogram(&name, &name_len, &buckets, &counts, &size), HXHIM_SUCCESS);

        EXPECT_EQ(std::string(name, name_len), TEST_HIST_NAME);
        ASSERT_NE(buckets, nullptr);
        ASSERT_NE(counts, nullptr);
        EXPECT_EQ(size, (std::size_t ) 1);

        EXPECT_EQ(buckets[0], 0);

        EXPECT_EQ(counts[0], TRIPLES);
    }

    hxhim::Results::Destroy(hist_results);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
