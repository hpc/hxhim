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
    *buckets = new double[*size]();
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

    // where the histograms will be collected
    // the rank selected by rank 0 is used
    int dst_rank = rand() % size;
    ASSERT_EQ(MPI_Bcast(&dst_rank, 1, MPI_INT, 0, MPI_COMM_WORLD), MPI_SUCCESS);

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

    // Flush all queued items
    hxhim::Results *put_results = hxhim::Flush(&hx);
    ASSERT_NE(put_results, nullptr);
    hxhim::Results::Destroy(put_results);

    // extract all histograms across this HXHIM instance
    hxhim_histograms_t *hists = nullptr;
    ASSERT_EQ(hxhim::GetHistograms(&hx, dst_rank, &hists), HXHIM_SUCCESS);

    // only dst_rank has the data
    if (rank == dst_rank)  {
        ASSERT_NE(hists, nullptr);

        // get how many histograms there are
        std::size_t hist_count = 0;
        EXPECT_EQ(hxhim::Histograms::Count(hists, &hist_count), HXHIM_SUCCESS);
        EXPECT_EQ(hist_count, total_ds);

        // extract histogram data and check values
        for(std::size_t i = 0; i < hist_count; i++) {
            double *buckets = nullptr;
            std::size_t *counts = nullptr;
            std::size_t size = 0;

            EXPECT_EQ(hxhim::Histograms::Get(hists, i, &buckets, &counts, &size), HXHIM_SUCCESS);
            EXPECT_EQ(size, (std::size_t) 1);

            // all buckets start at 0
            for(std::size_t j = 0; j < size; j++) {
                EXPECT_EQ(buckets[j], 0);
            }

            // the last datastore per rank got 2 values
            if ((i % DS_PER_RS) == (DS_PER_RS - 1)) {
                for(std::size_t j = 0; j < size; j++) {
                    EXPECT_EQ(counts[j],  2);
                }
            }
            // all the other datastores got 1
            else {
                for(std::size_t j = 0; j < size; j++) {
                    EXPECT_EQ(counts[j],  1);
                }
            }
        }

        EXPECT_EQ(hxhim::Histograms::Destroy(hists), HXHIM_SUCCESS);
    }
    else {
        EXPECT_EQ(hists, nullptr);
    }

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
