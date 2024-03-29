#include <gtest/gtest.h>

#include "TestDatastore.hpp"
#include "utils/elen.hpp"

const std::string TEST_HIST_NAME = "Test Histogram Name";
const std::size_t FIRST_N = 10;
const std::size_t HISTOGRAM_COUNT = 5;

void check_nothing_happend(Histogram::Histogram *hist) {
    std::size_t first_n = 0;
    double *cache = nullptr;
    std::size_t cache_size = 0;

    EXPECT_EQ(hist->get_cache(&first_n, &cache, &cache_size), HISTOGRAM_SUCCESS);
    EXPECT_EQ(first_n, FIRST_N);
    EXPECT_NE(cache, nullptr);
    EXPECT_EQ(cache_size, 0);
    EXPECT_EQ(hist->get(nullptr, nullptr, nullptr), HISTOGRAM_ERROR);
}

TEST(datastore, Histogram) {
    // This histogram will be owned by the datastore
    Histogram::Histogram *hist = construct<Histogram::Histogram>(
                                     Histogram::Config{
                                         FIRST_N,
                                         [](const double *, const size_t,
                                            double **buckets, size_t *size,
                                            void *) -> int {
                                             if (!buckets || !size) {
                                                 return HISTOGRAM_ERROR;
                                             }

                                             *size = 1;
                                             *buckets = alloc_array<double>(*size + 1);
                                             (*buckets)[0] = 0;

                                             return HISTOGRAM_SUCCESS;
                                         },
                                         nullptr
                                     },
                                     TEST_HIST_NAME);

    TestDatastore ds(-1);
    ds.AddHistogram(TEST_HIST_NAME, hist);

    // before any events
    check_nothing_happend(hist);

    Blob predicate(TEST_HIST_NAME);

    float flt_subject = rand();
    Blob flt_blob = ReferenceBlob(&flt_subject, sizeof(flt_subject), hxhim_data_t::HXHIM_DATA_FLOAT);

    double dbl_subject = rand();
    Blob dbl_blob = ReferenceBlob(&dbl_subject, sizeof(dbl_subject), hxhim_data_t::HXHIM_DATA_DOUBLE);

    // GET should not update the histogram
    {
        {
            Message::Request::BGet flt(1);
            flt.subjects[0] = flt_blob;
            flt.predicates[0] = predicate;
            flt.object_types[0] = hxhim_data_t::HXHIM_DATA_POINTER;
            flt.count = 1;
            ds.operate(&flt);

            check_nothing_happend(hist);
        }

        {
            Message::Request::BGet dbl(1);
            dbl.subjects[0] = dbl_blob;
            dbl.predicates[0] = predicate;
            dbl.object_types[0] = hxhim_data_t::HXHIM_DATA_POINTER;
            dbl.count = 1;
            ds.operate(&dbl);

            check_nothing_happend(hist);
        }
    }

    // GETOP should not update the histogram
    {
        {
            Message::Request::BGetOp flt(1);
            flt.subjects[0] = flt_blob;
            flt.predicates[0] = predicate;
            flt.object_types[0] = hxhim_data_t::HXHIM_DATA_FLOAT;
            flt.count = 1;
            ds.operate(&flt);

            check_nothing_happend(hist);
        }

        {
            Message::Request::BGetOp dbl(1);
            dbl.subjects[0] = dbl_blob;
            dbl.predicates[0] = predicate;
            dbl.object_types[0] = hxhim_data_t::HXHIM_DATA_DOUBLE;
            dbl.count = 1;
            ds.operate(&dbl);

            check_nothing_happend(hist);
        }
    }

    // DELETE should not update the histogram
    {
        {
            Message::Request::BDelete flt(1);
            flt.subjects[0] = flt_blob;
            flt.predicates[0] = predicate;
            flt.count = 1;
            ds.operate(&flt);

            check_nothing_happend(hist);
        }

        {
            Message::Request::BDelete dbl(1);
            dbl.subjects[0] = dbl_blob;
            dbl.predicates[0] = predicate;
            dbl.count = 1;
            ds.operate(&dbl);

            check_nothing_happend(hist);
        }
    }

    // have multiple histograms tracking values
    for(std::size_t i = 0; i < HISTOGRAM_COUNT; i++) {
        std::stringstream s;
        s << "HISTOGRAM " << i;
        std::string pred_str = s.str();
        Blob pred(pred_str);

        // add a new histogram
        Histogram::Histogram *hist = construct<Histogram::Histogram>(
                                         Histogram::Config{
                                             FIRST_N,
                                                 [](const double *, const size_t,
                                                    double **buckets, size_t *size,
                                                    void *) -> int {
                                                 if (!buckets || !size) {
                                                     return HISTOGRAM_ERROR;
                                                 }

                                                 *size = 1;
                                                 *buckets = alloc_array<double>(*size + 1);
                                                 (*buckets)[0] = 0;

                                                 return HISTOGRAM_SUCCESS;
                                             },
                                             nullptr
                                         },
                                         pred_str);

        ds.AddHistogram(pred_str, hist);

        // check histograms
        const Datastore::Datastore::Histograms *hists = nullptr;
        EXPECT_EQ(ds.GetHistograms(&hists), DATASTORE_SUCCESS);
        EXPECT_NE(hists, nullptr);
        EXPECT_EQ(hists->size(), i + 1 + 1); // extra + 1 due to earlier histogram
        EXPECT_TRUE(hists->find(pred_str) != hists->end());

        std::size_t first_n = 0;
        double *cache = nullptr;
        std::size_t cache_size = 0;

        // PUT a float
        {
            Message::Request::BPut flt(1);
            flt.subjects[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            flt.predicates[0] = pred;
            flt.objects[0] = flt_blob;
            flt.count = 1;
            destruct(ds.operate(&flt));
        }

        for(std::size_t i = 1; i < FIRST_N; i++) {
            // check that the value was tracked by the histogram
            EXPECT_EQ(hist->get_cache(&first_n, &cache, &cache_size), HISTOGRAM_SUCCESS);
            EXPECT_EQ(first_n, FIRST_N);
            EXPECT_NE(cache, nullptr);
            EXPECT_EQ(cache_size, i);
            EXPECT_EQ(hist->get(nullptr, nullptr, nullptr), HISTOGRAM_ERROR);

            // PUT doubles
            Message::Request::BPut dbl(1);
            dbl.subjects[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            dbl.predicates[0] = pred;
            dbl.objects[0] = dbl_blob;
            dbl.count = 1;
            destruct(ds.operate(&dbl));
        }

        EXPECT_EQ(hist->get_cache(&first_n, &cache, &cache_size), HISTOGRAM_SUCCESS);
        EXPECT_EQ(first_n, FIRST_N);
        EXPECT_NE(cache, nullptr);
        EXPECT_EQ(cache_size, FIRST_N);

        const char *name = nullptr;
        std::size_t name_len = 0;
        EXPECT_EQ(hist->get_name(&name, &name_len), HISTOGRAM_SUCCESS);
        EXPECT_EQ(std::string(name, name_len), pred_str);

        double *buckets = nullptr;
        std::size_t *counts = nullptr;
        std::size_t size = 0;
        EXPECT_EQ(hist->get(&buckets, &counts, &size), HISTOGRAM_SUCCESS);
        EXPECT_NE(buckets, nullptr);
        EXPECT_NE(counts, nullptr);
        EXPECT_EQ(size, 1);
        EXPECT_EQ(buckets[0], 0);
        EXPECT_EQ(counts[0], FIRST_N);
    }
}
