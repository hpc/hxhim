#include <gtest/gtest.h>

#include "TestDatastore.hpp"
#include "utils/elen.hpp"

const std::string TEST_HIST_NAME = "Test Histogram Name";
const std::size_t FIRST_N = 5;

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

    Blob predicate = ReferenceBlob((void *) TEST_HIST_NAME.data(), TEST_HIST_NAME.size(), hxhim_data_t::HXHIM_DATA_BYTE);

    float flt_subject = rand();
    Blob flt_blob = ReferenceBlob(&flt_subject, sizeof(flt_subject), hxhim_data_t::HXHIM_DATA_FLOAT);

    double dbl_subject = rand();
    Blob dbl_blob = ReferenceBlob(&dbl_subject, sizeof(dbl_subject), hxhim_data_t::HXHIM_DATA_DOUBLE);

    // GET should not update the histogram
    {
        {
            Transport::Request::BGet flt(1);
            flt.subjects[0] = flt_blob;
            flt.predicates[0] = predicate;
            flt.object_types[0] = hxhim_data_t::HXHIM_DATA_POINTER;
            flt.count = 1;
            ds.operate(&flt);

            check_nothing_happend(hist);
        }

        {
            Transport::Request::BGet dbl(1);
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
            Transport::Request::BGetOp flt(1);
            flt.subjects[0] = flt_blob;
            flt.predicates[0] = predicate;
            flt.object_types[0] = hxhim_data_t::HXHIM_DATA_FLOAT;
            flt.count = 1;
            ds.operate(&flt);

            check_nothing_happend(hist);
        }

        {
            Transport::Request::BGetOp dbl(1);
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
            Transport::Request::BDelete flt(1);
            flt.subjects[0] = flt_blob;
            flt.predicates[0] = predicate;
            flt.count = 1;
            ds.operate(&flt);

            check_nothing_happend(hist);
        }

        {
            Transport::Request::BDelete dbl(1);
            dbl.subjects[0] = dbl_blob;
            dbl.predicates[0] = predicate;
            dbl.count = 1;
            ds.operate(&dbl);

            check_nothing_happend(hist);
        }
    }

    // PUT some values
    {
        std::size_t first_n = 0;
        double *cache = nullptr;
        std::size_t cache_size = 0;

        {
            Transport::Request::BPut flt(1);
            flt.subjects[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            flt.predicates[0] = predicate;
            flt.objects[0] = flt_blob;
            flt.count = 1;
            destruct(ds.operate(&flt));
        }

        // check that the value was tracked by the histogram
        for(std::size_t i = 1; i < FIRST_N; i++) {
            EXPECT_EQ(hist->get_cache(&first_n, &cache, &cache_size), HISTOGRAM_SUCCESS);
            EXPECT_EQ(first_n, FIRST_N);
            EXPECT_NE(cache, nullptr);
            EXPECT_EQ(cache_size, i);
            EXPECT_EQ(hist->get(nullptr, nullptr, nullptr), HISTOGRAM_ERROR);

            Transport::Request::BPut dbl(1);
            dbl.subjects[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            dbl.predicates[0] = predicate;
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
        EXPECT_EQ(std::string(name, name_len), TEST_HIST_NAME);

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

    // capture some values in a different histogram
    {
        // create the second histogram
        const std::string SECOND_HIST_NAME = "Second Histogram";
        Histogram::Histogram *hist2 = construct<Histogram::Histogram>(
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
                                                  (*buckets)[0] = FIRST_N;

                                                  return HISTOGRAM_SUCCESS;
                                              },
                                              nullptr
                                          },
                                          SECOND_HIST_NAME);

        // add the second histogram into the datastore
        ds.AddHistogram(SECOND_HIST_NAME, hist2);

        {
            const datastore::Datastore::Histograms *hists = nullptr;
            ASSERT_EQ(ds.GetHistograms(&hists), DATASTORE_SUCCESS);
            EXPECT_EQ(hists->size(), 2);
            EXPECT_TRUE(hists->find(TEST_HIST_NAME)   != hists->end());
            EXPECT_TRUE(hists->find(SECOND_HIST_NAME) != hists->end());
        }

        Blob predicate2 = ReferenceBlob((void *) SECOND_HIST_NAME.data(),
                                        SECOND_HIST_NAME.size(),
                                        hxhim_data_t::HXHIM_DATA_BYTE);

        // PUT values in a different range than the first histogram
        {
            std::size_t first_n = 0;
            double *cache = nullptr;
            std::size_t cache_size = 0;

            {
                Transport::Request::BPut flt(FIRST_N);
                flt.subjects[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
                flt.predicates[0] = predicate2;
                flt.objects[0] = flt_blob;
                flt.count = 1;
                destruct(ds.operate(&flt));

                EXPECT_EQ(hist2->get_cache(&first_n, &cache, &cache_size), HISTOGRAM_SUCCESS);
                EXPECT_EQ(first_n, FIRST_N);
                EXPECT_NE(cache, nullptr);
                EXPECT_EQ(cache_size, 1);
                EXPECT_EQ(hist2->get(nullptr, nullptr, nullptr), HISTOGRAM_ERROR);
            }

            for(std::size_t i = 1; i < FIRST_N; i++) {
                EXPECT_EQ(hist2->get_cache(&first_n, &cache, &cache_size), HISTOGRAM_SUCCESS);
                EXPECT_EQ(first_n, FIRST_N);
                EXPECT_NE(cache, nullptr);
                EXPECT_EQ(cache_size, i);
                EXPECT_EQ(hist2->get(nullptr, nullptr, nullptr), HISTOGRAM_ERROR);

                Transport::Request::BPut dbl(FIRST_N);
                dbl.subjects[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
                dbl.predicates[0] = predicate2;
                dbl.objects[0] = dbl_blob;
                dbl.count = 1;
                destruct(ds.operate(&dbl));
            }

            EXPECT_EQ(hist2->get_cache(&first_n, &cache, &cache_size), HISTOGRAM_SUCCESS);
            EXPECT_EQ(first_n, FIRST_N);
            EXPECT_NE(cache, nullptr);
            EXPECT_EQ(cache_size, FIRST_N);

            const char *name = nullptr;
            std::size_t name_len = 0;
            EXPECT_EQ(hist2->get_name(&name, &name_len), HISTOGRAM_SUCCESS);
            EXPECT_EQ(std::string(name, name_len), SECOND_HIST_NAME);

            double *buckets = nullptr;
            std::size_t *counts = nullptr;
            std::size_t size = 0;
            EXPECT_EQ(hist2->get(&buckets, &counts, &size), HISTOGRAM_SUCCESS);
            EXPECT_NE(buckets, nullptr);
            EXPECT_NE(counts, nullptr);
            EXPECT_EQ(size, 1);
            EXPECT_EQ(buckets[0], FIRST_N);
            EXPECT_EQ(counts[0], FIRST_N);
        }
    }
}
